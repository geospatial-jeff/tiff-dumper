import asyncio
import logging
import os
import uuid
from functools import lru_cache

import geopandas as gpd
import obstore as obs
from anyio import create_memory_object_stream, create_task_group, to_thread
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from async_tiff import TIFF
from pyproj import Transformer
from shapely.geometry import Polygon
from shapely.ops import transform
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random

from tiff_dumper.stores import Stores

EXCLUDE_TAGS = {
    "strip_byte_counts",
    "strip_offsets",
    "tile_byte_counts",
    "tile_offsets",
    "geo_key_directory",
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@lru_cache()
def get_transformer(in_epsg: str, out_epsg: str) -> Transformer:
    return Transformer.from_crs(in_epsg, out_epsg, always_xy=True)


@retry(wait=wait_fixed(1) + wait_random(0, 2), stop=stop_after_attempt(3))
async def _open_tiff_with_retry(*args, **kwargs) -> TIFF:
    return await TIFF.open(*args, **kwargs)


async def _get_profile(stores: Stores, path: str):
    tiff = await _open_tiff_with_retry(path, store=stores.tiff_store, prefetch=65536)
    first_ifd = tiff.ifds[0]

    tiff_tags = {
        m: getattr(first_ifd, m)
        for m in dir(first_ifd)
        if not m.startswith("__") and m not in EXCLUDE_TAGS
    }
    geokeys = {
        m: getattr(first_ifd.geo_key_directory, m)
        for m in dir(first_ifd.geo_key_directory)
        if not m.startswith("__") and m not in EXCLUDE_TAGS
    }
    profile = {
        **tiff_tags,
        "geokeys": geokeys,
    }

    if model_transformation := first_ifd.other_tags.get(34264):
        xres = model_transformation[0]
        xmin = model_transformation[3]
        yres = model_transformation[5]
        ymin = model_transformation[7]

        xmax = xmin + (first_ifd.image_width * xres)
        ymax = ymin + (first_ifd.image_height * yres)
        polygon = Polygon.from_bounds(xmin, ymin, xmax, ymax)
    else:
        xres = first_ifd.model_pixel_scale[0]
        xmin = first_ifd.model_tiepoint[3]
        yres = -first_ifd.model_pixel_scale[1]
        ymax = first_ifd.model_tiepoint[4]

        ymin = ymax + (first_ifd.image_height * yres)
        xmax = xmin + (first_ifd.image_width * xres)
        polygon = Polygon.from_bounds(xmin, ymin, xmax, ymax)

    # Reproject geometry to WGS84
    epsg_code = first_ifd.geo_key_directory.projected_type
    transformer = get_transformer(in_epsg=epsg_code, out_epsg=4326)
    wgs84_geometry = transform(transformer.transform, polygon)
    profile["geometry"] = wgs84_geometry

    return profile


async def _consumer(
    receive: MemoryObjectReceiveStream[str],
    send_output: MemoryObjectSendStream[dict],
    send_error: MemoryObjectSendStream[dict],
    stores,
):
    """Consume"""
    logger.debug("starting consumer")
    async with send_output, send_error, receive:
        async for item in receive:
            try:
                try:
                    profile = await _get_profile(stores, item)
                    profile["path"] = item
                    await send_output.send(item=profile)
                except Exception as exc:
                    error_payload = {
                        "path": item,
                        "error_type": exc.__class__.__name__,
                        "error_message": str(exc),
                    }
                    await send_error.send(item=error_payload)
                    logger.exception("Error reading image %s", item)
                    continue
            except TimeoutError:
                logger.debug(f"Timed out on {item}")
    logger.debug("closing consumer")


async def _producer(
    send: MemoryObjectSendStream[str], stores, prefixes, chunk_size: int = 1000
):
    """Recursively list a prefix within the store, streaming each page of results onto a channel."""
    logger.debug("starting producer")
    async with send:
        for prefix in prefixes:
            stream = obs.list(stores.obs_store, prefix, chunk_size=chunk_size)
            async for batch in stream:
                for meta in batch:
                    if meta["path"].endswith(".tif"):
                        await send.send(meta["path"])
    logger.debug("closing producer")


def _write_parquet(records, filepath):
    df = gpd.GeoDataFrame.from_records(records)
    df.to_parquet(filepath)
    logger.debug(f"Wrote parquet - {filepath}")


async def _write_parquet_async(*args):
    await to_thread.run_sync(_write_parquet, *args)


async def _write_out(
    receive: MemoryObjectReceiveStream[dict], chunk_size: int, out_dir: str
):
    async with receive:
        records = []
        idx = 0
        async for item in receive:
            item["other_tags"] = {str(k): v for (k, v) in item["other_tags"].items()}
            records.append(item)
            if idx > chunk_size:
                await _write_parquet_async(
                    records, os.path.join(out_dir, f"{str(uuid.uuid4())}.parquet")
                )
                idx = 0
                records = []
            idx += 1

        # Write remaining records before closing stream
        if records:
            await _write_parquet_async(
                records, os.path.join(out_dir, f"{str(uuid.uuid4())}.parquet")
            )


async def _write_out_errors(
    receive: MemoryObjectReceiveStream[dict], chunk_size: int, out_dir: str
):
    async with receive:
        records = []
        idx = 0
        async for item in receive:
            records.append(item)
            if idx > chunk_size:
                await _write_parquet_async(
                    records, os.path.join(out_dir, f"{str(uuid.uuid4())}.parquet")
                )
                idx = 0
                records = []
            idx += 1

        # Write remaining records before closing stream
        if records:
            await _write_parquet_async(
                records, os.path.join(out_dir, f"{str(uuid.uuid4())}.parquet")
            )


async def _monitor(channel):
    while True:
        await asyncio.sleep(1)
        stats = channel.statistics()
        logger.debug(stats)
        if stats.current_buffer_used == 0 and stats.open_receive_streams == 0:
            break


async def dump_headers(
    out_dir: str,
    stores: Stores,
    prefixes: list[str] | None = None,
    n_consumers: int = 1000,
    list_chunk_size: int = 10000,
    max_buffer_size: int = 1000000,
    write_chunk_size: int = 100000,
    quiet: bool = False,
):
    if not quiet:
        logger.setLevel(logging.DEBUG)

    if not prefixes:
        prefixes = [""]

    try:
        async with create_task_group() as tg:
            send, receive = create_memory_object_stream(max_buffer_size, item_type=str)
            send_output, receive_output = create_memory_object_stream(
                max_buffer_size, item_type=dict
            )
            send_error, receive_error = create_memory_object_stream(
                max_buffer_size, item_type=dict
            )

            # Start the monitor
            if not quiet:
                tg.start_soon(_monitor, send)

            # Start the consumers
            streams = [(receive, send_output, send_error)]
            for _ in range(n_consumers - 1):
                streams.append(
                    (receive.clone(), send_output.clone(), send_error.clone())
                )
            for stream in streams:
                tg.start_soon(_consumer, *stream, stores)
            tg.start_soon(_write_out, receive_output, write_chunk_size, out_dir)

            # Write out errors
            error_dir = os.path.join(out_dir, "errors")
            os.makedirs(error_dir)
            tg.start_soon(_write_out_errors, receive_error, write_chunk_size, error_dir)

            # Start the producer
            tg.start_soon(_producer, send, stores, prefixes, list_chunk_size)
    except* Exception as excgroup:
        for exc in excgroup.exceptions:
            raise exc
