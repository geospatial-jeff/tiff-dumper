import asyncio
import logging
import os
import uuid

import obstore as obs
import pandas as pd
from anyio import create_memory_object_stream, create_task_group, to_thread
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from async_tiff import TIFF

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


async def _get_profile(stores: Stores, path: str):
    tiff = await TIFF.open(path, store=stores.tiff_store, prefetch=65536)
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
    return profile


async def _consumer(
    receive: MemoryObjectReceiveStream[str],
    send_output: MemoryObjectSendStream[dict],
    stores,
):
    """Consume"""
    logger.debug("starting consumer")
    async with send_output:
        async with receive:
            async for item in receive:
                try:
                    profile = await _get_profile(stores, item)
                    profile["path"] = item
                    await send_output.send(item=profile)
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
    df = pd.DataFrame.from_records(records)
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

            # Start the monitor
            if not quiet:
                tg.start_soon(_monitor, send)

            # Start the consumers
            streams = [(receive, send_output)]
            for _ in range(n_consumers - 1):
                streams.append((receive.clone(), send_output.clone()))
            for stream in streams:
                tg.start_soon(_consumer, *stream, stores)
            tg.start_soon(_write_out, receive_output, write_chunk_size, out_dir)

            # Start the producer
            tg.start_soon(_producer, send, stores, prefixes, list_chunk_size)
    except* Exception as excgroup:
        for exc in excgroup.exceptions:
            raise exc
