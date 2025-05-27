import asyncio

import click

from tiff_dumper import tiff_dumper
from tiff_dumper.config_file import DumpTiffHeadersConfig
from tiff_dumper.stores import Stores


@click.group
def app():
    pass


@app.command
@click.argument(
    "config_file", type=click.Path(file_okay=True, dir_okay=False, exists=True)
)
@click.argument("out_dir", type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.option("--quiet/--loud", default=False)
def headers(config_file: str, out_dir: str, quiet: bool = False):
    config = DumpTiffHeadersConfig.from_yaml(config_file)
    stores = Stores.from_config(config.store)

    asyncio.run(
        tiff_dumper.dump_headers(
            out_dir,
            stores,
            config.store.prefixes,
            config.concurrency.n_consumers,
            config.concurrency.list_chunk_size,
            config.concurrency.max_buffer_size,
            config.concurrency.write_chunk_size,
            quiet=quiet,
        )
    )
