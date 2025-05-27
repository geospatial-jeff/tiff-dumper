import yaml
from pydantic import BaseModel, model_validator


class Concurrency(BaseModel):
    # The number of concurrent consumers (coroutines) used when fetching
    # tiff headers.  Each consumer is responsible for dumping one key
    # at a time.  This should be set to a number less or equal
    # to `list_chunk_size` to provide backpresure.
    n_consumers: int = 1000

    # The number of keys to include in each page returned by the list request.
    list_chunk_size: int = 10000

    # The number of keys to buffer in memory before blocking production
    # against the stream.  This should be set higher than `list_chunk_size`,
    # such that the results of multiple list requests may be buffered in memory.
    # `list_chunk_size` should also divide evenly into `max_buffer_size` for optimal
    # memory efficiency.
    max_buffer_size: int = 1000000

    # How many keys to include in each `.parquet` file written to disk.  This should
    # be set larger than `list_chunk_size` but smaller than `max_buffer_size`.
    write_chunk_size: int = 100000

    @model_validator(mode="after")
    def validate_concurrency(self) -> "Concurrency":
        if self.n_consumers > self.list_chunk_size:
            raise ValueError("'n_consumers' must be set lower than 'list_chunk_size'")
        if self.max_buffer_size < self.list_chunk_size:
            raise ValueError(
                "'max_buffer_size' must be set higher than 'list_chunk_size'"
            )
        if self.max_buffer_size % self.list_chunk_size != 0:
            raise ValueError(
                "'max_buffer_size' should divide evenly into 'list_chunk_size'"
            )
        if (self.write_chunk_size > self.max_buffer_size) or (
            self.write_chunk_size < self.list_chunk_size
        ):
            raise ValueError(
                "'write_chunk_size' should be greater than 'list_chunk_size' but less than 'max_buffer_size'"
            )
        return self


class StoreConfig(BaseModel):
    name: str
    prefixes: list[str] | None = None
    obstore_config: dict = {}


class DumpTiffHeadersConfig(BaseModel):
    store: StoreConfig
    concurrency: Concurrency

    @classmethod
    def from_yaml(cls, filepath: str) -> "DumpTiffHeadersConfig":
        with open(filepath) as f:
            data = yaml.safe_load(f)
            return cls.model_validate(data)
