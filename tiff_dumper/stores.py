from dataclasses import dataclass

import obstore as obs
from async_tiff import store as async_tiff_store

from tiff_dumper.config_file import StoreConfig


@dataclass
class Stores:
    obs_store: obs.store.ObjectStore
    tiff_store: obs.store.ObjectStore

    @classmethod
    def from_config(cls, config: StoreConfig) -> "Stores":
        return cls.init(config.name, **config.obstore_config)

    @classmethod
    def init(cls, *args, **kwargs) -> "Stores":
        return cls(
            obs_store=obs.store.from_url(*args, **kwargs),
            tiff_store=async_tiff_store.from_url(*args, **kwargs),
        )
