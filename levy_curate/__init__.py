"""levy_curate -- a config-driven pipeline for depositing file collections into Dataverse.

The design principle: anything that varies per batch lives in a YAML config;
anything that varies per installation lives in .env; the code stays fixed.

Typical flow:

    from levy_curate import BatchConfig, scaffold, inventory

    cfg = BatchConfig.from_yaml("configs/geotiffs.yaml")
    scaffold.build(cfg)          # partial CSV + files on disk -> complete metadata
    inv = inventory.load(cfg)    # normalize columns, validate, reconcile vs disk
    ...
"""

from .config import BatchConfig, Credentials, load_credentials

__version__ = "2.0.0"

__all__ = ["BatchConfig", "Credentials", "load_credentials", "__version__"]
