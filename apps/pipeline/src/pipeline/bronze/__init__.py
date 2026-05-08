"""Bronze layer: snapshot-from-storage → bronze.* tables.

See ``load.run`` for the entry point. The CLI dispatcher in
``pipeline.__main__`` calls ``run()`` with the layer-stripped argv tail.
"""

from .load import run, run_bronze

__all__ = ["run", "run_bronze"]
