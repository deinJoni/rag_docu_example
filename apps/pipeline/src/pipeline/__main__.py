import sys
from collections.abc import Callable

from .bronze import run as run_bronze
from .gold import run as run_gold
from .silver import run as run_silver

LAYERS: dict[str, Callable[[], None]] = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in LAYERS:
        print(f"Usage: pipeline {{{'|'.join(LAYERS)}}}", file=sys.stderr)
        sys.exit(1)
    LAYERS[sys.argv[1]]()


if __name__ == "__main__":
    main()
