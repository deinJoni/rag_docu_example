import sys
from collections.abc import Callable

from .bronze import run as run_bronze
from .gold import run as run_gold
from .silver import run as run_silver

LAYERS: dict[str, Callable[[list[str]], None]] = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in LAYERS:
        print(f"Usage: pipeline {{{'|'.join(LAYERS)}}} [args...]", file=sys.stderr)
        sys.exit(1)
    layer = sys.argv[1]
    LAYERS[layer](sys.argv[2:])


if __name__ == "__main__":
    main()
