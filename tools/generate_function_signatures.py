import sys
from pathlib import Path

from opteryx.functions.signatures import write_function_signatures

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))



def main() -> None:
    write_function_signatures(ROOT / "opteryx/functions/function_signatures.json")


if __name__ == "__main__":
    main()
