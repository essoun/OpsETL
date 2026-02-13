# run_ops.py (project root)
from __future__ import annotations

import sys

from app.ops_cli import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

