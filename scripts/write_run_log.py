#!/usr/bin/env python3
"""
Generate RUN.md with execution log of data pipeline commands.

Usage:
    python scripts/write_run_log.py <data_dir> <make_target>

Appends to RUN.md with timestamp and command details.
"""

import pathlib
import sys
from datetime import datetime


def main():
    if len(sys.argv) != 3:
        print("Usage: python scripts/write_run_log.py <data_dir> <make_target>")
        sys.exit(1)

    data_dir = pathlib.Path(sys.argv[1])
    make_target = sys.argv[2]

    run_md = data_dir / "RUN.md"
    timestamp = datetime.utcnow().isoformat() + "Z"

    # Get environment info
    import os

    env_info = f"""
## Execution: {make_target} at {timestamp}

**Environment:**
- Python: {sys.version}
- Working Directory: {pathlib.Path.cwd()}
- Data Directory: {data_dir}
- User: {os.environ.get('USER', 'unknown')}

**Command:**
```bash
make {make_target}
```

**Makefile Variables:**
- DATA_DIR={os.environ.get('DATA_DIR', 'data')}
- QUERY={os.environ.get('QUERY', 'configs/query.small.yaml')}

---
"""

    # Append to RUN.md
    with run_md.open("a", encoding="utf-8") as f:
        f.write(env_info)

    print(f"Run log appended to {run_md}")


if __name__ == "__main__":
    main()
