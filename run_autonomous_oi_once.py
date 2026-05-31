from __future__ import annotations

from autonomous_oi_service import run_autonomous_oi_service


def main() -> None:
    rows = run_autonomous_oi_service()
    print(rows)


if __name__ == "__main__":
    main()
