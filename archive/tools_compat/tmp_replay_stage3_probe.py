from __future__ import annotations

import sys

print("tmp_replay_stage3_probe.py is deprecated; use verification_stage3_follow.py --follow-steps 0")

from verification_stage3_follow import main


if __name__ == "__main__":
    sys.argv.extend(["--follow-steps", "0"])
    main()
