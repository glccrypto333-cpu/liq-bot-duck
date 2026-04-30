import os

def runtime_mode_flags() -> dict:
    return {
        "RUN_DDL_MIGRATIONS": os.getenv("RUN_DDL_MIGRATIONS", "0"),
        "SKIP_HEAVY_AGGREGATES": os.getenv("SKIP_HEAVY_AGGREGATES", "0"),
        "SKIP_STAGE2_REBUILDS": os.getenv("SKIP_STAGE2_REBUILDS", "0"),
    }

def runtime_mode_text() -> str:
    flags = runtime_mode_flags()
    return " ".join(f"{k}={v}" for k, v in flags.items())
