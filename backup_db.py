import gzip
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ARCHIVE = Path("archive")
BACKUPS = ARCHIVE / "backups"
MANIFESTS = ARCHIVE / "manifests"
LOCKS = ARCHIVE / "locks"
TMP = ARCHIVE / "tmp"

LOCK_FILE = LOCKS / "heavy_job.lock"
INDEX_FILE = MANIFESTS / "archive_index.json"
RETENTION_KEEP = int(os.getenv("BACKUP_RETENTION_KEEP", "7"))


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dirs():
    for p in [BACKUPS, MANIFESTS, LOCKS, TMP]:
        p.mkdir(parents=True, exist_ok=True)


def load_index():
    if not INDEX_FILE.exists():
        return []
    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        return []


def save_index(rows):
    INDEX_FILE.write_text(json.dumps(rows, ensure_ascii=False, indent=2))


def acquire_lock():
    if LOCK_FILE.exists():
        raise RuntimeError(f"LOCK_EXISTS: {LOCK_FILE}")
    LOCK_FILE.write_text(json.dumps({
        "job": "backup_db",
        "started_at": utc_now(),
        "pid": os.getpid(),
    }, ensure_ascii=False))


def release_lock():
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


def retention():
    files = sorted(BACKUPS.glob("db_*.sql.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
    removed = []
    for p in files[RETENTION_KEEP:]:
        removed.append(str(p))
        p.unlink(missing_ok=True)
    return removed


def main():
    ensure_dirs()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is empty")

    started = time.time()
    started_at = utc_now()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    tmp_file = TMP / f"db_{stamp}.sql.gz.tmp"
    out_file = BACKUPS / f"db_{stamp}.sql.gz"

    acquire_lock()
    status = "ERROR"
    error = None

    try:
        with gzip.open(tmp_file, "wb") as gz:
            proc = subprocess.Popen(
                ["pg_dump", db_url],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(stderr.decode("utf-8", errors="replace")[:3000])
            gz.write(stdout)

        tmp_file.rename(out_file)
        removed = retention()

        status = "OK"
        duration = round(time.time() - started, 2)
        size = out_file.stat().st_size

        entry = {
            "type": "backup_db",
            "status": status,
            "started_at": started_at,
            "finished_at": utc_now(),
            "duration_sec": duration,
            "file": str(out_file),
            "size_bytes": size,
            "size_mb": round(size / 1024 / 1024, 2),
            "retention_removed": removed,
        }

        rows = load_index()
        rows.append(entry)
        save_index(rows)

        print(json.dumps(entry, ensure_ascii=False, indent=2))

    except Exception as exc:
        error = str(exc)
        duration = round(time.time() - started, 2)
        entry = {
            "type": "backup_db",
            "status": "ERROR",
            "started_at": started_at,
            "finished_at": utc_now(),
            "duration_sec": duration,
            "error": error,
        }
        rows = load_index()
        rows.append(entry)
        save_index(rows)
        print(json.dumps(entry, ensure_ascii=False, indent=2))
        raise

    finally:
        release_lock()
        tmp_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
