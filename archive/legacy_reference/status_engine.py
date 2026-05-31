from __future__ import annotations
from config import ПАПКА_ДАННЫХ, APP_VERSION

def build_status_text() -> str:
    files = sorted(p.name for p in ПАПКА_ДАННЫХ.glob("*") if p.is_file())
    files_txt = "\n".join(f"• {x}" for x in files[:30]) if files else "• файлов пока нет"
    return f"🥇 Mighty Duck / {APP_VERSION}\n\n📊 Статус\n\nФайлы:\n{files_txt}"
