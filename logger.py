from time_utils import текст_мск

def log(message: str) -> None:
    ts = текст_мск()
    print(f"{ts} | {message}", flush=True)
