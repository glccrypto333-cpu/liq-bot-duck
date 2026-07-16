"""CoinGlass chart screenshot via Playwright.

Поддерживает съёмку НЕСКОЛЬКИХ таймфреймов в ОДНОЙ сессии браузера
(make_coinglass_screenshots) — без второго аккаунта и без параллельного графика:
после загрузки графика переключаем кнопку ТФ (1m/5m/30m/1H/1D) и снимаем каждый.
Старая make_coinglass_screenshot (один ТФ) сохранена для обратной совместимости.
"""

import asyncio
import fcntl
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .config import (
    CHART_SCREENSHOT_DIR,
    CHART_LOAD_TIMEOUT_MS,
    CHART_SCREENSHOT_RETRIES,
    CHART_VIEWPORT_WIDTH,
    CHART_VIEWPORT_HEIGHT,
    HEADLESS,
    COINGLASS_EMAIL,
    COINGLASS_PASSWORD,
    COINGLASS_STORAGE_STATE_PATH,
    COINGLASS_LOGIN_URL,
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# CoinGlass бар /tv: прямые кнопки 1m 5m 30m 1H 1D. Остальные ТФ (3m 15m 2H 4H 6H 8H 12H 1W)
# — в ДРОПДАУНЕ (стрелка справа от 1D). 4H именно там.
_TF_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1H", "1H": "1H", "2h": "2H", "2H": "2H", "4h": "4H", "4H": "4H",
    "6h": "6H", "6H": "6H", "8h": "8H", "8H": "8H", "12h": "12H", "12H": "12H",
    "1d": "1D", "1D": "1D", "1w": "1W", "1W": "1W",
}
# Прямые кнопки на баре (top<40). Остальное — через дропдаун.
_BAR_TFS = {"1m", "5m", "30m", "1H", "1D"}

# Dropdown timeframes have no active button in the top bar. TradingView exposes
# the actually applied interval in its blob-frame legend, which is independent
# of whether the menu click itself appeared to succeed.
_TV_LEGEND_TOKEN = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1H": "60", "2H": "2h", "4H": "4h", "6H": "6h", "8H": "8h", "12H": "12h",
    "1D": "1D", "1W": "1W",
}
DROPDOWN_ITEM_LOOKUP_ATTEMPTS = 3
DROPDOWN_ITEM_RETRY_SECONDS = 0.5


@dataclass(frozen=True)
class ChartCaptureResult:
    photo_paths: list[str]
    requested_timeframes: tuple[str, ...]
    captured_timeframes: tuple[str, ...]
    capture_seconds_total: float
    timeframe_seconds: dict[str, float] = field(default_factory=dict)
    timeframe_verification_failures: tuple[str, ...] = ()
    failure_reason: str | None = None

    @property
    def ok(self) -> bool:
        return bool(self.photo_paths)


def _log(msg: str) -> None:
    print(f"[CoinGlass] {msg}", flush=True)


async def _dismiss_consent(page) -> None:
    """Закрывает cookie/consent баннер (иначе перекрывает форму логина)."""
    for t in ("Accept All", "Accept all", "I Accept", "Accept", "Agree", "Got it", "OK", "Принять"):
        try:
            b = page.get_by_role("button", name=t, exact=False)
            if await b.count() > 0:
                await b.first.click(timeout=2000)
                await asyncio.sleep(0.6)
                return
        except Exception:
            pass
    for s in ("[class*='consent'] button", "#onetrust-accept-btn-handler", "[aria-label*='accept' i]", "[class*='cookie'] button"):
        try:
            el = await page.query_selector(s)
            if el:
                await el.click(timeout=2000)
                await asyncio.sleep(0.6)
                return
        except Exception:
            pass


async def _coinglass_login(page) -> bool:
    """Авторизация в CoinGlass через email/password. Возвращает True если успешно.

    ВАЖНО: на странице ДВЕ кнопки "Login" — в шапке (перезагружает /login) и
    кнопка сабмита внутри формы. Жмём именно форменную (самую широкую).
    Поля заполняем через type() (React-controlled inputs не реагируют на fill()).
    """
    _log(f"Логин в CoinGlass: {COINGLASS_EMAIL}")
    try:
        await page.goto(COINGLASS_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2.5)
        await _dismiss_consent(page)
        await asyncio.sleep(0.8)

        email_input = await page.query_selector('input[type="email"], input[name="email"], input[placeholder*="mail" i]')
        if not email_input:
            _log("Поле email не найдено — возможно другая вёрстка")
            return False
        await email_input.click()
        await email_input.fill("")
        await email_input.type(COINGLASS_EMAIL, delay=25)
        await asyncio.sleep(0.3)

        pwd_input = await page.query_selector('input[type="password"]')
        if not pwd_input:
            _log("Поле пароля не найдено")
            return False
        await pwd_input.click()
        await pwd_input.fill("")
        await pwd_input.type(COINGLASS_PASSWORD, delay=25)
        await asyncio.sleep(0.3)

        # Сабмит: самая широкая кнопка Login (форменная, не шапка)
        submitted = False
        try:
            info = await page.evaluate("""() => {
                const bs = Array.from(document.querySelectorAll('button'));
                let idx = -1, w = 0;
                bs.forEach((b, i) => {
                    const t = (b.innerText || '').trim().toLowerCase();
                    const r = b.getBoundingClientRect();
                    if ((t === 'login' || t === 'log in' || t === 'sign in') && r.width > w && r.height > 0) { w = r.width; idx = i; }
                });
                if (idx >= 0) { bs[idx].setAttribute('data-cg-submit', '1'); return true; }
                return false;
            }""")
            if info:
                await page.click("button[data-cg-submit='1']", timeout=3000)
                submitted = True
        except Exception as e:
            _log(f"клик по форменной кнопке не удался: {e}")
        if not submitted:
            await pwd_input.press("Enter")

        await asyncio.sleep(5)
        current = page.url.lower()
        if "login" in current:
            _log("Логин не удался — всё ещё на странице логина")
            return False

        try:
            cookies = await page.context.cookies()
            has_session = any(c["name"] in ("obe",) or "token" in c["name"].lower() for c in cookies)
        except Exception:
            has_session = True
        if not has_session:
            _log("Редирект есть, но сессионной cookie нет — считаю неуспехом")
            return False

        _log("Логин успешен, сохраняю storage_state.json")
        await page.context.storage_state(path=COINGLASS_STORAGE_STATE_PATH)
        return True

    except Exception as e:
        _log(f"Ошибка логина: {e}")
        return False


async def _apply_timeframe(page, timeframe: str) -> bool:
    """Устанавливает таймфрейм. Прямые кнопки бара (1m/5m/30m/1H/1D) — клик по кнопке;
    остальные (2H/4H/6H/8H/12H/3m/15m/1W) — через дропдаун-стрелку справа от 1D.
    Возвращает True если ТФ применён."""
    tf_label = _TF_MAP.get(str(timeframe).strip(), str(timeframe).strip())
    try:
        # 1) Прямая кнопка на баре
        if tf_label in _BAR_TFS:
            changed = await page.evaluate(
                """(label) => {
                    for (const e of document.querySelectorAll('button')) {
                        const t = (e.innerText || '').trim();
                        const r = e.getBoundingClientRect();
                        if (t === label && r.top < 40 && r.width > 0 && r.height > 0) {
                            const active = getComputedStyle(e).color.includes('41, 98, 255');
                            if (!active) e.click();
                            return true;
                        }
                    }
                    return false;
                }""",
                tf_label,
            )
            if changed:
                _log(f"Таймфрейм {tf_label} применён (бар)")
                return True

        # 2) Через дропдаун (стрелка справа от 1D, ~left 470-545, top<38, узкая IconButton)
        opened = await page.evaluate(
            """() => {
                for (const e of document.querySelectorAll('button')) {
                    const r = e.getBoundingClientRect();
                    if (r.top < 38 && r.left >= 470 && r.left <= 545 && r.width > 0 && r.width < 40 && r.height > 0) {
                        e.click();
                        return true;
                    }
                }
                return false;
            }"""
        )
        if opened:
            await asyncio.sleep(1.0)
            picked = await _pick_dropdown_timeframe(page, tf_label)
            if picked:
                _log(f"Таймфрейм {tf_label} применён (дропдаун)")
                return True
            _log(f"ТФ {tf_label} не найден в дропдауне")
            return False

        _log(f"Кнопка/дропдаун ТФ {tf_label} не найдены — оставляю текущий layout")
        return False
    except Exception as e:
        _log(f"Ошибка установки таймфрейма {tf_label}: {e}")
        return False


async def _pick_dropdown_timeframe(page, tf_label: str) -> bool:
    """Wait briefly for a delayed CoinGlass dropdown item, then click it."""
    for attempt in range(DROPDOWN_ITEM_LOOKUP_ATTEMPTS):
        picked = await page.evaluate(
            """(label) => {
                    let best = null, area = 1e9;
                    for (const e of document.querySelectorAll('li,div,button,span,a')) {
                        const t = (e.innerText || '').trim();
                        const r = e.getBoundingClientRect();
                        if (t === label && r.width > 0 && r.height > 0 && r.top > 20) {
                            const a = r.width * r.height;
                            if (a < area) { area = a; best = e; }
                        }
                    }
                    if (best) { best.click(); return true; }
                    return false;
                }""",
            tf_label,
        )
        if picked:
            return True
        if attempt + 1 < DROPDOWN_ITEM_LOOKUP_ATTEMPTS:
            await asyncio.sleep(DROPDOWN_ITEM_RETRY_SECONDS)
    return False


async def _verify_timeframe(page, timeframe: str) -> bool:
    tf_label = _TF_MAP.get(str(timeframe).strip(), str(timeframe).strip())
    try:
        if tf_label in _BAR_TFS:
            # Бар: активная кнопка подсвечена цветом rgb(41,98,255) — надёжная проверка.
            return bool(
                await page.evaluate(
                    """(label) => {
                        for (const e of document.querySelectorAll('button')) {
                            const t = (e.innerText || '').trim();
                            const r = e.getBoundingClientRect();
                            if (t === label && r.top < 40 && r.width > 0 && r.height > 0) {
                                return getComputedStyle(e).color.includes('41, 98, 255');
                            }
                        }
                        return false;
                    }""",
                    tf_label,
                )
            )
        evidence = await _read_dropdown_timeframe_evidence(page, tf_label)
        return dropdown_timeframe_has_reliable_evidence(tf_label, evidence)
    except Exception as e:
        _log(f"Ошибка проверки таймфрейма {tf_label}: {e}")
        return False


async def _read_dropdown_timeframe_evidence(page, timeframe: str) -> str | None:
    expected = _TV_LEGEND_TOKEN.get(_TF_MAP.get(str(timeframe).strip(), str(timeframe).strip()))
    if not expected:
        return None
    expected_lower = expected.lower()
    for frame in page.frames:
        if not frame.url.startswith("blob:"):
            continue
        try:
            text = await frame.locator("body").inner_text(timeout=1500)
        except Exception:
            continue
        tokens = [token.strip().lower() for token in text.replace("\t", "\n").split("\n") if token.strip()]
        if expected_lower in tokens:
            return expected
    return None


def dropdown_timeframe_has_reliable_evidence(timeframe: str, evidence: str | None) -> bool:
    expected = _TV_LEGEND_TOKEN.get(_TF_MAP.get(str(timeframe).strip(), str(timeframe).strip()))
    return bool(expected and evidence and str(evidence).strip().lower() == expected.lower())


async def _wait_chart_loaded(page) -> None:
    """Ждёт загрузки графика (iframe + canvas + исчезновение спиннера)."""
    try:
        await page.wait_for_selector("iframe", timeout=CHART_LOAD_TIMEOUT_MS)
        await asyncio.sleep(1)
        frame = page.frame_locator("iframe").first
        try:
            await frame.locator("canvas").first.wait_for(timeout=CHART_LOAD_TIMEOUT_MS)
        except Exception:
            pass
        try:
            await page.wait_for_selector(".loading, .spinner, .tv-spinner, [class*=loading], [class*=spinner]", state="hidden", timeout=10000)
        except Exception:
            pass
    except Exception:
        _log("Элемент графика не найден за таймаут")


_LOCAL_CAPTURE_SEMAPHORE = asyncio.Semaphore(2)
_GLOBAL_CAPTURE_SLOT_DIR = Path("/tmp/openclaw_chart_capture_slots")
_GLOBAL_CAPTURE_SLOT_COUNT = 3


@asynccontextmanager
async def _global_capture_slot():
    """Bound concurrent Chromium instances across all chart bots."""
    _GLOBAL_CAPTURE_SLOT_DIR.mkdir(parents=True, exist_ok=True)
    handle = None
    while handle is None:
        for index in range(_GLOBAL_CAPTURE_SLOT_COUNT):
            candidate = (_GLOBAL_CAPTURE_SLOT_DIR / f"slot-{index}").open("a+")
            try:
                fcntl.flock(candidate.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                handle = candidate
                break
            except BlockingIOError:
                candidate.close()
        if handle is None:
            await asyncio.sleep(0.25)
    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


async def _capture_session_unlocked(symbol: str, exchange, timeframes) -> ChartCaptureResult:
    """Съёмка нескольких таймфреймов в ОДНОЙ сессии браузера.
    Возвращает список путей к PNG (best-effort: только успешно снятые ТФ)."""
    os.makedirs(CHART_SCREENSHOT_DIR, exist_ok=True)

    ex = (exchange or "BYBIT").upper()
    if ex not in ("BINANCE", "BYBIT"):
        ex = "BYBIT"
    sym = symbol.upper()
    if not sym.endswith("USDT"):
        sym += "USDT"
    chart_url = f"https://www.coinglass.com/tv/{ex}_{sym}"
    tfs = [str(t).strip() for t in (timeframes or ["5m"]) if str(t).strip()]
    _log(f"Скриншоты {tfs}: {chart_url}")
    session_started_at = time.monotonic()

    for attempt in range(1, CHART_SCREENSHOT_RETRIES + 1):
        browser = None
        try:
            from playwright.async_api import async_playwright

            pw = await async_playwright().start()
            browser = await pw.chromium.launch(headless=HEADLESS)

            context_kwargs = {
                "viewport": {"width": CHART_VIEWPORT_WIDTH, "height": CHART_VIEWPORT_HEIGHT},
                "user_agent": _UA,
                "locale": "en-US",
            }
            if os.path.exists(COINGLASS_STORAGE_STATE_PATH):
                context_kwargs["storage_state"] = COINGLASS_STORAGE_STATE_PATH
                _log(f"Использую сохранённую сессию: {COINGLASS_STORAGE_STATE_PATH}")

            context = await browser.new_context(**context_kwargs)
            await context.add_init_script(
                "try { localStorage.setItem('cg_hide_right_v2','1'); } catch(e) {}"
            )
            page = await context.new_page()

            # Проверка авторизации через /account
            await page.goto("https://www.coinglass.com/account", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(6)
            body = ""
            try:
                body = await page.evaluate("() => document.body.innerText")
            except Exception:
                pass
            logged_in = ("login" not in page.url.lower()) and any(
                kw in body for kw in ("Welcome Back", "Change Password", "Log Out", "Logout", "Invite Friends")
            )
            if not logged_in:
                _log("Не залогинены, выполняю авто-логин...")
                if await _coinglass_login(page):
                    _log("Авто-логин успешен")
                    await page.context.storage_state(path=COINGLASS_STORAGE_STATE_PATH)
                else:
                    _log("Авто-логин не удался — скриншот будет без авторизации")
            else:
                _log("Сессия активна (логин не требуется)")

            # Переходим на график и ждём загрузки
            await page.goto(chart_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            await _wait_chart_loaded(page)
            await asyncio.sleep(4)  # первичный рендер индикаторов

            # Съёмка каждого ТФ в этой же сессии
            paths = []
            captured_timeframes: list[str] = []
            timeframe_seconds: dict[str, float] = {}
            verification_failures: list[str] = []
            for i, tf in enumerate(tfs):
                tf_started_at = time.monotonic()
                await _apply_timeframe(page, tf)
                await asyncio.sleep(5 if i == 0 else 4)  # ждём перерисовку свечей
                tf_label = _TF_MAP.get(str(tf).strip(), str(tf).strip())
                timeframe_seconds[tf_label] = round(time.monotonic() - tf_started_at, 3)
                if not await _verify_timeframe(page, tf):
                    verification_failures.append(tf_label)
                    _log(f"Таймфрейм {tf_label} не подтвердился после переключения")
                    continue
                out_path = os.path.join(CHART_SCREENSHOT_DIR, f"{sym}_{tf_label}_{int(time.time())}_{i}.png")
                try:
                    await page.screenshot(path=out_path, full_page=False)
                except Exception as se:
                    _log(f"Ошибка screenshot {tf_label}: {se}")
                    continue
                if os.path.getsize(out_path) < 1000:
                    _log(f"Скриншот {tf_label} пустой/мал — пропуск")
                    try:
                        os.remove(out_path)
                    except Exception:
                        pass
                    continue
                _log(f"Скриншот {tf_label} сохранён: {out_path}")
                paths.append(out_path)
                captured_timeframes.append(tf_label)

            await browser.close()
            await pw.stop()

            if paths:
                return ChartCaptureResult(
                    photo_paths=paths,
                    requested_timeframes=tuple(tfs),
                    captured_timeframes=tuple(captured_timeframes),
                    capture_seconds_total=round(time.monotonic() - session_started_at, 3),
                    timeframe_seconds=dict(timeframe_seconds),
                    timeframe_verification_failures=tuple(verification_failures),
                )
            # если ничего не сняли — попробуем ещё раз (в пределах retries)
        except Exception as e:
            _log(f"Ошибка сессии (попытка {attempt}/{CHART_SCREENSHOT_RETRIES}): {e}")
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            if attempt < CHART_SCREENSHOT_RETRIES:
                await asyncio.sleep(2)
            continue

    return ChartCaptureResult(
        photo_paths=[],
        requested_timeframes=tuple(tfs),
        captured_timeframes=(),
        capture_seconds_total=round(time.monotonic() - session_started_at, 3),
        failure_reason="capture_failed",
    )


async def _capture_session(symbol: str, exchange, timeframes) -> ChartCaptureResult:
    # One capture per bot plus a server-wide three-browser ceiling prevents bursts.
    async with _LOCAL_CAPTURE_SEMAPHORE:
        async with _global_capture_slot():
            return await _capture_session_unlocked(symbol, exchange, timeframes)


async def make_coinglass_screenshots(
    symbol: str,
    exchange: str | None = None,
    timeframes=None,
) -> list:
    """Несколько таймфреймов в одной сессии."""
    result = await _capture_session(symbol, exchange, timeframes or ["5m", "4H"])
    return result.photo_paths


async def capture_coinglass_screenshots(
    symbol: str,
    exchange: str | None = None,
    timeframes=None,
) -> ChartCaptureResult:
    """Возвращает расширенный результат съёмки с метриками и проверкой таймфреймов."""
    return await _capture_session(symbol, exchange, timeframes or ["5m", "4H"])


async def make_coinglass_screenshot(
    symbol: str,
    exchange: str | None = None,
    timeframe: str = "5m",
) -> str | None:
    """Один таймфрейм (обратная совместимость). Возвращает путь к PNG или None."""
    result = await _capture_session(symbol, exchange, [timeframe])
    return result.photo_paths[0] if result.photo_paths else None
