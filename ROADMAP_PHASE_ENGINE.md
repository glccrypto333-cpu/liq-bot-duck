# Mighty Duck — актуальное ТЗ / Roadmap

## Режим после каждого патча

После каждого патча фиксируем:

- что изменено;
- какие файлы затронуты;
- какие таблицы изменились;
- какие расчеты перепроверены;
- какие риски остались;
- что требует решения;
- обновленный roadmap;
- статус: ✅ выполнено / 🟨 в работе / ⛔ заблокировано.

## Архитектура

Фазы строятся OI-first.

Цена и объем НЕ являются главными phase drivers.

### Главный приоритет метрик

1. OI structure
2. OI priority
3. OI trend / slope
4. OI hold state
5. Price structure
6. Volume structure / hold
7. DMD / confidence

## Phase transitions

| Переход | Что реально влияет | Усилители |
|---|---|---|
| 0 → 1 | OI ожил, oi_priority=P3/P4, структура не мусор, price не хаос | volume обычный/краткий всплеск |
| 1 → 2 | Stage 1 ≥ 1h, oi_priority=P1/P2/P3, slope устойчивый, hold/trying_hold | volume растет, price держится |
| 2 → 3 | Stage 2 ≥ 15m, oi_priority=P1/P2, сильный/плавный oi_slope, структура качественная | volume 10x/100x, price удержание |
| 2 → 1 | OI остыл, slope потерян, hold пропал, но мусора нет | volume затух |
| 1 → 0 | OI мертвый/нисходящий, структура мусорная | шум/invalid |
| 2 → 0 | пила, распределение, нисходящий OI, invalid market | хаос price/volume |
| 3 → 0 | только manual reset | после reset пересчет заново |

## Уже сделано

### ✅ PATCH 01 — Убрать старую score-логику из фаз

Удалено из новой архитектуры:

- raw_strength как основа фаз;
- strength как phase driver;
- alignment-first модель;
- continuation_score;
- exhaustion_score;
- market_regime как главный phase source.

### 🟨 PATCH 02 — Active universe

Текущее состояние:

- Binance: 480
- Bybit: 0

Осталось:

- срезать Binance до top-100.

Требует решения:

- volume / OI / whitelist / funding-liquidity filter.

### ✅ PATCH 03 — OI-first архитектура

Зафиксировано:

- фаза не строится от price;
- фаза не строится от volume;
- Stage 3 не появляется от volume spike;
- Stage 3 не появляется от price pump.

### ✅ PATCH 04 — market_oi_slope_engine

Добавлено:

- oi_structure
- oi_quality
- oi_priority
- oi_hold_state
- oi_trend_1h
- oi_trend_4h
- oi_trend_24h
- oi_reason

### ✅ PATCH 05 — OI evaluation table

Категории:

- OI delta
- acceleration
- slope
- quality

### ✅ PATCH 06 — OI structure taxonomy

Реализовано:

- тишина
- спокойный боковик
- плавный набор
- ступенчатый набор
- ускорение
- агрессивный набор
- удержание после роста
- пила
- всплеск без удержания
- перегрев
- распределение
- нисходящий OI

### ✅ PATCH 07 — Price engine

Добавлено:

- price_structure
- price_quality
- price_slope_state
- price_trend_24h
- price_range_from_median_pct
- price_reason

Цена теперь контекст, а не direction signal.

### 🟨 PATCH 08 — Volume engine

Добавлено:

- volume_structure
- volume_quality
- volume_baseline_24h
- volume_hold_state
- volume_reason

Объем теперь усилитель/контекст, не trigger.

Осталось:

- финализировать 10x/100x persistence;
- rolling window для volume hold.

### 🟨 PATCH 09 — Убрать volume score

Volume больше не phase driver.

Осталось:

- дочистить legacy remnants.

### ✅ PATCH 10 — market_phase state machine

Сделано:

- создан `market_phase_engine.py`;
- создана таблица `market_phase`;
- создана таблица `market_phase_history`;
- подключен rebuild в `main.py`;
- запрещен прямой 0→3 / 1→3;
- Stage 3 lock до manual reset;
- добавлены `phase_status`, `priority`, `dmd_level`, `confidence`;
- добавлен `market_phase.csv` в export bundle.

Остаточный риск:

- нужна оптимизация SQL до latest-only snapshot;
- нужен manual reset action;
- нужен Telegram UI.

## Новые движки

### market_oi_slope_engine

Главный фазовый источник.

Управляет:

- stage interest;
- slope;
- OI quality;
- OI priority;
- hold.

### market_price_engine

Контекст.

Фильтрует:

- хаос;
- памп/дамп без OI;
- возврат;
- сжатие;
- расширение.

### market_volume_engine

Усилитель.

Управляет:

- confidence;
- DMD;
- подтверждением активности;
- шумом.

### market_phase_engine

State-machine.

Управляет:

- phase;
- phase_status;
- priority;
- manual_reset_required;
- transition_reason;
- history.

## Следующие патчи

### PATCH 10.1 — market_phase performance fix

- считать только latest snapshot;
- не пересчитывать 534k строк;
- добавить индексы;
- снизить memory pressure.

### PATCH 13 — Manual reset Stage 3

- команда Telegram;
- reset в Stage 0;
- последующий автоматический пересчет.

### PATCH 14–17 — Telegram

- меню фаз;
- Stage 3 alerts;
- top OI slope;
- phase details.

### PATCH 20–21 — Invalid / quarantine

- invalid market отдельно от phase;
- quarantine отдельно от phase.

### PATCH 22–23 — Backtest / calibration

- шаблон калибровки;
- кейсы false positive / false negative;
- подбор thresholds.

