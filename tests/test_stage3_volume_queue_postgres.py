"""Real PostgreSQL contract tests for Duck's Stage-3 volume queue.

Set DUCK_QUEUE_TEST_DATABASE_URL only to a disposable, loopback-only database
whose name contains "test". The tests create and drop a uniquely named schema.
"""

import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row


@pytest.fixture
def isolated_queue_schema(monkeypatch):
    import db

    dsn = os.getenv("DUCK_QUEUE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DUCK_QUEUE_TEST_DATABASE_URL not configured")
    parsed = urlparse(dsn)
    db_name = parsed.path.rsplit("/", 1)[-1].lower()
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or "test" not in db_name:
        pytest.skip("integration tests require a loopback disposable database named *test*")

    schema = "duck_queue_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        admin.execute(sql.SQL("""
            CREATE TABLE {}.stage3_volume_queue(
                exchange TEXT NOT NULL, symbol TEXT NOT NULL,
                stage3_transition_ts TIMESTAMPTZ NOT NULL,
                queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), status TEXT NOT NULL,
                volume_unlocked_at TIMESTAMPTZ, growth_4h_pct DOUBLE PRECISION,
                quality_reason TEXT, volume_snapshot JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                terminal_at TIMESTAMPTZ, sent_at TIMESTAMPTZ,
                oi_1h_class TEXT, oi_cycle_ts TIMESTAMPTZ,
                PRIMARY KEY(exchange, symbol)
            )
        """).format(sql.Identifier(schema)))
        admin.execute(sql.SQL("""
            CREATE TABLE {}.stage3_volume_queue_observations(
                exchange TEXT NOT NULL, symbol TEXT NOT NULL,
                stage3_transition_ts TIMESTAMPTZ NOT NULL, observed_at TIMESTAMPTZ NOT NULL,
                source_exchange TEXT, source_symbol TEXT, data_source_cycle_ts TIMESTAMPTZ,
                volume_ready BOOLEAN NOT NULL DEFAULT FALSE, growth_4h_pct DOUBLE PRECISION,
                quality_reason TEXT, gate_status TEXT NOT NULL, queue_status TEXT NOT NULL,
                delivery_block_reason TEXT, volume_snapshot JSONB,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY(exchange, symbol, stage3_transition_ts, observed_at)
            )
        """).format(sql.Identifier(schema)))
        admin.execute(sql.SQL("CREATE TABLE {}.core_state_v2(exchange TEXT, symbol TEXT, current_stage INTEGER, PRIMARY KEY(exchange,symbol))").format(sql.Identifier(schema)))
        admin.execute(sql.SQL("CREATE TABLE {}.active_symbol_universe(exchange TEXT, symbol TEXT, PRIMARY KEY(exchange,symbol))").format(sql.Identifier(schema)))
        admin.execute(sql.SQL("CREATE TABLE {}.transition_history_v2(exchange TEXT, symbol TEXT, to_stage INTEGER, cycle_ts TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())").format(sql.Identifier(schema)))

    def test_connection():
        conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        return conn

    monkeypatch.setattr(db, "DATABASE_URL", dsn)
    monkeypatch.setattr(db, "_conn", test_connection)
    try:
        yield test_connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def _stage3_candidate(symbol, transition_at, observed_at, *, status="waiting_volume", growth=70.0):
    snapshot = {
        "source": "BINANCE",
        "source_cycle_ts": observed_at.isoformat(),
        "growth_4h_pct": growth,
        "current_4h_distribution_status": "ok",
    }
    return {
        "exchange": "BINANCE", "symbol": symbol, "source": "BINANCE", "source_symbol": symbol,
        "transition_ts": transition_at, "observed_at": observed_at,
        "status": status, "gate_status": "pass" if status == "unlocked" else "below_100pct",
        "ready": True, "volume_unlocked_at": observed_at if status == "unlocked" else None,
        "growth_4h_pct": growth, "quality_reason": "ready",
        "observation_snapshot": snapshot, "volume_snapshot": snapshot if status == "unlocked" else None,
    }


def test_postgres_queue_waits_unlocks_invalidates_and_deduplicates_observations(isolated_queue_schema):
    import db

    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    first_observation = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    unlock_at = datetime(2026, 9, 23, 8, 10, tzinfo=timezone.utc)
    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE','TESTUSDT',3)")
        conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE','TESTUSDT')")
        conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE','TESTUSDT',3,%s)", (transition_at,))

    waiting = _stage3_candidate("TESTUSDT", transition_at, first_observation)
    db.sync_stage3_volume_queue([waiting])
    db.sync_stage3_volume_queue([waiting])
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,volume_unlocked_at FROM stage3_volume_queue WHERE symbol='TESTUSDT'").fetchone()
        observations = conn.execute("SELECT COUNT(*) AS n FROM stage3_volume_queue_observations WHERE symbol='TESTUSDT'").fetchone()["n"]
    assert queue == {"status": "waiting_volume", "volume_unlocked_at": None}
    assert observations == 1

    unlocked = _stage3_candidate("TESTUSDT", transition_at, unlock_at, status="unlocked", growth=105.0)
    db.sync_stage3_volume_queue([unlocked])
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,volume_unlocked_at,growth_4h_pct,volume_snapshot FROM stage3_volume_queue WHERE symbol='TESTUSDT'").fetchone()
    assert queue["status"] == "unlocked"
    assert queue["volume_unlocked_at"] == unlock_at
    assert queue["growth_4h_pct"] == 105.0
    assert queue["volume_snapshot"]["source_cycle_ts"] == unlock_at.isoformat()

    below_threshold_after_unlock = _stage3_candidate(
        "TESTUSDT", transition_at, unlock_at + timedelta(minutes=5),
        status="unlocked", growth=92.0,
    )
    below_threshold_after_unlock["volume_unlocked_at"] = unlock_at
    db.sync_stage3_volume_queue([below_threshold_after_unlock])
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,volume_unlocked_at,growth_4h_pct,volume_snapshot FROM stage3_volume_queue WHERE symbol='TESTUSDT'").fetchone()
    assert queue["status"] == "unlocked"
    assert queue["volume_unlocked_at"] == unlock_at
    assert queue["growth_4h_pct"] == 92.0
    assert queue["volume_snapshot"]["source_cycle_ts"] == unlock_at.isoformat()

    with isolated_queue_schema() as conn:
        conn.execute("UPDATE core_state_v2 SET current_stage=1 WHERE symbol='TESTUSDT'")
    db.sync_stage3_volume_queue([])
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,terminal_at FROM stage3_volume_queue WHERE symbol='TESTUSDT'").fetchone()
    assert queue["status"] == "invalidated"
    assert queue["terminal_at"] is not None


def test_postgres_queue_applies_72h_retention(isolated_queue_schema):
    import db

    now = datetime.now(timezone.utc)
    old_transition = now - timedelta(days=5)
    old_observation = now - timedelta(hours=73)
    recent_observation = now - timedelta(hours=71)
    with isolated_queue_schema() as conn:
        conn.execute("""
            INSERT INTO stage3_volume_queue(exchange,symbol,stage3_transition_ts,status,terminal_at)
            VALUES ('BINANCE','OLDUSDT',%s,'invalidated',NOW()-INTERVAL '73 hours'),
                   ('BINANCE','RECENTUSDT',%s,'invalidated',NOW()-INTERVAL '71 hours')
        """, (old_transition, old_transition))
        conn.execute("""
            INSERT INTO stage3_volume_queue(exchange,symbol,stage3_transition_ts,status,terminal_at)
            VALUES ('BINANCE','OLD_OIUSDT',%s,'invalidated_oi1h',NOW()-INTERVAL '73 hours'),
                   ('BINANCE','RECENT_OIUSDT',%s,'invalidated_oi1h',NOW()-INTERVAL '71 hours')
        """, (old_transition, old_transition))
        conn.execute("""
            INSERT INTO stage3_volume_queue_observations(
                exchange,symbol,stage3_transition_ts,observed_at,gate_status,queue_status
            ) VALUES ('BINANCE','OLDUSDT',%s,%s,'waiting_volume','invalidated'),
                     ('BINANCE','RECENTUSDT',%s,%s,'waiting_volume','invalidated')
        """, (old_transition, old_observation, old_transition, recent_observation))

    db.sync_stage3_volume_queue([])
    with isolated_queue_schema() as conn:
        queue_symbols = {r["symbol"] for r in conn.execute("SELECT symbol FROM stage3_volume_queue").fetchall()}
        observation_symbols = {r["symbol"] for r in conn.execute("SELECT symbol FROM stage3_volume_queue_observations").fetchall()}
    assert queue_symbols == {"RECENTUSDT", "RECENT_OIUSDT"}
    assert observation_symbols == {"RECENTUSDT"}


def test_postgres_universe_block_marks_latest_observation_but_never_reopens_sent(isolated_queue_schema):
    import db

    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    observed_at = datetime(2026, 9, 23, 8, 10, tzinfo=timezone.utc)
    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE','BLOCKUSDT',3)")
        conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE','BLOCKUSDT')")
        conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE','BLOCKUSDT',3,%s)", (transition_at,))
    candidate = _stage3_candidate("BLOCKUSDT", transition_at, observed_at, status="unlocked", growth=110.0)
    db.sync_stage3_volume_queue([candidate])
    db.mark_stage3_volume_queue_blocked("BINANCE", "BLOCKUSDT", transition_at, "blocked:asset_class_stock")
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,terminal_at FROM stage3_volume_queue WHERE symbol='BLOCKUSDT'").fetchone()
        observation = conn.execute("SELECT queue_status,delivery_block_reason FROM stage3_volume_queue_observations WHERE symbol='BLOCKUSDT'").fetchone()
    assert queue["status"] == "blocked_universe"
    assert queue["terminal_at"] is not None
    assert observation == {
        "queue_status": "blocked_universe",
        "delivery_block_reason": "blocked:asset_class_stock",
    }

    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO stage3_volume_queue(exchange,symbol,stage3_transition_ts,status) VALUES ('BINANCE','SENTUSDT',%s,'sent')", (transition_at,))
    db.mark_stage3_volume_queue_blocked("BINANCE", "SENTUSDT", transition_at, "blocked:asset_class_stock")
    with isolated_queue_schema() as conn:
        sent = conn.execute("SELECT status,terminal_at FROM stage3_volume_queue WHERE symbol='SENTUSDT'").fetchone()
    assert sent["status"] == "sent"
    assert sent["terminal_at"] is None


def test_postgres_mark_sent_is_terminal_for_repeated_candidate_sync(isolated_queue_schema):
    import db

    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    observed_at = datetime(2026, 9, 23, 8, 10, tzinfo=timezone.utc)
    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE','SENTSYNCUSDT',3)")
        conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE','SENTSYNCUSDT')")
        conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE','SENTSYNCUSDT',3,%s)", (transition_at,))
    unlocked = _stage3_candidate("SENTSYNCUSDT", transition_at, observed_at, status="unlocked", growth=120.0)
    db.sync_stage3_volume_queue([unlocked])
    db.mark_stage3_volume_queue_sent("BINANCE", "SENTSYNCUSDT", transition_at)
    db.sync_stage3_volume_queue([_stage3_candidate(
        "SENTSYNCUSDT", transition_at, observed_at + timedelta(minutes=5),
        status="sent", growth=40.0,
    )])
    with isolated_queue_schema() as conn:
        sent = conn.execute("SELECT status,sent_at,terminal_at FROM stage3_volume_queue WHERE symbol='SENTSYNCUSDT'").fetchone()
    assert sent["status"] == "sent"
    assert sent["sent_at"] is not None
    assert sent["terminal_at"] is not None


def test_postgres_queue_invalidates_only_pre_unlock_oi_decline_and_never_resurrects(isolated_queue_schema):
    import db
    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    weak_at = transition_at + timedelta(minutes=20)
    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE','WEAKUSDT',3)")
        conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE','WEAKUSDT')")
        conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE','WEAKUSDT',3,%s)", (transition_at,))
    waiting = _stage3_candidate("WEAKUSDT", transition_at, weak_at)
    waiting.update(oi_1h_class="weak_down", oi_cycle_ts=weak_at)
    db.sync_stage3_volume_queue([waiting])
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,terminal_at,oi_1h_class,oi_cycle_ts FROM stage3_volume_queue WHERE symbol='WEAKUSDT'").fetchone()
    assert queue["status"] == "invalidated_oi1h"
    assert queue["terminal_at"] is not None
    assert queue["oi_1h_class"] == "weak_down"
    assert queue["oi_cycle_ts"] == weak_at
    with isolated_queue_schema() as conn:
        observation = conn.execute("SELECT queue_status,delivery_block_reason FROM stage3_volume_queue_observations WHERE symbol='WEAKUSDT'").fetchone()
    assert observation == {
        "queue_status": "invalidated_oi1h",
        "delivery_block_reason": "blocked:oi_1h_decline_before_volume",
    }
    rebound = _stage3_candidate("WEAKUSDT", transition_at, weak_at + timedelta(minutes=5), status="unlocked", growth=110.0)
    rebound.update(oi_1h_class="good_up", oi_cycle_ts=weak_at + timedelta(minutes=5))
    db.sync_stage3_volume_queue([rebound])
    with isolated_queue_schema() as conn:
        assert conn.execute("SELECT status FROM stage3_volume_queue WHERE symbol='WEAKUSDT'").fetchone()["status"] == "invalidated_oi1h"

    new_transition = transition_at + timedelta(hours=1)
    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE','WEAKUSDT',3,%s)", (new_transition,))
    fresh_candidate = _stage3_candidate("WEAKUSDT", new_transition, new_transition + timedelta(minutes=5))
    fresh_candidate.update(oi_1h_class="good_up", oi_cycle_ts=new_transition + timedelta(minutes=5))
    db.sync_stage3_volume_queue([fresh_candidate])
    with isolated_queue_schema() as conn:
        queue = conn.execute("SELECT status,terminal_at,stage3_transition_ts FROM stage3_volume_queue WHERE symbol='WEAKUSDT'").fetchone()
    assert queue == {
        "status": "waiting_volume",
        "terminal_at": None,
        "stage3_transition_ts": new_transition,
    }


def test_postgres_queue_oi_decline_after_volume_unlock_does_not_invalidate_and_tie_does(isolated_queue_schema):
    import db
    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    unlock_at = transition_at + timedelta(minutes=10)
    with isolated_queue_schema() as conn:
        for symbol in ("AFTERUSDT", "TIEUSDT"):
            conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE',%s,3)", (symbol,))
            conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE',%s)", (symbol,))
            conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE',%s,3,%s)", (symbol, transition_at))
    unlocked = _stage3_candidate("AFTERUSDT", transition_at, unlock_at, status="unlocked", growth=105.0)
    db.sync_stage3_volume_queue([unlocked])
    later_decline = _stage3_candidate("AFTERUSDT", transition_at, unlock_at + timedelta(minutes=5), status="unlocked", growth=110.0)
    later_decline["volume_unlocked_at"] = unlock_at
    later_decline.update(oi_1h_class="strong_down", oi_cycle_ts=unlock_at + timedelta(minutes=5))
    db.sync_stage3_volume_queue([later_decline])
    tie = _stage3_candidate("TIEUSDT", transition_at, unlock_at, status="unlocked", growth=105.0)
    tie.update(oi_1h_class="weak_down", oi_cycle_ts=unlock_at)
    db.sync_stage3_volume_queue([tie])
    with isolated_queue_schema() as conn:
        rows = {r["symbol"]: r["status"] for r in conn.execute("SELECT symbol,status FROM stage3_volume_queue").fetchall()}
    assert rows == {"AFTERUSDT": "unlocked", "TIEUSDT": "invalidated_oi1h"}


def test_postgres_queue_missing_or_stale_oi_does_not_invalidate(isolated_queue_schema):
    import db
    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    observed_at = transition_at + timedelta(minutes=5)
    with isolated_queue_schema() as conn:
        conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE','NOOIUSDT',3)")
        conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE','NOOIUSDT')")
        conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE','NOOIUSDT',3,%s)", (transition_at,))
    candidate = _stage3_candidate("NOOIUSDT", transition_at, observed_at)
    candidate.update(oi_1h_class=None, oi_cycle_ts=None)
    db.sync_stage3_volume_queue([candidate])
    with isolated_queue_schema() as conn:
        assert conn.execute("SELECT status FROM stage3_volume_queue WHERE symbol='NOOIUSDT'").fetchone()["status"] == "waiting_volume"



def test_postgres_price_decline_at_first_unlock_is_terminal_but_later_decline_is_not(isolated_queue_schema):
    import db

    transition_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    unlock_at = transition_at + timedelta(minutes=10)
    later_at = unlock_at + timedelta(minutes=5)
    with isolated_queue_schema() as conn:
        for symbol in ("PRICEVETOUSDT", "LATEPRICEUSDT"):
            conn.execute("INSERT INTO core_state_v2 VALUES ('BINANCE',%s,3)", (symbol,))
            conn.execute("INSERT INTO active_symbol_universe VALUES ('BINANCE',%s)", (symbol,))
            conn.execute("INSERT INTO transition_history_v2(exchange,symbol,to_stage,cycle_ts) VALUES ('BINANCE',%s,3,%s)", (symbol, transition_at))

    first_unlock = _stage3_candidate("PRICEVETOUSDT", transition_at, unlock_at, status="invalidated_price", growth=105.0)
    first_unlock["volume_unlocked_at"] = unlock_at
    first_unlock["delivery_block_reason"] = "blocked:price_30m_down_at_volume_unlock"
    first_unlock["volume_snapshot"].update({
        "price_30m_class": "weak_down",
        "price_1h_class": "good_up",
        "price_cycle_ts": unlock_at.isoformat(),
    })
    first_unlock["observation_snapshot"].update(first_unlock["volume_snapshot"])
    db.sync_stage3_volume_queue([first_unlock])
    with isolated_queue_schema() as conn:
        saved = conn.execute("SELECT status,volume_unlocked_at,terminal_at FROM stage3_volume_queue WHERE symbol='PRICEVETOUSDT'").fetchone()
        observation = conn.execute("SELECT queue_status,delivery_block_reason FROM stage3_volume_queue_observations WHERE symbol='PRICEVETOUSDT'").fetchone()
    assert saved["status"] == "invalidated_price"
    assert saved["volume_unlocked_at"] == unlock_at
    assert saved["terminal_at"] is not None
    assert observation == {
        "queue_status": "invalidated_price",
        "delivery_block_reason": "blocked:price_30m_down_at_volume_unlock",
    }

    rebound = _stage3_candidate("PRICEVETOUSDT", transition_at, later_at, status="unlocked", growth=120.0)
    rebound["volume_unlocked_at"] = later_at
    db.sync_stage3_volume_queue([rebound])
    with isolated_queue_schema() as conn:
        assert conn.execute("SELECT status FROM stage3_volume_queue WHERE symbol='PRICEVETOUSDT'").fetchone()["status"] == "invalidated_price"

    initially_unlocked = _stage3_candidate("LATEPRICEUSDT", transition_at, unlock_at, status="unlocked", growth=105.0)
    db.sync_stage3_volume_queue([initially_unlocked])
    later_decline = _stage3_candidate("LATEPRICEUSDT", transition_at, later_at, status="invalidated_price", growth=110.0)
    later_decline["volume_unlocked_at"] = later_at
    later_decline["delivery_block_reason"] = "blocked:price_1h_down_at_volume_unlock"
    later_decline["volume_snapshot"].update({
        "price_30m_class": "good_up",
        "price_1h_class": "strong_down",
        "price_cycle_ts": later_at.isoformat(),
    })
    later_decline["observation_snapshot"].update(later_decline["volume_snapshot"])
    db.sync_stage3_volume_queue([later_decline])
    with isolated_queue_schema() as conn:
        saved = conn.execute("SELECT status,volume_unlocked_at FROM stage3_volume_queue WHERE symbol='LATEPRICEUSDT'").fetchone()
    assert saved == {"status": "unlocked", "volume_unlocked_at": unlock_at}
