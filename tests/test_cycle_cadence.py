from cycle_cadence import should_run_this_cycle, should_run_maintenance_this_cycle


def test_runs_every_cycle_when_period_is_one() -> None:
    assert should_run_this_cycle(1, 1) is True
    assert should_run_this_cycle(2, 1) is True
    assert should_run_this_cycle(10, 0) is True


def test_decision_steps_can_still_run_on_first_cycle_even_with_sparse_period() -> None:
    assert should_run_this_cycle(1, 6) is True


def test_skips_middle_cycles_until_period_boundary() -> None:
    assert should_run_this_cycle(2, 6) is False
    assert should_run_this_cycle(5, 6) is False
    assert should_run_this_cycle(6, 6) is True


def test_maintenance_steps_do_not_run_on_first_cycle_when_period_is_sparse() -> None:
    assert should_run_maintenance_this_cycle(1, 6) is False
    assert should_run_maintenance_this_cycle(2, 6) is False
    assert should_run_maintenance_this_cycle(5, 6) is False
    assert should_run_maintenance_this_cycle(6, 6) is True


def test_maintenance_steps_run_each_cycle_when_period_is_one() -> None:
    assert should_run_maintenance_this_cycle(1, 1) is True
    assert should_run_maintenance_this_cycle(2, 1) is True
