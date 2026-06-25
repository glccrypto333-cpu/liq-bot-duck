from __future__ import annotations


def should_run_this_cycle(cycle_no: int, every_cycles: int) -> bool:
    if every_cycles <= 1:
        return True
    if cycle_no <= 1:
        return True
    return cycle_no % every_cycles == 0


def should_run_maintenance_this_cycle(cycle_no: int, every_cycles: int) -> bool:
    if every_cycles <= 1:
        return True
    if cycle_no <= 1:
        return False
    return cycle_no % every_cycles == 0
