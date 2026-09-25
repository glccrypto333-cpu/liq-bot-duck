import sys
from pathlib import Path
sys.path.insert(0, '/home/alexey/openclaw/apps/liq-bot-duck')
from phase_service import apply_stage_guardrails, determine_target_stage, compute_transition_permission

PRICE_OK = ('цена_не_блокирует', 'нет', False, 3)
VOLUME = ('пустой', 'нейтрально')

def oi(a='flat', b='flat', h='flat', q='flat'):
    return {'oi_slope_class_15m':a,'oi_slope_class_30m':b,'oi_slope_class_1h':h,'oi_slope_class_4h':q}

def test_early_1_to_2_uses_strong_15m_30m_1h_after_15_minutes():
    s=oi('good_up','good_up','good_up','good_up')
    target,_=determine_target_stage(s,PRICE_OK,VOLUME)
    stage,reason=apply_stage_guardrails({'current_stage':1},target,s,PRICE_OK,VOLUME,15.0,15.0)
    assert target == 2
    assert stage == 2
    assert reason == 'переход_1_2:ранний_выпуск_A_15м_30м_1ч_подтвердили_набор'

def test_early_1_to_2_does_not_pass_before_15_minutes():
    s=oi('strong_up','strong_up','strong_up','good_up')
    target,_=determine_target_stage(s,PRICE_OK,VOLUME)
    stage,reason=apply_stage_guardrails({'current_stage':1},target,s,PRICE_OK,VOLUME,10.0,10.0)
    assert stage == 1
    assert reason == 'удержание_1:ждем_15_минут_для_раннего_коридора'

def test_transition_permission_exposes_early_1_to_2():
    s=oi('good_up','strong_up','good_up','good_up')
    permission=compute_transition_permission({'current_stage':1},2,15.0,s,PRICE_OK,15.0)
    assert permission == 'разрешен_ранний_вход_в_2'
