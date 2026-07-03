from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_rate_controller import (
    AdaptiveRateController,
)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def _controller(clock, **kw):
    params = dict(floor_rps=2.0, ceiling_rps=10.0, initial_rps=9.0, decrease_factor=0.7,
                  safety_factor=0.9, recovery_step=1.0, recovery_interval_s=1.0,
                  cooldown_s=2.0, time_fn=clock)
    params.update(kw)
    return AdaptiveRateController(**params)


def test_on_429_multiplicative_decrease():
    c = _controller(FakeClock())
    assert c.effective_rps == 9.0
    c.on_429()
    assert abs(c.effective_rps - 6.3) < 1e-9  # 9 * 0.7


def test_on_429_not_below_floor():
    c = _controller(FakeClock(), initial_rps=3.0)
    c.on_429()
    c.on_429()
    c.on_429()
    assert c.effective_rps == 2.0  # floor


import pytest


def test_probed_ceiling_recorded_and_caps_recovery():
    clock = FakeClock()
    c = _controller(clock)
    c.on_429()  # probed=9.0, effective=6.3
    assert c.snapshot()["probed_ceiling"] == 9.0
    clock.advance(2.0)  # 过 cooldown
    for _ in range(20):
        clock.advance(1.0)
        c._maybe_recover()
    # 恢复上限 = min(initial=9, probed 9*0.9=8.1) = 8.1
    assert abs(c.effective_rps - 8.1) < 1e-9


def test_steady_state_no_probing_when_never_429():
    clock = FakeClock()
    c = _controller(clock)  # initial=9
    for _ in range(20):
        clock.advance(1.0)
        c._maybe_recover()
    assert c.effective_rps == 9.0  # 稳在 initial, 不往上试探(ceiling=10 也不撞)


def test_additive_recovery_paced_by_time():
    clock = FakeClock()
    c = _controller(clock, initial_rps=9.0)
    c.on_429()  # effective 6.3
    clock.advance(2.0)  # cooldown 过
    clock.advance(1.0)
    c._maybe_recover()
    assert abs(c.effective_rps - 7.3) < 1e-9  # +1
    clock.advance(1.0)
    c._maybe_recover()
    assert abs(c.effective_rps - 8.1) < 1e-9  # +1 -> 撞 recovery_cap 8.1 停


def test_backoff_freezes_recovery():
    clock = FakeClock()
    c = _controller(clock)
    c.on_429(retry_after=5.0)
    assert c.is_backing_off
    clock.advance(1.0)
    before = c.effective_rps
    c._maybe_recover()
    assert c.effective_rps == before  # backoff 内不恢复


def test_retry_after_overrides_cooldown():
    clock = FakeClock()
    c = _controller(clock, cooldown_s=2.0)
    c.on_429(retry_after=10.0)
    clock.advance(3.0)
    assert c.is_backing_off  # 10s Retry-After 优先于 2s cooldown


@pytest.mark.asyncio
async def test_acquire_paces_by_effective_rps():
    clock = FakeClock()
    c = _controller(clock, initial_rps=10.0)  # min_interval 0.1s
    slept = []

    async def fake_sleep(d):
        slept.append(d)
        clock.advance(d)

    import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_rate_controller as mod
    orig = mod.asyncio.sleep
    mod.asyncio.sleep = fake_sleep
    try:
        await c.acquire()          # 第一次: last_grant=0 附近, 不必等
        clock.advance(0.02)        # 只过 0.02s
        await c.acquire()          # 需再等 ~0.08s
    finally:
        mod.asyncio.sleep = orig
    assert any(abs(s - 0.08) < 0.02 for s in slept)


def test_ceiling_100_allows_climb_not_hardcoded():
    clock = FakeClock()
    c = _controller(clock, ceiling_rps=100.0, initial_rps=90.0, safety_factor=0.9)
    assert c.effective_rps == 90.0
    c.on_429()  # probed=90, effective=63
    clock.advance(2.0)
    for _ in range(40):
        clock.advance(1.0)
        c._maybe_recover()
    # 恢复上限 min(initial=90, probed 90*0.9=81)=81
    assert abs(c.effective_rps - 81.0) < 1e-9
