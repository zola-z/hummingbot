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


@pytest.mark.asyncio
async def test_acquire_reserves_grant_slots_before_await():
    """令牌桶预约: 占位(推进 _last_grant_ts)必须发生在 await 之前。

    两次顺序 acquire 应把 _last_grant_ts 各推进一个 min_interval, 且单调递增。
    这验证 grant 时刻是被"预约"的(串行错开), 而非读当前时钟。
    """
    clock = FakeClock()
    c = _controller(clock, initial_rps=10.0)  # min_interval 0.1s

    async def fake_sleep(d):
        clock.advance(d)

    import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_rate_controller as mod
    orig = mod.asyncio.sleep
    mod.asyncio.sleep = fake_sleep
    try:
        await c.acquire()
        first = c._last_grant_ts
        await c.acquire()
        second = c._last_grant_ts
    finally:
        mod.asyncio.sleep = orig
    # 第二次预约相对第一次推进了一个 min_interval, 且单调递增(错开, 不是同一时刻)。
    assert second > first
    assert abs((second - first) - 0.1) < 1e-9


@pytest.mark.asyncio
async def test_acquire_concurrent_burst_is_staggered():
    """核心 I1: N 个并发 acquire 的 grant 时刻必须按 min_interval 错开, 而非挤成 burst。

    旧实现下 N 个并发 acquire 读到同一 _last_grant_ts, 算出同一 wait, 同睡同醒 ->
    全部 grant 时刻挤在一起(间隔 ~0)。新实现(预约)下应各自错开 >= min_interval。
    """
    import asyncio as _asyncio

    clock = FakeClock()
    c = _controller(clock, initial_rps=10.0)  # min_interval 0.1s
    grant_times = []

    import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_rate_controller as mod
    orig = mod.asyncio.sleep

    async def fake_sleep(d):
        # 假 sleep: 在进入 sleep 的这一刻, acquire 已把 _last_grant_ts 推进到本协程预约的
        # grant 时刻(占位在 await 之前发生)。记录它 -> 即"我的"预约放行时刻。然后让出控制权
        # (真实 sleep(0)) 让下一个 acquire 得以在更晚的 _last_grant_ts 基础上预约, 从而错开。
        grant_times.append(c._last_grant_ts)
        await orig(0)

    mod.asyncio.sleep = fake_sleep

    async def one():
        await c.acquire()

    try:
        await _asyncio.gather(*[one() for _ in range(5)])
    finally:
        mod.asyncio.sleep = orig

    reserved = sorted(grant_times)
    # 5 个预约时刻两两相邻间隔 >= min_interval(错开), 而不是全部相同(burst)。
    for earlier, later in zip(reserved, reserved[1:]):
        assert later - earlier >= 0.1 - 1e-9
    # 且跨度应 ~ (N-1)*interval = 0.4s, 明显大于旧 burst 的 ~0。
    assert reserved[-1] - reserved[0] >= 0.4 - 1e-9


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
