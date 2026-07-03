import asyncio
import time
from typing import Optional


class AdaptiveRateController:
    """MSX 自适应限流控制器(配置为主 + AIMD 兜底)。

    正常态: 稳定运行在 initial_rps(≈ceiling×0.9), 不主动往上试探 -> 零 429。
    遇 429: 乘性降速 ×decrease_factor + 记 _probed_ceiling + 进入 cooldown。
    恢复: 平稳期按 recovery_interval 每次 +recovery_step, 上限 min(initial, probed×safety)。
    """

    def __init__(
        self,
        floor_rps: float = 2.0,
        ceiling_rps: float = 10.0,
        initial_rps: float = 9.0,
        decrease_factor: float = 0.7,
        safety_factor: float = 0.9,
        recovery_step: float = 1.0,
        recovery_interval_s: float = 1.0,
        cooldown_s: float = 2.0,
        time_fn=time.monotonic,
    ):
        self._floor = floor_rps
        # ceiling_rps 仅用于把 initial 夹到硬上限(局部参数即可, 不存字段——effective 的实际上限
        # 由 _recovery_cap 用 initial/_probed_ceiling 决定, 无处再读 self._ceiling)。
        self._initial = min(initial_rps, ceiling_rps)
        self._decrease_factor = decrease_factor
        self._safety_factor = safety_factor
        self._recovery_step = recovery_step
        self._recovery_interval_s = recovery_interval_s
        self._cooldown_s = cooldown_s
        self._now = time_fn

        self._effective_rps = self._initial
        self._probed_ceiling: Optional[float] = None
        self._backing_off_until = 0.0
        self._last_recovery_ts = self._now()
        self._last_grant_ts = 0.0

    @property
    def effective_rps(self) -> float:
        return self._effective_rps

    @property
    def is_backing_off(self) -> bool:
        return self._now() < self._backing_off_until

    def on_429(self, retry_after: Optional[float] = None) -> None:
        self._probed_ceiling = self._effective_rps
        self._effective_rps = max(self._floor, self._effective_rps * self._decrease_factor)
        cooldown = retry_after if retry_after is not None else self._cooldown_s
        self._backing_off_until = self._now() + cooldown

    def on_success(self) -> None:
        # 不立即加速; 恢复由 _maybe_recover 按时间节奏做。
        return

    def _recovery_cap(self) -> float:
        if self._probed_ceiling is None:
            return self._initial
        return min(self._initial, self._probed_ceiling * self._safety_factor)

    def _maybe_recover(self) -> None:
        if self.is_backing_off:
            return
        if self._now() - self._last_recovery_ts >= self._recovery_interval_s:
            cap = self._recovery_cap()
            self._effective_rps = min(cap, self._effective_rps + self._recovery_step)
            self._last_recovery_ts = self._now()

    async def acquire(self) -> None:
        self._maybe_recover()
        min_interval = 1.0 / self._effective_rps if self._effective_rps > 0 else 0.0
        now = self._now()
        # 令牌桶预约: 计算"我的"grant 时刻并在 await 之前就把 _last_grant_ts 推进到它(占位)。
        # 这样 N 个并发 acquire 会各自预约到 t0, t0+interval, t0+2*interval... 错开放行,
        # 而不是全部读到同一 _last_grant_ts、算出同一 wait、同睡同醒挤成一个 burst 绕过节流。
        grant_at = max(now, self._last_grant_ts + min_interval)
        self._last_grant_ts = grant_at
        wait = grant_at - now
        if wait > 0:
            await asyncio.sleep(wait)

    def snapshot(self) -> dict:
        return {
            "effective_rps": self._effective_rps,
            "is_backing_off": self.is_backing_off,
            "probed_ceiling": self._probed_ceiling,
        }
