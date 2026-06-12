"""Hummingbot V2 自动发现入口。

V2 框架扫描本模块, 找到唯一的 StrategyV2Base 子类(ProfitFirstXemmStrategy)
与唯一的 StrategyV2ConfigBase 子类(ProfitFirstXemmV2Config), 自动构造运行。
配置来自 conf/scripts/<name>.yml 或默认值; Redis 仅运行时热更新。

需要把策略包加入 sys.path(策略代码在主仓库 engine/strategies/, 不在 submodule 内)。
"""

import os
import sys

# engine/hummingbot/scripts/ -> 仓库根 = 上三级
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from engine.strategies.profit_first_xemm.strategy import (  # noqa: E402
    ProfitFirstXemmStrategy,
    ProfitFirstXemmV2Config,
)

__all__ = ["ProfitFirstXemmStrategy", "ProfitFirstXemmV2Config"]
