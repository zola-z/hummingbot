import asyncio
from typing import TYPE_CHECKING, Optional

import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_auth import MsxPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_derivative import (
        MsxPerpetualDerivative,
    )


class MsxPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """MSX 没有私有用户流 WebSocket(docs/connectors/msx-api-notes.md 第3节: WS 仅公共行情)。

    因此这里是一个最小空实现: 不连接任何私有 WS, 不产出用户流消息。
    订单状态/余额/持仓 的更新改由 connector 主类用 REST 轮询完成
    (当前委托 /order/limit、历史订单 /order/history、当前持仓 /position/current)。
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: MsxPerpetualAuth,
            connector: 'MsxPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._connector = connector
        # MSX 无用户流 WS, 无 ws_assistant.last_recv_time。用本地时间戳表示
        # "用户流子系统已就绪(REST 轮询接管)", 供 connector 的 ready 判据使用。
        # 0 = 尚未启动; >0 = 已启动并存活。
        self._last_recv_time: float = 0.0

    @property
    def last_recv_time(self) -> float:
        """override 基类: MSX 无 WS ws_assistant, 用本地保活时间戳。

        基类默认从 self._ws_assistant.last_recv_time 取; MSX 从不建立 ws_assistant,
        会恒为 0, 使 connector 的 user_stream_initialized 永远 False -> 永不 ready。
        这里返回 listen_for_user_stream 启动后持续刷新的本地时间戳, 语义上表示
        "用户流子系统已就绪(由 REST 轮询代替 WS 推送)"。
        """
        return self._last_recv_time

    async def _connected_websocket_assistant(self) -> WSAssistant:
        # MSX 无私有用户流, 不应被调用。
        raise NotImplementedError("MSX has no private user-stream WebSocket; updates come via REST polling.")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        # 无私有频道可订阅。
        return

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """MSX 无用户流推送: 不产出消息, 但持续刷新保活时间戳。

        connector 主类的 REST 轮询负责订单/余额/持仓更新。这里一旦启动即把
        last_recv_time 置为当前时间, 使 connector 能进入 ready; 并周期性刷新表示存活。
        """
        # 启动即标记就绪, 让 connector 尽快 ready(不必等第一个心跳间隔)。
        self._last_recv_time = self._time()
        while True:
            try:
                await asyncio.sleep(CONSTANTS.HEARTBEAT_TIME_INTERVAL)
                self._last_recv_time = self._time()
            except asyncio.CancelledError:
                raise
