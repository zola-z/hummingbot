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

    async def _connected_websocket_assistant(self) -> WSAssistant:
        # MSX 无私有用户流, 不应被调用。
        raise NotImplementedError("MSX has no private user-stream WebSocket; updates come via REST polling.")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        # 无私有频道可订阅。
        return

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """MSX 无用户流推送: 永久挂起, 不产出消息。

        connector 主类的 REST 轮询负责订单/余额/持仓更新。
        """
        while True:
            try:
                await asyncio.sleep(CONSTANTS.HEARTBEAT_TIME_INTERVAL)
            except asyncio.CancelledError:
                raise
