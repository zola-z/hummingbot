import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_user_stream_data_source import (
    MsxPerpetualUserStreamDataSource,
)


class MsxPerpetualUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    """MSX 无私有用户流 WS, 数据源是空桩(更新走 REST 轮询)。"""

    def _make_source(self) -> MsxPerpetualUserStreamDataSource:
        return MsxPerpetualUserStreamDataSource(
            auth=MagicMock(),
            connector=MagicMock(),
            api_factory=MagicMock(),
            domain="msx_perpetual_testnet",
        )

    async def test_connected_websocket_assistant_not_implemented(self):
        source = self._make_source()
        with self.assertRaises(NotImplementedError):
            await source._connected_websocket_assistant()

    async def test_subscribe_channels_is_noop(self):
        source = self._make_source()
        # 不应抛异常
        await source._subscribe_channels(MagicMock())

    async def test_listen_for_user_stream_produces_no_messages(self):
        source = self._make_source()
        output: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(source.listen_for_user_stream(output))
        await asyncio.sleep(0.05)
        self.assertTrue(output.empty())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
