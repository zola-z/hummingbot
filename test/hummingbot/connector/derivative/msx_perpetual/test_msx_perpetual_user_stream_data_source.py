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

    async def test_last_recv_time_zero_before_start(self):
        """未启动前 last_recv_time 应为 0(connector 尚未 ready)。"""
        source = self._make_source()
        self.assertEqual(source.last_recv_time, 0.0)

    async def test_last_recv_time_positive_after_start(self):
        """listen_for_user_stream 启动后 last_recv_time>0, 使 connector 能进入 ready。

        这是 MSX 无用户流 WS 的 ready 修复关键: 无此项 user_stream_initialized
        永远 False -> connector 永不 ready -> 策略不报价。
        """
        source = self._make_source()
        self.assertEqual(source.last_recv_time, 0.0)
        output: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(source.listen_for_user_stream(output))
        await asyncio.sleep(0.05)
        self.assertGreater(source.last_recv_time, 0.0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
