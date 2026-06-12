import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.msx_perpetual import msx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_api_order_book_data_source import (
    MsxPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class MsxPerpetualAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # The MSX derivative connector pulls in heavy transitive deps (yaml, aioprocessing, ...)
    # which are not part of the data-source unit under test. Per the task constraints we
    # construct the data source against a MagicMock connector exposing the async methods the
    # data source actually calls. This keeps the tests focused on the order-book data source.
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"
        cls.domain = "msx_perpetual_testnet"

    async def asyncSetUp(self) -> None:
        self.log_records = []
        self.listening_task = None
        self.async_tasks: List[asyncio.Task] = []

        self.symbol_to_pair = {self.ex_trading_pair: self.trading_pair}
        self.pair_to_symbol = {self.trading_pair: self.ex_trading_pair}

        async def _exchange_symbol(trading_pair: str) -> str:
            return self.pair_to_symbol[trading_pair]

        async def _trading_pair(symbol: str) -> str:
            return self.symbol_to_pair[symbol]

        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(side_effect=_exchange_symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=_trading_pair)
        self.connector._api_get = AsyncMock()
        self.connector.get_last_traded_prices = AsyncMock()

        self.api_factory = MagicMock()

        self.data_source = MsxPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        for task in self.async_tasks:
            task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def resume_test_callback(self, *_, **__):
        self.resume_test_event.set()
        return None

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    # ---- MSX message fixtures -------------------------------------------------

    def _snapshot_data(self) -> Dict[str, Any]:
        # The connector unwraps the {"code","msg","data"} envelope and returns `data`.
        return {
            "symbol": self.ex_trading_pair,
            "bids": [["43614.31", "1.5"]],
            "asks": [["45277.14", "0.257"]],
            "timestamp": 1631591424189,
            "id": 1027024,
        }

    def _order_book_update_event(self) -> Dict[str, Any]:
        # Shape A: action/result diff message
        return {
            "action": "order_book_update",
            "result": {
                "s": self.ex_trading_pair,
                "U": 1000,
                "u": 1005,
                "b": [["43614.31", "0.000"]],
                "a": [["45277.14", "0.257"]],
                "t": 1631591424198,
            },
        }

    def _order_book_snapshot_event(self) -> Dict[str, Any]:
        # Shape B: event=data / channel=order_book
        return {
            "event": "data",
            "channel": "order_book",
            "data": {
                "symbol": self.ex_trading_pair,
                "bids": [["43614.31", "1.5"]],
                "asks": [["45277.14", "0.257"]],
                "timestamp": 1631591424189,
                "id": 2048,
            },
            "time": 1631591424200,
        }

    def _book_ticker_event(self) -> Dict[str, Any]:
        # Shape A: action/result BBO message
        return {
            "action": "book_ticker",
            "result": {
                "s": self.ex_trading_pair,
                "u": 1010,
                "b": "43614.31",
                "B": "1.0",
                "a": "45277.14",
                "A": "0.5",
                "t": 1631591424300,
            },
        }

    def _ticker_event(self) -> Dict[str, Any]:
        # Shape B: event=data / channel=ticker
        return {
            "event": "data",
            "stream": f"{self.ex_trading_pair}@ticker",
            "channel": "ticker",
            "timestamp": 1631591424400,
            "data": {
                "symbol": self.ex_trading_pair,
                "lastPrice": "45000.00",
                "markPrice": "45010.00",
                "priceChange": "10.0",
                "priceChangePercent": "0.02",
                "high24h": "46000.00",
                "low24h": "44000.00",
                "volume24h": "1000.0",
                "timestamp": 1631591424400,
            },
        }

    def _pong_event(self) -> Dict[str, Any]:
        return {"event": "pong", "timestamp": 1631591424500}

    # ---- REST snapshot --------------------------------------------------------

    async def test_request_order_book_snapshot_uses_symbol_path(self):
        self.connector._api_get.return_value = self._snapshot_data()

        result = await self.data_source._request_order_book_snapshot(trading_pair=self.trading_pair)

        self.assertEqual(self._snapshot_data(), result)
        call_kwargs = self.connector._api_get.call_args.kwargs
        self.assertEqual(CONSTANTS.SNAPSHOT_PATH_URL + "/" + self.ex_trading_pair, call_kwargs["path_url"])
        # depth + with_id requested for incremental sync
        self.assertEqual("100", str(call_kwargs["params"]["depth"]))
        self.assertTrue(call_kwargs["params"]["with_id"] in (True, "true"))

    async def test_order_book_snapshot_parsed_with_data_id(self):
        self.connector._api_get.return_value = self._snapshot_data()

        snapshot_msg = await self.data_source._order_book_snapshot(trading_pair=self.trading_pair)

        self.assertIsInstance(snapshot_msg, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_msg.type)
        self.assertTrue(snapshot_msg.has_update_id)
        self.assertEqual(1027024, snapshot_msg.update_id)
        self.assertEqual(self.trading_pair, snapshot_msg.content["trading_pair"])
        self.assertEqual([["43614.31", "1.5"]], snapshot_msg.content["bids"])
        self.assertEqual([["45277.14", "0.257"]], snapshot_msg.content["asks"])

    async def test_get_new_order_book(self):
        self.connector._api_get.return_value = self._snapshot_data()

        result = await self.data_source.get_new_order_book(trading_pair=self.trading_pair)
        self.assertIsInstance(result, OrderBook)
        self.assertEqual(1027024, result.snapshot_uid)

    # ---- Diff parse -----------------------------------------------------------

    async def test_parse_order_book_diff_message(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(
            raw_message=self._order_book_update_event(), message_queue=msg_queue)

        result: OrderBookMessage = await msg_queue.get()
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.DIFF, result.type)
        self.assertTrue(result.has_update_id)
        self.assertEqual(1005, result.update_id)
        self.assertEqual(self.trading_pair, result.content["trading_pair"])
        self.assertEqual([["43614.31", "0.000"]], result.content["bids"])
        self.assertEqual([["45277.14", "0.257"]], result.content["asks"])

    # ---- Snapshot parse from WS ----------------------------------------------

    async def test_parse_order_book_snapshot_message(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        await self.data_source._parse_order_book_snapshot_message(
            raw_message=self._order_book_snapshot_event(), message_queue=msg_queue)

        result: OrderBookMessage = await msg_queue.get()
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, result.type)
        self.assertTrue(result.has_update_id)
        self.assertEqual(2048, result.update_id)
        self.assertEqual(self.trading_pair, result.content["trading_pair"])

    # ---- Channel router (both envelope shapes) --------------------------------

    async def test_channel_originating_message_diff(self):
        channel = self.data_source._channel_originating_message(self._order_book_update_event())
        self.assertEqual(self.data_source._diff_messages_queue_key, channel)

    async def test_channel_originating_message_snapshot(self):
        channel = self.data_source._channel_originating_message(self._order_book_snapshot_event())
        self.assertEqual(self.data_source._snapshot_messages_queue_key, channel)

    async def test_channel_originating_message_book_ticker_ignored_for_orderbook(self):
        channel = self.data_source._channel_originating_message(self._book_ticker_event())
        self.assertNotIn(channel, self.data_source._get_messages_queue_keys())

    async def test_channel_originating_message_ticker_ignored_for_orderbook(self):
        channel = self.data_source._channel_originating_message(self._ticker_event())
        self.assertNotIn(channel, self.data_source._get_messages_queue_keys())

    async def test_channel_originating_message_pong_ignored(self):
        channel = self.data_source._channel_originating_message(self._pong_event())
        self.assertNotIn(channel, self.data_source._get_messages_queue_keys())

    # ---- Subscriptions (single WS) --------------------------------------------

    async def test_subscribe_channels_sends_single_envelope(self):
        mock_ws = AsyncMock()
        await self.data_source._subscribe_channels(mock_ws)

        self.assertEqual(1, mock_ws.send.call_count)
        payload = mock_ws.send.call_args[0][0].payload
        self.assertEqual(CONSTANTS.WS_ACTION_SUBSCRIBE, payload["action"])
        self.assertIn(f"{self.ex_trading_pair}@{CONSTANTS.WS_ORDER_BOOK_UPDATE}", payload["streams"])
        self.assertIn(f"{self.ex_trading_pair}@{CONSTANTS.WS_BOOK_TICKER}", payload["streams"])

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book streams...")
        )

    async def test_subscribe_to_trading_pair_successful(self):
        new_pair = "ETH-USDT"
        ex_new_pair = "ETHUSDT"
        self.pair_to_symbol[new_pair] = ex_new_pair
        self.symbol_to_pair[ex_new_pair] = new_pair

        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertTrue(result)
        self.assertEqual(1, mock_ws.send.call_count)
        self.assertIn(new_pair, self.data_source._trading_pairs)

    async def test_subscribe_to_trading_pair_websocket_not_connected(self):
        new_pair = "ETH-USDT"
        self.data_source._ws_assistant = None
        result = await self.data_source.subscribe_to_trading_pair(new_pair)
        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", f"Cannot subscribe to {new_pair}: WebSocket not connected")
        )

    async def test_subscribe_to_trading_pair_raises_exception_and_logs_error(self):
        new_pair = "ETH-USDT"
        ex_new_pair = "ETHUSDT"
        self.pair_to_symbol[new_pair] = ex_new_pair
        self.symbol_to_pair[ex_new_pair] = new_pair

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)
        self.assertFalse(result)
        self.assertTrue(self._is_logged("ERROR", f"Error subscribing to {new_pair}"))

    async def test_unsubscribe_from_trading_pair_successful(self):
        self.assertIn(self.trading_pair, self.data_source._trading_pairs)
        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertTrue(result)
        self.assertEqual(1, mock_ws.send.call_count)
        payload = mock_ws.send.call_args[0][0].payload
        self.assertEqual(CONSTANTS.WS_ACTION_UNSUBSCRIBE, payload["action"])
        self.assertNotIn(self.trading_pair, self.data_source._trading_pairs)

    async def test_unsubscribe_from_trading_pair_websocket_not_connected(self):
        self.data_source._ws_assistant = None
        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)
        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", f"Cannot unsubscribe from {self.trading_pair}: WebSocket not connected")
        )

    # ---- Funding info (no funding channel/endpoint documented for MSX) --------

    async def test_get_funding_info_returns_zero_rate(self):
        # MSX docs do not document a funding endpoint; ticker provides mark/last price only.
        self.connector._api_get.return_value = {
            "symbol": self.ex_trading_pair,
            "close": "45000.00",
            "markPrice": "45010.00",
            "updateTime": 1631591424400,
        }

        result = await self.data_source.get_funding_info(self.trading_pair)

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(self.trading_pair, result.trading_pair)
        self.assertEqual(Decimal(0), result.rate)
        self.assertEqual(Decimal("45010.00"), result.mark_price)
        self.assertEqual(Decimal("45000.00"), result.index_price)

    async def test_get_funding_info_handles_missing_ticker_fields(self):
        # If the ticker response is incomplete, funding info must still be returned with zero rate.
        self.connector._api_get.return_value = {"symbol": self.ex_trading_pair}

        result = await self.data_source.get_funding_info(self.trading_pair)

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(Decimal(0), result.rate)

    async def test_parse_funding_info_message_is_safe_noop(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        await self.data_source._parse_funding_info_message(
            raw_message=self._ticker_event(), message_queue=msg_queue)
        # No funding channel exists; nothing should be queued.
        self.assertEqual(0, msg_queue.qsize())

    async def test_listen_for_funding_info_cancelled_error_raised(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_funding_info(mock_queue)

    # ---- Last traded prices ---------------------------------------------------

    async def test_get_last_traded_prices_delegates_to_connector(self):
        self.connector.get_last_traded_prices.return_value = {self.trading_pair: 45000.0}
        result = await self.data_source.get_last_traded_prices(trading_pairs=[self.trading_pair])
        self.assertEqual({self.trading_pair: 45000.0}, result)

    # ---- listen_for_subscriptions plumbing ------------------------------------

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    async def test_listen_for_subscriptions_cancelled_when_connecting(self, _):
        with patch.object(self.data_source, "_connected_websocket_assistant",
                          new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_subscriptions()

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    async def test_listen_for_subscriptions_logs_exception_details(self, sleep_mock):
        sleep_mock.side_effect = asyncio.CancelledError
        with patch.object(self.data_source, "_connected_websocket_assistant",
                          new=AsyncMock(side_effect=Exception("TEST ERROR."))):
            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_subscriptions()

        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...")
        )
