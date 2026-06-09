import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_web_utils as web_utils
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_derivative import (
        MsxPerpetualDerivative,
    )


class MsxPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """Order book data source for the MSX perpetual exchange.

    MSX uses a SINGLE WebSocket connection (CONSTANTS.PERPETUAL_WS_URL) carrying two
    push envelope shapes (see docs/connectors/msx-api-notes.md section 3):

    * Shape A (``action`` / ``result``): ``order_book_update`` (diff), ``book_ticker`` (BBO)
    * Shape B (``event`` == "data" / ``channel`` / ``data``): ``order_book`` (snapshot),
      ``ticker``, ``kline``

    MSX does NOT document a public trade-print channel nor a funding-rate channel/endpoint
    for perpetuals, so neither is subscribed. Funding info is returned with a zero rate and
    mark/index price taken from the ticker when available.
    """

    _bpobds_logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'MsxPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._diff_messages_queue_key = CONSTANTS.WS_ORDER_BOOK_UPDATE
        self._snapshot_messages_queue_key = CONSTANTS.WS_CHANNEL_ORDER_BOOK
        self._funding_info_messages_queue_key = "funding_info"

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """Return the last traded price per trading pair, delegating to the connector."""
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Return funding info for a trading pair.

        MSX does not document a funding-rate endpoint (see msx-api-notes.md): the rate is
        always ``Decimal(0)``. Mark/index price are taken from the 24h ticker when present.
        """
        ticker = await self._request_ticker(trading_pair)
        mark_price = Decimal(str(ticker.get("markPrice", ticker.get("close", "0"))))
        index_price = Decimal(str(ticker.get("close", ticker.get("markPrice", "0"))))
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=index_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=int(time.time()),
            # No funding-rate channel/endpoint documented for MSX perpetuals.
            rate=Decimal(0),
        )

    async def _request_ticker(self, trading_pair: str) -> Dict[str, Any]:
        """Fetch the 24h ticker (used as the mark/index price source for funding info)."""
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        data = await self._connector._api_get(
            path_url=CONSTANTS.TICKER_PATH_URL + "/" + symbol,
            limit_id=CONSTANTS.TICKER_PATH_URL,
        )
        return data

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Fetch the REST order book snapshot ``data`` payload (already unwrapped by the connector)."""
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        data = await self._connector._api_get(
            path_url=CONSTANTS.SNAPSHOT_PATH_URL + "/" + symbol,
            params={"depth": "100", "with_id": "true"},
            limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Build a SNAPSHOT OrderBookMessage from the REST snapshot, using ``data.id`` as update_id."""
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        update_id = snapshot_response["id"]
        snapshot_msg: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot_response["bids"],
            "asks": snapshot_response["asks"],
        }, timestamp=snapshot_timestamp)
        return snapshot_msg

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Open the single MSX WebSocket connection."""
        url = web_utils.wss_url(domain=self._domain)
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to ``order_book_update`` (diffs) and ``book_ticker`` (BBO/last price)."""
        try:
            streams: List[str] = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                streams.append(f"{symbol}@{CONSTANTS.WS_ORDER_BOOK_UPDATE}")
                streams.append(f"{symbol}@{CONSTANTS.WS_BOOK_TICKER}")
            payload = {
                "action": CONSTANTS.WS_ACTION_SUBSCRIBE,
                "streams": streams,
            }
            await ws.send(WSJSONRequest(payload))
            self.logger().info("Subscribed to public order book channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """Route an incoming WS message to its internal queue key by inspecting both envelopes.

        Returns "" (unknown channel, ignored) for book_ticker, ticker, kline and pong.
        """
        # Shape A: action/result
        action = event_message.get("action")
        if action == CONSTANTS.WS_ORDER_BOOK_UPDATE:
            return self._diff_messages_queue_key
        # Shape B: event=data / channel
        if event_message.get("event") == "data":
            channel = event_message.get("channel")
            if channel == CONSTANTS.WS_CHANNEL_ORDER_BOOK:
                return self._snapshot_messages_queue_key
        # book_ticker (Shape A), ticker/kline (Shape B), pong, etc. are not order-book channels.
        return ""

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse a Shape A ``order_book_update`` into a DIFF OrderBookMessage (update_id = ``u``)."""
        result = raw_message["result"]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(result["s"])
        timestamp = result.get("t", time.time() * 1e3) * 1e-3
        order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": trading_pair,
            "first_update_id": result["U"],
            "update_id": result["u"],
            "bids": result["b"],
            "asks": result["a"],
        }, timestamp=timestamp)
        message_queue.put_nowait(order_book_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse a Shape B ``order_book`` push into a SNAPSHOT OrderBookMessage."""
        data = raw_message["data"]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(data["symbol"])
        timestamp = data.get("timestamp", time.time() * 1e3) * 1e-3
        order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": data["id"],
            "bids": data["bids"],
            "asks": data["asks"],
        }, timestamp=timestamp)
        message_queue.put_nowait(order_book_message)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """No-op: MSX does not document a public trade-print WebSocket channel (msx-api-notes.md)."""
        return

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """No-op: MSX does not document a funding-rate WebSocket channel (msx-api-notes.md)."""
        return

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """Subscribe to the order book diff and book ticker streams for a single trading pair.

        :return: True on success, False if the WebSocket is not connected or an error occurs.
        """
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: WebSocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            payload = {
                "action": CONSTANTS.WS_ACTION_SUBSCRIBE,
                "streams": [
                    f"{symbol}@{CONSTANTS.WS_ORDER_BOOK_UPDATE}",
                    f"{symbol}@{CONSTANTS.WS_BOOK_TICKER}",
                ],
            }
            await self._ws_assistant.send(WSJSONRequest(payload))
            self.add_trading_pair(trading_pair)
            self.logger().info(f"Subscribed to {trading_pair} order book channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error subscribing to {trading_pair}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """Unsubscribe from the order book diff and book ticker streams for a single trading pair.

        :return: True on success, False if the WebSocket is not connected or an error occurs.
        """
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: WebSocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            payload = {
                "action": CONSTANTS.WS_ACTION_UNSUBSCRIBE,
                "streams": [
                    f"{symbol}@{CONSTANTS.WS_ORDER_BOOK_UPDATE}",
                    f"{symbol}@{CONSTANTS.WS_BOOK_TICKER}",
                ],
            }
            await self._ws_assistant.send(WSJSONRequest(payload))
            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Unsubscribed from {trading_pair} order book channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error unsubscribing from {trading_pair}")
            return False
