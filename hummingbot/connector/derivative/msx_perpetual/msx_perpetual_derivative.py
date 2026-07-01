import asyncio
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.msx_perpetual import (
    msx_perpetual_constants as CONSTANTS,
    msx_perpetual_utils as utils,
    msx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_api_order_book_data_source import (
    MsxPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_auth import MsxPerpetualAuth
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_user_stream_data_source import (
    MsxPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

bpm_logger = None

# MSX-specific design decisions (see docs/connectors/msx-api-notes.md §2, §7):
#   * No clientOrderId: the exchange identifies orders by a server-assigned int64 ``orderId``.
#     Hummingbot's client_order_id is tracked locally; exchange_order_id == str(orderId).
#   * There is no "get single order" endpoint: order status is reconstructed by scanning the
#     current open orders (/order/limit) then the order history (/order/history) for id==orderId.
#   * Closing a position needs the position's ``id`` (posId), not a reduce-only reverse order.
#   * Balances and positions both come from the single POST /position/current object.
#   * No private user-stream WebSocket; all updates come from REST polling.
#   * ONEWAY position mode only; leverage is per-order (set locally, no set-leverage endpoint);
#     no funding endpoint; no exchangeInfo (trading rules built from /price-steps).
DEFAULT_MIN_AMOUNT_INCREMENT = Decimal("0.00000001")
DEFAULT_MIN_ORDER_SIZE = Decimal("0.00000001")
DEFAULT_MIN_NOTIONAL = Decimal("0.00000001")
DEFAULT_LEVERAGE = 1


class MsxPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils
    # MSX rate-limits aggressively (empirically ~10 rps globally before HTTP 429). Order status /
    # fills polling scans /order/limit + /order/history per order, so keep the polling cadence low
    # to stay well under the limit. (A batch lookup that scans once for all orders is a follow-up.)
    SHORT_POLL_INTERVAL = 12.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 12.0
    LONG_POLL_INTERVAL = 120.0

    def __init__(
            self,
            balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
            rate_limits_share_pct: Decimal = Decimal("100"),
            msx_perpetual_api_key: str = None,
            msx_perpetual_api_secret: str = None,
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DOMAIN,
    ):
        self.msx_perpetual_api_key = msx_perpetual_api_key
        self.msx_perpetual_secret_key = msx_perpetual_api_secret
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._position_mode = None
        self._last_trade_history_timestamp = None
        # Cache of current position ids keyed by (exchange_symbol, PositionSide); used to build
        # the ``posId`` required when closing a position via /order/create.
        self._position_id_by_symbol_side: Dict[Tuple[str, PositionSide], int] = {}
        # MSX rejects concurrent order requests (empirically: 2 concurrent OK, 3+ -> code 1006
        # "Request too frequent"). Serialize all /order/create and /order/cancel calls through this
        # lock so a strategy placing buy+sell (or cancel+place) in the same tick never overlaps.
        # Serialization alone is enough; no artificial delay is needed (50ms-spaced serial calls pass).
        self._order_request_lock: Optional[asyncio.Lock] = None
        # Per-cycle snapshot for order-status polling: {exchange_symbol: {order_id: order_dict}}.
        # Populated at the top of _update_order_status so the fills path and the status path share a
        # single /order/limit + /order/history fetch per symbol instead of one lookup per order
        # (MSX rate-limits ~10 rps; the old per-order scan amplified requests ~4x M). None = disabled
        # -> _locate_order_on_exchange falls back to per-order queries (direct calls / unit tests).
        self._order_status_cache: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None
        # Symbols whose list fetch failed this cycle: their lookups must fall back to per-order queries
        # rather than treating a snapshot miss as "order gone" (which would wrongly fire not-found).
        self._order_status_cache_failed_symbols: Set[str] = set()
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def order_request_lock(self) -> asyncio.Lock:
        # Lazily created so it binds to the running event loop.
        if self._order_request_lock is None:
            self._order_request_lock = asyncio.Lock()
        return self._order_request_lock

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> MsxPerpetualAuth:
        return MsxPerpetualAuth(self.msx_perpetual_api_key, self.msx_perpetual_secret_key,
                                self._time_synchronizer)

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        # MSX has no broker/client-order-id concept; the local client_order_id needs no prefix.
        return ""

    @property
    def trading_rules_request_path(self) -> str:
        # No exchangeInfo endpoint; rules are pulled per pair from /price-steps in _update_trading_rules.
        return CONSTANTS.PRICE_STEPS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.PRICE_STEPS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        # MSX has no /ping; use the public ticker as a lightweight connectivity probe.
        return CONSTANTS.TICKER_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 600

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        # MSX documents only cross/isolated margin, with no hedge/one-way dual-side switch.
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        # MSX rejects requests outside the ±30s window with HTTP 401; treat that as a clock issue.
        error_description = str(request_exception)
        return "401" in error_description and "timestamp" in error_description.lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # _request_order_status raises an IOError tagged with this marker when an order can be found
        # neither in the open-orders list nor in the order history.
        return "ORDER_NOT_FOUND" in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "ORDER_NOT_FOUND" in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return MsxPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return MsxPerpetualUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        pass

    async def _user_stream_event_listener(self):
        # MSX has no private user-stream WebSocket; the user-stream data source never emits messages,
        # so this listener is a no-op. Order/balance/position updates come from REST polling.
        return

    async def check_network(self) -> NetworkStatus:
        # MSX has no /ping; probe a public ticker for the first configured trading pair.
        try:
            trading_pair = (self._trading_pairs or [utils.EXAMPLE_PAIR])[0]
            symbol = utils.convert_to_exchange_trading_pair(trading_pair)
            resp = await self._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL + "/" + symbol,
                limit_id=CONSTANTS.TICKER_PATH_URL,
                is_auth_required=True,  # MSX 测试环境公共接口也需签名
            )
            if self._is_success(resp):
                return NetworkStatus.CONNECTED
            return NetworkStatus.NOT_CONNECTED
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    async def _make_network_check_request(self):
        # check_network() is overridden, so this base hook is unused; keep it harmless.
        return

    async def _status_polling_loop_fetch_updates(self):
        # MSX has no private user stream; everything is REST polled here.
        await safe_gather(
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    # ------------------------------------------------------------------ orders

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            position_action: PositionAction = PositionAction.NIL,
            **kwargs,
    ) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        side = CONSTANTS.SIDE_LONG if trade_type is TradeType.BUY else CONSTANTS.SIDE_SHORT
        is_close = position_action == PositionAction.CLOSE
        open_type = CONSTANTS.OPEN_TYPE_CLOSE if is_close else CONSTANTS.OPEN_TYPE_OPEN

        # v1.1: /order/create 不再接受 marginMode。保证金模式由账户配置决定,
        # 通过 _set_trading_pair_leverage -> POST /account/leverage 预先设为逐仓。
        api_params: Dict[str, Any] = {
            "symbol": symbol,
            "coType": CONSTANTS.DEFAULT_CO_TYPE,
            "orderType": CONSTANTS.ORDER_TYPE_MARKET if order_type is OrderType.MARKET else CONSTANTS.ORDER_TYPE_LIMIT,
            "openType": open_type,
            "side": side,
            "vol": f"{amount:f}",
            "triggerType": CONSTANTS.TRIGGER_NORMAL,
        }
        if order_type.is_limit_type():
            api_params["price"] = f"{price:f}"

        if is_close:
            # Closing requires the server-side position id. When closing a LONG we submit a SELL
            # (side=SHORT) and vice-versa, so the position we close is on the opposite side.
            pos_side = PositionSide.LONG if trade_type is TradeType.SELL else PositionSide.SHORT
            pos_id = self._position_id_by_symbol_side.get((symbol, pos_side))
            if pos_id is None:
                pos_id = await self._fetch_position_id(symbol, pos_side)
            if pos_id is None:
                raise ValueError(
                    f"Cannot close position for {trading_pair} ({pos_side.name}): no open position id found "
                    f"on MSX. A posId from /position/current is required to close.")
            api_params["posId"] = pos_id
        else:
            # leverage is mandatory on open; MSX takes it per order (no set-leverage endpoint).
            api_params["leverage"] = str(self._leverage_for(trading_pair))

        # Serialize the order request: MSX rejects concurrent /order/create (3+ in flight -> 1006).
        # The lock also covers the follow-up orderId lookup so a concurrent placement can't interleave.
        async with self.order_request_lock:
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_CREATE_PATH_URL,
                data=api_params,
                is_auth_required=True)
            self._raise_on_error(order_result)
            transact_time = time.time()

            # v1.1 文档称下单响应返回 data.orderId, 但生产实测(2026-06)仍为 data:null,
            # 需回查最新订单匹配。若服务端后续修复直接返回 orderId, 此处会优先采用。
            data = order_result.get("data") if isinstance(order_result, dict) else None
            if isinstance(data, dict) and data.get("orderId") is not None:
                return str(data["orderId"]), transact_time
            o_id = await self._resolve_new_order_id(symbol, side, open_type)
            return o_id, transact_time

    async def _resolve_new_order_id(self, symbol: str, side: int, open_type: int) -> str:
        """下单成功但响应无 orderId 时, 回查最新一笔匹配订单的 id。

        优先查当前委托(/order/limit, 未成交挂单), 再查历史(/order/history, 含已成交),
        取 ctime 最新且方向(longFlag)匹配的一笔。
        """
        def _pick(resp: Dict[str, Any]) -> Optional[str]:
            if not self._is_success(resp):
                return None
            items = [
                it for it in (resp.get("data") or [])
                if int(it.get("longFlag", 0)) == side
                and int(it.get("openFlag", 0)) == open_type
            ]
            if not items:
                return None
            newest = max(items, key=lambda it: int(it.get("ctime", 0)))
            return str(newest.get("id"))

        open_orders = await self._api_post(
            path_url=CONSTANTS.ORDER_LIMIT_PATH_URL,
            data={"symbol": symbol, "coType": CONSTANTS.DEFAULT_CO_TYPE},
            is_auth_required=True, return_err=True)
        oid = _pick(open_orders)
        if oid is not None:
            return oid
        history = await self._api_post(
            path_url=CONSTANTS.ORDER_HISTORY_PATH_URL,
            data={"symbol": symbol, "coType": CONSTANTS.DEFAULT_CO_TYPE},
            is_auth_required=True, return_err=True)
        oid = _pick(history)
        if oid is None:
            raise IOError(
                f"Order placed on MSX but could not resolve its orderId from /order/limit or "
                f"/order/history (symbol={symbol}, side={side}, openType={open_type}).")
        return oid

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        api_params = {"orderId": int(exchange_order_id)}
        # Serialize with order placement: cancel + place in the same tick must not overlap (MSX 1006).
        async with self.order_request_lock:
            cancel_result = await self._api_post(
                path_url=CONSTANTS.ORDER_CANCEL_PATH_URL,
                data=api_params,
                is_auth_required=True,
                return_err=True)
        return self._is_success(cancel_result)

    async def _locate_order_on_exchange(self, tracked_order: InFlightOrder) -> Optional[Dict[str, Any]]:
        """Find the exchange order dict by scanning /order/limit then /order/history.

        MSX has no "get single order" endpoint; returns None if found in neither.
        Shared by _request_order_status and _all_trade_updates_for_order.

        When a per-cycle snapshot is populated (see _update_order_status), read from it instead of
        issuing requests: the snapshot holds the FULL /order/limit + /order/history lists for the
        symbol, so a hit returns the order and a miss means the order is genuinely absent. Symbols
        whose fetch failed this cycle fall back to per-order queries (a miss there is "unknown", not
        "gone").
        """
        exchange_order_id = await tracked_order.get_exchange_order_id()
        target_id = int(exchange_order_id)
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

        if self._order_status_cache is not None and symbol not in self._order_status_cache_failed_symbols:
            return self._order_status_cache.get(symbol, {}).get(target_id)

        # Fallback: no snapshot this cycle (direct call / unit test) or this symbol's fetch failed.
        # 1) current open orders (status 0/1/3) via /order/limit
        open_orders = await self._api_post(
            path_url=CONSTANTS.ORDER_LIMIT_PATH_URL,
            data={"symbol": symbol, "coType": CONSTANTS.DEFAULT_CO_TYPE},
            is_auth_required=True,
            return_err=True)
        match = self._find_order(open_orders, target_id)
        if match is None:
            # 2) order history (status 0..6) via /order/history
            history = await self._api_post(
                path_url=CONSTANTS.ORDER_HISTORY_PATH_URL,
                data={"symbol": symbol, "coType": CONSTANTS.DEFAULT_CO_TYPE},
                is_auth_required=True,
                return_err=True)
            match = self._find_order(history, target_id)
        return match

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # MSX has no per-trade fills endpoint; it only exposes aggregate avgPrice + cumulative
        # filledVol on /order/limit and /order/history. HB's did_fill_order / OrderFilledEvent is
        # driven ONLY by TradeUpdate (process_trade_update); an OrderUpdate reaching FILLED does
        # not by itself emit a fill. So we must synthesize a TradeUpdate from the aggregate data:
        # emit ONE TradeUpdate for the *increment* since the last poll (cumulative_filled minus the
        # already-recorded executed_amount_base), with a stable trade_id for dedup.
        #
        # Skip orders that don't yet have an exchange_order_id: MSX returns data:null on create so
        # the orderId is resolved by a follow-up lookup and may lag. Calling get_exchange_order_id()
        # here would BLOCK on the id-update event and get cancelled by the poll timeout, which
        # surfaced as "Failed to fetch trade updates ... CancelledError" and broke fill detection.
        # The next poll (after the id is set) will pick up the fill.
        if order.exchange_order_id is None:
            return []
        match = await self._locate_order_on_exchange(order)
        if match is None:
            return []

        cumulative_filled = Decimal(str(match.get("filledVol") or "0"))
        already_recorded = order.executed_amount_base
        increment = cumulative_filled - already_recorded
        if increment <= Decimal("0"):
            return []

        avg_price = Decimal(str(match.get("avgPrice") or "0"))
        if avg_price <= Decimal("0"):
            # Cannot value the fill without a price; skip until avgPrice is populated.
            return []

        ts_ms = match.get("tradedTime") or match.get("ctime") or self.current_timestamp * 1e3
        exchange_order_id = str(match["id"])
        fee = self._get_fee(
            base_currency=order.base_asset,
            quote_currency=order.quote_asset,
            order_type=order.order_type,
            order_side=order.trade_type,
            position_action=order.position,
            amount=increment,
            price=avg_price,
            is_maker=True,
        )
        trade_update = TradeUpdate(
            # Stable + monotonic in cumulative fill -> update_with_trade_update dedups by trade_id,
            # so re-polling the same cumulative amount will not double-count.
            trade_id=f"{exchange_order_id}-{cumulative_filled}",
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=order.trading_pair,
            fill_timestamp=float(ts_ms) * 1e-3,
            fill_price=avg_price,
            fill_base_amount=increment,
            fill_quote_amount=increment * avg_price,
            fee=fee,
        )
        return [trade_update]

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        match = await self._locate_order_on_exchange(tracked_order)

        if match is None:
            raise IOError(f"ORDER_NOT_FOUND: order {exchange_order_id} not found on MSX.")

        status = int(match["status"])
        ts_ms = match.get("tradedTime") or match.get("ctime") or self.current_timestamp * 1e3
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(ts_ms) * 1e-3,
            new_state=CONSTANTS.ORDER_STATE[status],
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(match["id"]),
        )

    async def _update_order_status(self):
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        if current_tick <= last_tick:
            return

        # Fills FIRST, and gated ONLY by the tick throttle (not by active_orders): a filled order
        # leaves active_orders and moves to cached, so keying the fills poll off active_orders meant
        # that once all resting quotes filled (active empty) fills were never polled and the trade
        # was never detected. all_fillable_orders includes active + cached + lost.
        # MSX has no user stream, so trade updates only come from polling aggregate avgPrice/filledVol.
        fillable = list(self._order_tracker.all_fillable_orders.values())
        if fillable:
            await self._update_orders_fills(orders=fillable)

        tracked_orders = list(self._order_tracker.active_orders.values())
        if len(tracked_orders) == 0:
            return
        tasks = [self._request_order_status(order) for order in tracked_orders]
        results = await safe_gather(*tasks, return_exceptions=True)
        for order_update, tracked_order in zip(results, tracked_orders):
            client_order_id = tracked_order.client_order_id
            if isinstance(order_update, Exception):
                if self._is_order_not_found_during_status_update_error(order_update):
                    await self._order_tracker.process_order_not_found(client_order_id)
                else:
                    self.logger().network(
                        f"Error fetching status update for the order {client_order_id}: {order_update}.")
                continue
            self._order_tracker.process_order_update(order_update)

    # --------------------------------------------------------------- balances

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        response = await self._api_post(
            path_url=CONSTANTS.POSITION_CURRENT_PATH_URL,
            data={"coType": CONSTANTS.DEFAULT_CO_TYPE},
            is_auth_required=True)
        self._raise_on_error(response)
        data = response["data"] or {}

        # MSX prices everything in USDT. ``balance`` is the free/available capital and
        # ``AcctBalance`` (note the capital A) is the total account balance.
        quote = "USDT"
        available = Decimal(str(data.get("balance", "0")))
        total = Decimal(str(data.get("AcctBalance", data.get("balance", "0"))))
        self._account_available_balances[quote] = available
        self._account_balances[quote] = total
        remote_asset_names.add(quote)

        for asset_name in local_asset_names.difference(remote_asset_names):
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _update_positions(self):
        response = await self._api_post(
            path_url=CONSTANTS.POSITION_CURRENT_PATH_URL,
            data={"coType": CONSTANTS.DEFAULT_CO_TYPE},
            is_auth_required=True)
        self._raise_on_error(response)
        data = response["data"] or {}
        pos_list = data.get("posList") or []

        seen_keys = set()
        self._position_id_by_symbol_side.clear()
        for pos in pos_list:
            symbol = pos.get("symbol")
            try:
                hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol)
            except KeyError:
                continue
            position_side = PositionSide.LONG if int(pos.get("longFlag")) == CONSTANTS.SIDE_LONG else PositionSide.SHORT
            self._position_id_by_symbol_side[(symbol, position_side)] = int(pos["id"])

            amount = Decimal(str(pos.get("nowVolTotal", "0")))
            if position_side == PositionSide.SHORT:
                amount = -amount
            unrealized_pnl = Decimal(str(pos.get("pnl", "0")))
            entry_price = Decimal(str(pos.get("avgPrice", "0")))
            leverage = Decimal(str(pos.get("leverage", DEFAULT_LEVERAGE)))
            pos_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
            if amount != Decimal("0"):
                _position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, _position)
                seen_keys.add(pos_key)
            else:
                self._perpetual_trading.remove_position(pos_key)

        # Drop any locally tracked positions no longer reported by the exchange.
        for pos_key in list(self._perpetual_trading.account_positions.keys()):
            if pos_key not in seen_keys:
                self._perpetual_trading.remove_position(pos_key)

    async def _fetch_position_id(self, symbol: str, position_side: PositionSide) -> Optional[int]:
        """Fetch /position/current and return the matching position id (posId) for closing."""
        response = await self._api_post(
            path_url=CONSTANTS.POSITION_CURRENT_PATH_URL,
            data={"symbol": symbol, "coType": CONSTANTS.DEFAULT_CO_TYPE},
            is_auth_required=True,
            return_err=True)
        if not self._is_success(response):
            return None
        for pos in (response.get("data") or {}).get("posList") or []:
            side = PositionSide.LONG if int(pos.get("longFlag")) == CONSTANTS.SIDE_LONG else PositionSide.SHORT
            if pos.get("symbol") == symbol and side == position_side:
                pos_id = int(pos["id"])
                self._position_id_by_symbol_side[(symbol, position_side)] = pos_id
                return pos_id
        return None

    # ---------------------------------------------------------- trading rules

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        # ``exchange_info_dict`` here maps trading_pair -> price-steps ``data`` (see _update_trading_rules).
        rules: List[TradingRule] = []
        for trading_pair, steps_data in exchange_info_dict.items():
            try:
                steps = steps_data.get("steps") or []
                # The smallest step is the price tick size.
                min_price_increment = (
                    min(Decimal(str(s)) for s in steps) if steps else DEFAULT_MIN_AMOUNT_INCREMENT
                )
                rules.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=DEFAULT_MIN_ORDER_SIZE,
                        min_price_increment=min_price_increment,
                        min_base_amount_increment=DEFAULT_MIN_AMOUNT_INCREMENT,
                        min_notional_size=DEFAULT_MIN_NOTIONAL,
                        buy_order_collateral_token="USDT",
                        sell_order_collateral_token="USDT",
                    )
                )
            except Exception as e:
                self.logger().error(
                    f"Error parsing trading pair rule for {trading_pair}: {e}. Skipping...", exc_info=True)
        return rules

    async def _update_trading_rules(self):
        # No exchangeInfo: build a {trading_pair: price-steps data} dict by querying /price-steps per pair.
        steps_by_pair: Dict[str, Any] = {}
        for trading_pair in self._trading_pairs or []:
            symbol = utils.convert_to_exchange_trading_pair(trading_pair)
            try:
                resp = await self._api_get(
                    path_url=CONSTANTS.PRICE_STEPS_PATH_URL + "/" + symbol,
                    limit_id=CONSTANTS.PRICE_STEPS_PATH_URL,
                    is_auth_required=True,  # MSX 测试环境公共接口也需签名
                )
                if self._is_success(resp):
                    steps_by_pair[trading_pair] = resp.get("data") or {}
            except Exception as e:
                self.logger().error(f"Error fetching price steps for {trading_pair}: {e}", exc_info=True)
        trading_rules_list = await self._format_trading_rules(steps_by_pair)
        # Do NOT wipe existing rules when the fetch failed (e.g. HTTP 429 rate limit) and returned
        # nothing. The original code unconditionally cleared then rebuilt, so a transient failure
        # left _trading_rules empty and every subsequent _create_order raised KeyError(trading_pair),
        # crashing order placement and hedging. Only replace rules we actually obtained.
        if not trading_rules_list:
            self.logger().warning(
                "MSX price-steps fetch yielded no trading rules this cycle (likely transient/rate-limited); "
                "keeping existing trading rules.")
            return
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(steps_by_pair)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        # No exchangeInfo listing; derive the symbol map directly from the configured trading pairs.
        mapping = bidict()
        for trading_pair in self._trading_pairs or []:
            exchange_symbol = utils.convert_to_exchange_trading_pair(trading_pair)
            mapping[exchange_symbol] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _initialize_trading_pair_symbol_map(self):
        # Override: MSX has no exchangeInfo to query, so build the map from configured pairs.
        try:
            self._initialize_trading_pair_symbols_from_exchange_info({})
        except Exception:
            self.logger().exception("There was an error initializing the trading pair symbols map.")

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_PATH_URL + "/" + symbol,
            limit_id=CONSTANTS.TICKER_PATH_URL,
            is_auth_required=True,  # MSX 测试环境公共接口也需签名
        )
        self._raise_on_error(response)
        data = response["data"]
        price = data.get("close", data.get("lastPrice"))
        return float(price)

    # ------------------------------------------------------ leverage / margin

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        # v1.1: orders no longer carry marginMode, so the account config must be set to ISOLATED here.
        # IMPORTANT: POST /account/leverage requires a marginMode field but does NOT actually change
        # the margin mode (实测确认). Margin mode can only be changed via POST /account/margin-mode.
        # Therefore set the margin mode first, then the leverage.
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        margin_response = await self._api_post(
            path_url=CONSTANTS.ACCOUNT_MARGIN_MODE_PATH_URL,
            data={"symbol": symbol, "marginMode": CONSTANTS.DEFAULT_MARGIN_MODE},
            is_auth_required=True,
            return_err=True)
        if not self._is_success(margin_response):
            message = (margin_response.get("message") or margin_response.get("msg")
                       if isinstance(margin_response, dict) else str(margin_response))
            return False, f"MSX set margin mode failed: {message}"

        leverage_response = await self._api_post(
            path_url=CONSTANTS.ACCOUNT_LEVERAGE_PATH_URL,
            data={
                "symbol": symbol,
                "leverage": str(leverage),
                "marginMode": CONSTANTS.DEFAULT_MARGIN_MODE,  # required field, ignored by server
            },
            is_auth_required=True,
            return_err=True)
        if self._is_success(leverage_response):
            return True, ""
        message = (leverage_response.get("message") or leverage_response.get("msg")
                   if isinstance(leverage_response, dict) else str(leverage_response))
        return False, f"MSX set leverage failed: {message}"

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode != PositionMode.ONEWAY:
            return False, "MSX only supports ONEWAY position mode."
        self._position_mode = PositionMode.ONEWAY
        return True, ""

    async def _get_position_mode(self) -> Optional[PositionMode]:
        self._position_mode = PositionMode.ONEWAY
        return self._position_mode

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        # MSX documents no funding-fee/income endpoint.
        return 0, Decimal("-1"), Decimal("-1")

    # ----------------------------------------------------------------- helpers

    def _leverage_for(self, trading_pair: str) -> int:
        try:
            leverage = self._perpetual_trading.get_leverage(trading_pair)
        except KeyError:
            leverage = DEFAULT_LEVERAGE
        return int(leverage) if leverage else DEFAULT_LEVERAGE

    @staticmethod
    def _is_success(response: Any) -> bool:
        # 公共行情 code==0, 私有 code==200; 均为成功。
        return isinstance(response, dict) and response.get("code") in CONSTANTS.SUCCESS_CODES

    @staticmethod
    def _find_order(response: Dict[str, Any], target_id: int) -> Optional[Dict[str, Any]]:
        if not MsxPerpetualDerivative._is_success(response):
            return None
        for item in response.get("data") or []:
            try:
                if int(item.get("id")) == target_id:
                    return item
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _raise_on_error(response: Dict[str, Any]):
        if not MsxPerpetualDerivative._is_success(response):
            message = response.get("message") if isinstance(response, dict) else str(response)
            raise IOError(f"MSX API error: {message} (full response: {response})")
