import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses.core import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.msx_perpetual.msx_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_api_order_book_data_source import (
    MsxPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_derivative import MsxPerpetualDerivative
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.network_iterator import NetworkStatus


class MsxPerpetualDerivativeUnitTest(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.symbol = f"{cls.base_asset}{cls.quote_asset}"
        cls.domain = CONSTANTS.TESTNET_DOMAIN

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        self.exchange = MsxPerpetualDerivative(
            msx_perpetual_api_key="testAPIKey",
            msx_perpetual_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
            domain=self.domain,
        )

        if hasattr(self.exchange, "_time_synchronizer"):
            self.exchange._time_synchronizer.add_time_offset_ms_sample(0)

        MsxPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {
            self.domain: bidict({self.symbol: self.trading_pair})
        }
        self.exchange._set_trading_pair_symbol_map(bidict({self.symbol: self.trading_pair}))

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)

    def tearDown(self) -> None:
        MsxPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and record.getMessage() == message for record in self.log_records
        )

    def _regex(self, url: str):
        return re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal("0.00000001"),
                min_price_increment=Decimal("0.01"),
                min_base_amount_increment=Decimal("0.00000001"),
                min_notional_size=Decimal("0.00000001"),
            )
        }
        return self.exchange._trading_rules

    # ---- properties -----------------------------------------------------------

    def test_supported_position_modes_oneway_only(self):
        self.assertEqual([PositionMode.ONEWAY], self.exchange.supported_position_modes())

    def test_client_order_id_prefix_does_not_reference_missing_constant(self):
        # Must not raise (BROKER_ID no longer exists).
        self.assertIsInstance(self.exchange.client_order_id_prefix, str)

    def test_check_network_request_path_points_at_ticker(self):
        self.assertIn(CONSTANTS.TICKER_PATH_URL, self.exchange.check_network_request_path)

    # ---- _place_order ---------------------------------------------------------

    @aioresponses()
    async def test_place_order_open_buy_limit_builds_msx_body(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, 10)
        url = web_utils.private_rest_url(CONSTANTS.ORDER_CREATE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(url),
            body=json.dumps({"code": 200, "msg": "success", "data": {"orderId": 8886774, "orderNo": "N1"}}),
        )

        o_id, transact_time = await self.exchange._place_order(
            order_id="OID1",
            trading_pair=self.trading_pair,
            amount=Decimal("0.5"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("30000"),
            position_action=PositionAction.OPEN,
        )

        self.assertEqual("8886774", o_id)
        self.assertGreater(transact_time, 0)

        request_body = json.loads(self._last_request_body(mock_api, url))
        self.assertEqual(self.symbol, request_body["symbol"])
        self.assertEqual(CONSTANTS.CO_TYPE_CRYPTO, request_body["coType"])
        self.assertEqual(CONSTANTS.ORDER_TYPE_LIMIT, request_body["orderType"])
        self.assertEqual(CONSTANTS.OPEN_TYPE_OPEN, request_body["openType"])
        self.assertEqual(CONSTANTS.SIDE_LONG, request_body["side"])
        self.assertEqual("0.5", request_body["vol"])
        self.assertEqual("30000", request_body["price"])
        self.assertEqual("10", request_body["leverage"])
        self.assertEqual(CONSTANTS.TRIGGER_NORMAL, request_body["triggerType"])

    @aioresponses()
    async def test_place_order_open_sell_market_omits_price_uses_side_short(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, 5)
        url = web_utils.private_rest_url(CONSTANTS.ORDER_CREATE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(url),
            body=json.dumps({"code": 200, "msg": "success", "data": {"orderId": 42, "orderNo": "N2"}}),
        )

        o_id, _ = await self.exchange._place_order(
            order_id="OID2",
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            trade_type=TradeType.SELL,
            order_type=OrderType.MARKET,
            price=Decimal("0"),
            position_action=PositionAction.OPEN,
        )

        self.assertEqual("42", o_id)
        request_body = json.loads(self._last_request_body(mock_api, url))
        self.assertEqual(CONSTANTS.ORDER_TYPE_MARKET, request_body["orderType"])
        self.assertEqual(CONSTANTS.SIDE_SHORT, request_body["side"])
        self.assertNotIn("price", request_body)

    @aioresponses()
    async def test_place_order_omits_margin_mode_v11(self, mock_api):
        # v1.1: /order/create 不再接受 marginMode。下单 body 不应包含该字段
        # (保证金模式由 _set_trading_pair_leverage -> /account/leverage 预设)。
        self._simulate_trading_rules_initialized()
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, 10)
        url = web_utils.private_rest_url(CONSTANTS.ORDER_CREATE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(url),
            body=json.dumps({"code": 0, "msg": "success", "data": {"orderId": 7, "orderNo": "N3"}}),
        )
        await self.exchange._place_order(
            order_id="OID3", trading_pair=self.trading_pair, amount=Decimal("0.5"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("30000"),
            position_action=PositionAction.OPEN,
        )
        request_body = json.loads(self._last_request_body(mock_api, url))
        self.assertNotIn("marginMode", request_body)

    @aioresponses()
    async def test_place_order_resolves_order_id_when_response_has_no_data(self, mock_api):
        # MSX 测试环境下单成功只回 {code, message}, 无 data.orderId; 需回查 /order/limit。
        self._simulate_trading_rules_initialized()
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, 10)
        create_url = web_utils.private_rest_url(CONSTANTS.ORDER_CREATE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(create_url),
            body=json.dumps({"code": 200, "message": "订单创建成功"}),
        )
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(limit_url),
            body=json.dumps({"code": 200, "data": [
                {"id": 555, "symbol": self.symbol, "longFlag": CONSTANTS.SIDE_LONG,
                 "openFlag": CONSTANTS.OPEN_TYPE_OPEN, "ctime": 1000, "status": 1},
                {"id": 999, "symbol": self.symbol, "longFlag": CONSTANTS.SIDE_LONG,
                 "openFlag": CONSTANTS.OPEN_TYPE_OPEN, "ctime": 2000, "status": 1},
            ]}),
        )
        o_id, _ = await self.exchange._place_order(
            order_id="OID4", trading_pair=self.trading_pair, amount=Decimal("0.5"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("30000"),
            position_action=PositionAction.OPEN,
        )
        # 取 ctime 最新的一笔(999)。
        self.assertEqual("999", o_id)

    @aioresponses()
    async def test_place_order_close_uses_posid(self, mock_api):
        self._simulate_trading_rules_initialized()
        # Seed a cached current position (LONG) for the symbol.
        self.exchange._position_id_by_symbol_side = {(self.symbol, PositionSide.LONG): 555}
        url = web_utils.private_rest_url(CONSTANTS.ORDER_CREATE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(url),
            body=json.dumps({"code": 200, "msg": "success", "data": {"orderId": 99, "orderNo": "N3"}}),
        )

        o_id, _ = await self.exchange._place_order(
            order_id="OID3",
            trading_pair=self.trading_pair,
            amount=Decimal("0.5"),
            trade_type=TradeType.SELL,  # closing a LONG -> sell
            order_type=OrderType.LIMIT,
            price=Decimal("31000"),
            position_action=PositionAction.CLOSE,
        )

        self.assertEqual("99", o_id)
        request_body = json.loads(self._last_request_body(mock_api, url))
        self.assertEqual(CONSTANTS.OPEN_TYPE_CLOSE, request_body["openType"])
        self.assertEqual(555, request_body["posId"])

    # ---- _place_cancel --------------------------------------------------------

    @aioresponses()
    async def test_place_cancel_posts_order_id_and_detects_success(self, mock_api):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        tracked = self.exchange.in_flight_orders["OID1"]
        url = web_utils.private_rest_url(CONSTANTS.ORDER_CANCEL_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(url),
            body=json.dumps({"code": 200, "msg": "success", "data": {"orderId": 8886774}}),
        )

        result = await self.exchange._place_cancel("OID1", tracked)
        self.assertTrue(result)
        request_body = json.loads(self._last_request_body(mock_api, url))
        self.assertEqual(8886774, request_body["orderId"])

    @aioresponses()
    async def test_place_cancel_failure_returns_false(self, mock_api):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        tracked = self.exchange.in_flight_orders["OID1"]
        url = web_utils.private_rest_url(CONSTANTS.ORDER_CANCEL_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(url), body=json.dumps({"code": 400, "message": "bad"}))

        result = await self.exchange._place_cancel("OID1", tracked)
        self.assertFalse(result)

    # ---- balances & positions -------------------------------------------------

    @aioresponses()
    async def test_update_balances_parses_position_current_object(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.POSITION_CURRENT_PATH_URL, domain=self.domain)
        response = {
            "code": 200,
            "msg": "success",
            "data": {
                "balance": "900.5",
                "AcctBalance": "1000.0",
                "assetValuation": "1010.0",
                "pnlTotal": "10.0",
                "posList": [],
            },
        }
        mock_api.post(self._regex(url), body=json.dumps(response))

        await self.exchange._update_balances()

        self.assertEqual(Decimal("900.5"), self.exchange.available_balances["USDT"])
        self.assertEqual(Decimal("1000.0"), self.exchange.get_balance("USDT"))

    @aioresponses()
    async def test_update_positions_parses_poslist(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.POSITION_CURRENT_PATH_URL, domain=self.domain)
        response = {
            "code": 200,
            "msg": "success",
            "data": {
                "balance": "900",
                "AcctBalance": "1000",
                "posList": [
                    {
                        "id": 777,
                        "symbol": self.symbol,
                        "longFlag": 1,
                        "leverage": 10,
                        "nowVolTotal": "0.5",
                        "avgPrice": "30000",
                        "markPrice": "30100",
                        "pnl": "50",
                        "posStatus": 1,
                    }
                ],
            },
        }
        mock_api.post(self._regex(url), body=json.dumps(response))

        await self.exchange._update_positions()

        positions = list(self.exchange.account_positions.values())
        self.assertEqual(1, len(positions))
        pos = positions[0]
        self.assertEqual(self.trading_pair, pos.trading_pair)
        self.assertEqual(PositionSide.LONG, pos.position_side)
        self.assertEqual(Decimal("0.5"), pos.amount)
        self.assertEqual(Decimal("30000"), pos.entry_price)
        self.assertEqual(Decimal("50"), pos.unrealized_pnl)
        self.assertEqual(Decimal("10"), pos.leverage)
        # posId cached for CLOSE order routing
        self.assertEqual(777, self.exchange._position_id_by_symbol_side[(self.symbol, PositionSide.LONG)])

    @aioresponses()
    async def test_update_positions_short_negative_amount(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.POSITION_CURRENT_PATH_URL, domain=self.domain)
        response = {
            "code": 200,
            "msg": "success",
            "data": {
                "balance": "900",
                "AcctBalance": "1000",
                "posList": [
                    {
                        "id": 888,
                        "symbol": self.symbol,
                        "longFlag": 2,
                        "leverage": 3,
                        "nowVolTotal": "2",
                        "avgPrice": "29000",
                        "pnl": "-5",
                        "posStatus": 1,
                    }
                ],
            },
        }
        mock_api.post(self._regex(url), body=json.dumps(response))

        await self.exchange._update_positions()
        pos = list(self.exchange.account_positions.values())[0]
        self.assertEqual(PositionSide.SHORT, pos.position_side)
        self.assertEqual(Decimal("-2"), pos.amount)

    # ---- order status ---------------------------------------------------------

    @aioresponses()
    async def test_request_order_status_found_in_open_orders(self, mock_api):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        tracked = self.exchange.in_flight_orders["OID1"]
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(limit_url),
            body=json.dumps(
                {
                    "code": 200,
                    "msg": "success",
                    "data": [
                        {"id": 8886774, "symbol": self.symbol, "status": 3, "ctime": 1640780000000}
                    ],
                }
            ),
        )

        order_update = await self.exchange._request_order_status(tracked)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order_update.new_state)
        self.assertEqual("8886774", order_update.exchange_order_id)
        self.assertEqual("OID1", order_update.client_order_id)

    @aioresponses()
    async def test_request_order_status_falls_back_to_history(self, mock_api):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        tracked = self.exchange.in_flight_orders["OID1"]
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(limit_url), body=json.dumps({"code": 200, "msg": "success", "data": []}))
        mock_api.post(
            self._regex(history_url),
            body=json.dumps(
                {
                    "code": 200,
                    "msg": "success",
                    "data": [
                        {
                            "id": 8886774,
                            "symbol": self.symbol,
                            "status": 2,
                            "avgPrice": "30000",
                            "tradedTime": 1640780500000,
                        }
                    ],
                }
            ),
        )

        order_update = await self.exchange._request_order_status(tracked)
        self.assertEqual(OrderState.FILLED, order_update.new_state)
        self.assertEqual("8886774", order_update.exchange_order_id)

    # ---- order request serialization (MSX rejects concurrent order requests) --

    async def test_place_order_serialized_no_concurrent_api_calls(self):
        """MSX 不允许并发下单(并发 3+ -> 1006)。connector 必须串行化下单请求。

        本测试: 两个并发 _place_order, 用 _api_post 打点检测是否有请求重叠(并发)。
        修复前(无锁)会重叠 -> max_concurrent==2; 修复后(有锁)串行 -> max_concurrent==1。
        """
        import asyncio

        self._simulate_trading_rules_initialized()
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, 1)

        in_flight = 0
        max_concurrent = 0

        async def fake_api_post(*args, **kwargs):
            nonlocal in_flight, max_concurrent
            in_flight += 1
            max_concurrent = max(max_concurrent, in_flight)
            await asyncio.sleep(0.05)  # 模拟网络往返, 制造重叠窗口
            in_flight -= 1
            return {"code": 0, "msg": "success", "data": {"orderId": 111, "orderNo": "N"}}

        self.exchange._api_post = fake_api_post  # type: ignore

        async def one(side):
            return await self.exchange._place_order(
                order_id=f"OID_{side}",
                trading_pair=self.trading_pair,
                amount=Decimal("0.001"),
                trade_type=side,
                order_type=OrderType.LIMIT,
                price=Decimal("30000"),
                position_action=PositionAction.OPEN,
            )

        await asyncio.gather(one(TradeType.BUY), one(TradeType.SELL))
        self.assertEqual(1, max_concurrent, "下单请求必须串行, 不得并发(MSX 会 1006)")

    # ---- trade updates (fill synthesis, no per-trade endpoint) ----------------

    def _track_open_buy(self, order_id="OID1", exchange_order_id="8886774", amount="1"):
        self.exchange.start_tracking_order(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal(amount),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        return self.exchange.in_flight_orders[order_id]

    async def test_all_trade_updates_returns_empty_when_no_exchange_order_id(self):
        """订单尚未拿到 exchange_order_id 时, 不得阻塞等待(会超时 CancelledError), 直接返回 []。

        原 bug: _locate_order_on_exchange 调 get_exchange_order_id() 会阻塞等待未就绪的
        exchange_order_id, 轮询超时 -> CancelledError -> fill 通道失败 -> 成交感知失效。
        MSX 下单返回 data:null 需回查, orderId 有延迟, 这种订单很常见。
        """
        self.exchange.start_tracking_order(
            order_id="OID_NOID",
            exchange_order_id=None,  # 尚未确认
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        tracked = self.exchange.in_flight_orders["OID_NOID"]
        # 不应阻塞、不应抛异常, 直接空(下轮 orderId 就绪后再处理)。
        updates = await asyncio.wait_for(
            self.exchange._all_trade_updates_for_order(tracked), timeout=1.0
        )
        self.assertEqual([], updates)

    async def test_request_order_status_no_exchange_order_id_raises_not_found(self):
        """状态轮询路径与成交路径对称: 订单无 exchange_order_id 时不得阻塞等待。

        原 bug: _request_order_status -> _locate_order_on_exchange 调 get_exchange_order_id()
        对永远拿不到 orderId 的僵尸单(下单即 FAILED, id=None)阻塞 -> 轮询超时 CancelledError
        (str 为空) -> 不含 ORDER_NOT_FOUND -> 跳过 process_order_not_found -> 无限刷
        "Error fetching status update ...: ." 且僵尸单永不被清理。
        修复: id 缺失时直接抛 ORDER_NOT_FOUND, 使上层走 process_order_not_found(累计计数
        超阈标记 lost order 清理)。断言不阻塞(超时保护)且异常含 ORDER_NOT_FOUND。
        """
        self.exchange.start_tracking_order(
            order_id="OID_ZOMBIE",
            exchange_order_id=None,  # 下单即失败, 永不就绪
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("30000"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            leverage=1,
            position_action=PositionAction.OPEN,
        )
        tracked = self.exchange.in_flight_orders["OID_ZOMBIE"]
        with self.assertRaises(IOError) as ctx:
            await asyncio.wait_for(
                self.exchange._request_order_status(tracked), timeout=1.0
            )
        self.assertIn("ORDER_NOT_FOUND", str(ctx.exception))
        self.assertTrue(
            self.exchange._is_order_not_found_during_status_update_error(ctx.exception)
        )

    @aioresponses()
    async def test_all_trade_updates_synthesizes_fill_from_aggregate(self, mock_api):
        """MSX 无逐笔成交端点: 从 /order/history 的 filledVol+avgPrice 合成 TradeUpdate。"""
        tracked = self._track_open_buy(amount="1")
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(limit_url), body=json.dumps({"code": 0, "msg": "success", "data": []}))
        mock_api.post(
            self._regex(history_url),
            body=json.dumps(
                {
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "id": 8886774,
                            "symbol": self.symbol,
                            "status": 2,
                            "vol": "1",
                            "filledVol": "1",
                            "avgPrice": "30000",
                            "tradedTime": 1640780500000,
                        }
                    ],
                }
            ),
        )

        updates = await self.exchange._all_trade_updates_for_order(tracked)
        self.assertEqual(1, len(updates))
        tu = updates[0]
        self.assertEqual("8886774", tu.exchange_order_id)
        self.assertEqual("OID1", tu.client_order_id)
        self.assertEqual(Decimal("1"), tu.fill_base_amount)
        self.assertEqual(Decimal("30000"), tu.fill_price)
        self.assertEqual(Decimal("30000"), tu.fill_quote_amount)

    @aioresponses()
    async def test_all_trade_updates_empty_when_no_fill(self, mock_api):
        """未成交(filledVol=0)不应合成 TradeUpdate。"""
        tracked = self._track_open_buy(amount="1")
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(limit_url),
            body=json.dumps(
                {
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {"id": 8886774, "symbol": self.symbol, "status": 1,
                         "vol": "1", "filledVol": "0", "avgPrice": "0", "ctime": 1640780000000}
                    ],
                }
            ),
        )

        updates = await self.exchange._all_trade_updates_for_order(tracked)
        self.assertEqual([], updates)

    @aioresponses()
    async def test_all_trade_updates_only_increment_after_partial(self, mock_api):
        """已记 0.4, 交易所累计 filledVol=1.0 -> 只合成增量 0.6。"""
        tracked = self._track_open_buy(amount="1")
        tracked.executed_amount_base = Decimal("0.4")  # 本地已记账
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(limit_url), body=json.dumps({"code": 0, "msg": "success", "data": []}))
        mock_api.post(
            self._regex(history_url),
            body=json.dumps(
                {
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {"id": 8886774, "symbol": self.symbol, "status": 2,
                         "vol": "1", "filledVol": "1", "avgPrice": "30000", "tradedTime": 1640780500000}
                    ],
                }
            ),
        )

        updates = await self.exchange._all_trade_updates_for_order(tracked)
        self.assertEqual(1, len(updates))
        self.assertEqual(Decimal("0.6"), updates[0].fill_base_amount)

    @aioresponses()
    async def test_all_trade_updates_stable_trade_id_for_dedup(self, mock_api):
        """trade_id 由 exchange_order_id + 累计成交量组成, 同一累计量下稳定(供去重)。"""
        tracked = self._track_open_buy(amount="1")
        history_body = json.dumps(
            {
                "code": 0,
                "msg": "success",
                "data": [
                    {"id": 8886774, "symbol": self.symbol, "status": 2,
                     "vol": "1", "filledVol": "1", "avgPrice": "30000", "tradedTime": 1640780500000}
                ],
            }
        )
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(limit_url), body=json.dumps({"code": 0, "msg": "success", "data": []}))
        mock_api.post(self._regex(history_url), body=history_body)

        updates = await self.exchange._all_trade_updates_for_order(tracked)
        self.assertEqual("8886774-1", updates[0].trade_id)

    @aioresponses()
    async def test_update_order_status_records_fill_via_trade_update(self, mock_api):
        """_update_order_status 必须调用 fills 通道, 使成交经 TradeUpdate 记入 executed_amount_base。

        这是修复 did_fill_order 不触发的关键: 无此调用, 成交量永远不进 order tracker。
        """
        tracked = self._track_open_buy(amount="1")
        # 让节流逻辑放行(current_tick > last_tick)。
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(1640780000 + 100)

        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        # 订单已成交, 出现在 history(不在 open orders)。fills 与 status 两条通道都会查。
        mock_api.post(self._regex(limit_url), body=json.dumps({"code": 0, "msg": "success", "data": []}))
        mock_api.post(
            self._regex(history_url),
            body=json.dumps(
                {
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {"id": 8886774, "symbol": self.symbol, "status": 2,
                         "vol": "1", "filledVol": "1", "avgPrice": "30000", "tradedTime": 1640780500000}
                    ],
                }
            ),
        )

        self.assertEqual(Decimal("0"), tracked.executed_amount_base)
        await self.exchange._update_order_status()
        # 成交量已经过 TradeUpdate 记入(证明 fills 通道被调用)。
        self.assertEqual(Decimal("1"), tracked.executed_amount_base)

    @aioresponses()
    async def test_update_order_status_runs_fills_from_fillable_not_active(self, mock_api):
        """fills 通道须以 all_fillable_orders 为准且仅受 tick 节流, 不得被 active 判空 gate。

        原 bug: _update_order_status 在 'len(active_orders)==0 -> return' 之后才调 fills。
        成交完的订单离开 active 进 cached, 若某轮 active 为空(挂单全成交)则 fills 被跳过,
        成交永不被检测。本测试放一个 fillable 订单并把 active 清空, 验证 fills 仍运行。
        """
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000 + 1000)  # 放行 tick 节流

        # 轮首快照拉一次 /order/limit + /order/history(即使 active 为空), mock 之。
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(limit_url),
                      body=json.dumps({"code": 0, "msg": "success", "data": []}), repeat=True)
        mock_api.post(self._regex(history_url),
                      body=json.dumps({"code": 0, "msg": "success", "data": []}), repeat=True)

        self._track_open_buy(order_id="OID_FILL", exchange_order_id="999")
        tracked = self.exchange._order_tracker.active_orders.pop("OID_FILL")
        # 移到 cached: 已离开 active 但仍属 all_fillable。
        self.exchange._order_tracker._cached_orders["OID_FILL"] = tracked
        self.assertEqual(0, len(self.exchange._order_tracker.active_orders))
        self.assertIn("OID_FILL", self.exchange._order_tracker.all_fillable_orders)

        seen = {"orders": None}

        async def spy_fills(orders):
            seen["orders"] = list(orders)

        self.exchange._update_orders_fills = spy_fills  # type: ignore
        await self.exchange._update_order_status()
        self.assertIsNotNone(seen["orders"], "active 为空时 fills 仍须运行")
        self.assertIn(tracked, seen["orders"])

    # ---- trading rules & symbol map -------------------------------------------

    @aioresponses()
    async def test_update_trading_rules_from_price_steps(self, mock_api):
        url = web_utils.public_rest_url(
            CONSTANTS.PRICE_STEPS_PATH_URL + "/" + self.symbol, domain=self.domain
        )
        mock_api.get(
            self._regex(url),
            body=json.dumps(
                {"code": 200, "msg": "success", "data": {"symbol": self.symbol, "steps": ["0.01", "0.1", "1"]}}
            ),
        )

        await self.exchange._update_trading_rules()

        rule = self.exchange.trading_rules[self.trading_pair]
        self.assertEqual(Decimal("0.01"), rule.min_price_increment)
        self.assertEqual(Decimal("0.00000001"), rule.min_base_amount_increment)

    # ---- batch snapshot (order-status poll dedup) -----------------------------

    async def test_locate_reads_snapshot_without_api_call(self):
        """填充快照后, _locate_order_on_exchange 命中即返回, 不得发任何请求。"""
        tracked = self._track_open_buy(order_id="OID1", exchange_order_id="8886774")
        order_dict = {"id": 8886774, "symbol": self.symbol, "status": 3, "ctime": 1640780000000}
        self.exchange._order_status_cache = {self.symbol: {8886774: order_dict}}
        self.exchange._order_status_cache_failed_symbols = set()

        called = {"n": 0}

        async def boom(*a, **k):
            called["n"] += 1
            raise AssertionError("must not call _api_post when snapshot is populated")

        self.exchange._api_post = boom  # type: ignore
        match = await self.exchange._locate_order_on_exchange(tracked)
        self.assertEqual(order_dict, match)
        self.assertEqual(0, called["n"])

    @aioresponses()
    async def test_update_order_status_batches_list_requests(self, mock_api):
        """M 个同 symbol 订单一轮只拉一次 /order/limit + 一次 /order/history(共 2 次), 而非每订单各查。"""
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(1640780000 + 100)  # 放行 tick 节流
        for i, oid in enumerate(("A", "B", "C")):
            self._track_open_buy(order_id=oid, exchange_order_id=str(1000 + i))

        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        # 三个订单都在当前委托里(status 1 未成交)。
        open_body = json.dumps({
            "code": 0, "msg": "success",
            "data": [
                {"id": 1000, "symbol": self.symbol, "status": 1, "filledVol": "0", "avgPrice": "0", "ctime": 1},
                {"id": 1001, "symbol": self.symbol, "status": 1, "filledVol": "0", "avgPrice": "0", "ctime": 1},
                {"id": 1002, "symbol": self.symbol, "status": 1, "filledVol": "0", "avgPrice": "0", "ctime": 1},
            ],
        })
        mock_api.post(self._regex(limit_url), body=open_body, repeat=True)
        mock_api.post(self._regex(history_url),
                      body=json.dumps({"code": 0, "msg": "success", "data": []}), repeat=True)

        await self.exchange._update_order_status()

        posts = [
            (key[1].human_repr() if hasattr(key[1], "human_repr") else str(key[1]))
            for key, calls in mock_api.requests.items() for _ in calls
            if key[0] == "POST"
        ]
        limit_calls = sum(1 for u in posts if CONSTANTS.ORDER_LIMIT_PATH_URL in u)
        history_calls = sum(1 for u in posts if CONSTANTS.ORDER_HISTORY_PATH_URL in u)
        self.assertEqual(1, limit_calls, "3 个订单应只拉 1 次 /order/limit")
        self.assertLessEqual(history_calls, 1, "/order/history 至多 1 次")

    @aioresponses()
    async def test_snapshot_fetch_failure_falls_back_not_gone(self, mock_api):
        """某 symbol 列表拉取失败(429): 该 symbol 记入 failed_symbols, 查询回退逐订单, 不误判订单消失。"""
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(1640780000 + 100)
        self._track_open_buy(order_id="OID1", exchange_order_id="8886774")

        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        # 快照阶段: limit 返回 429(失败) -> symbol 进 failed_symbols。
        # 回退阶段(逐订单): limit 命中该订单 -> 正常状态更新, 不 not-found。
        # 注: 一轮内回退会被调用多次(fills 通道 + status 通道各查一次 /order/limit),
        # 故成功回退响应用 repeat=True 让每次回退都命中该订单(aioresponses 先消费 429,
        # 再持续返回成功)。
        mock_api.post(self._regex(limit_url), body=json.dumps({"code": 429, "msg": "rate limited"}))
        mock_api.post(self._regex(history_url),
                      body=json.dumps({"code": 0, "msg": "success", "data": []}), repeat=True)
        mock_api.post(
            self._regex(limit_url),
            body=json.dumps({"code": 0, "msg": "success",
                             "data": [{"id": 8886774, "symbol": self.symbol, "status": 1,
                                       "filledVol": "0", "avgPrice": "0", "ctime": 1}]}),
            repeat=True,
        )

        called = {"not_found": 0}
        orig = self.exchange._order_tracker.process_order_not_found

        async def spy_not_found(cid):
            called["not_found"] += 1
            return await orig(cid)

        self.exchange._order_tracker.process_order_not_found = spy_not_found  # type: ignore

        # failed_symbols 在 _update_order_status 的 finally 里被清空, 故在快照构建后、finally 前
        # 探测(见 brief Step 3 备选), 捕获该 symbol 确实进入了 failed_symbols。
        seen_failed = {"symbols": None}
        orig_snapshot = self.exchange._build_order_status_snapshot

        async def spy_snapshot(orders):
            await orig_snapshot(orders)
            seen_failed["symbols"] = set(self.exchange._order_status_cache_failed_symbols)

        self.exchange._build_order_status_snapshot = spy_snapshot  # type: ignore

        await self.exchange._update_order_status()
        self.assertEqual(0, called["not_found"], "拉取失败不得误触发 not-found")
        self.assertIn(self.symbol, seen_failed["symbols"],
                      "列表拉取失败的 symbol 必须记入 failed_symbols(回退逐订单)")

    @aioresponses()
    async def test_snapshot_cleared_after_cycle(self, mock_api):
        """一轮结束后快照必须清空(None), 不得跨轮复用陈旧数据。"""
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(1640780000 + 100)
        self._track_open_buy(order_id="OID1", exchange_order_id="8886774")
        limit_url = web_utils.private_rest_url(CONSTANTS.ORDER_LIMIT_PATH_URL, domain=self.domain)
        history_url = web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH_URL, domain=self.domain)
        mock_api.post(self._regex(limit_url),
                      body=json.dumps({"code": 0, "msg": "success",
                                       "data": [{"id": 8886774, "symbol": self.symbol, "status": 1,
                                                 "filledVol": "0", "avgPrice": "0", "ctime": 1}]}),
                      repeat=True)
        mock_api.post(self._regex(history_url),
                      body=json.dumps({"code": 0, "msg": "success", "data": []}), repeat=True)

        await self.exchange._update_order_status()
        self.assertIsNone(self.exchange._order_status_cache, "轮末快照必须为 None")
        self.assertEqual(set(), self.exchange._order_status_cache_failed_symbols)

    @aioresponses()
    async def test_update_trading_rules_keeps_existing_on_fetch_failure(self, mock_api):
        """price-steps 请求失败(如 429)时不得清空已有 trading rules。

        原 bug: _update_trading_rules 无条件 clear() 再重建; 拉取失败 -> rules 空 ->
        后续 _create_order 的 self._trading_rules['BTC-USDT'] KeyError -> 下单/对冲崩溃。
        """
        # 先成功加载一次规则
        self._simulate_trading_rules_initialized()
        self.assertIn(self.trading_pair, self.exchange.trading_rules)

        # 再次更新时 price-steps 返回 429(失败)
        url = web_utils.public_rest_url(
            CONSTANTS.PRICE_STEPS_PATH_URL + "/" + self.symbol, domain=self.domain
        )
        mock_api.get(self._regex(url), status=429, body=json.dumps({"code": 429, "msg": "rate limit"}))

        await self.exchange._update_trading_rules()

        # 拉取失败不应清空已有规则(否则下单会 KeyError)。
        self.assertIn(self.trading_pair, self.exchange.trading_rules)
        self.assertEqual(Decimal("0.01"), self.exchange.trading_rules[self.trading_pair].min_price_increment)

    async def test_initialize_symbol_map_from_configured_pairs(self):
        self.exchange._set_trading_pair_symbol_map(None)
        self.exchange._initialize_trading_pair_symbols_from_exchange_info({})
        self.assertEqual(
            self.symbol, await self.exchange.exchange_symbol_associated_to_pair(self.trading_pair)
        )
        self.assertEqual(
            self.trading_pair, await self.exchange.trading_pair_associated_to_exchange_symbol(self.symbol)
        )

    # ---- last traded price ----------------------------------------------------

    @aioresponses()
    async def test_get_last_traded_price_from_ticker(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL + "/" + self.symbol, domain=self.domain)
        mock_api.get(
            self._regex(url),
            body=json.dumps({"code": 200, "msg": "success", "data": {"symbol": self.symbol, "close": "30123.45"}}),
        )

        price = await self.exchange._get_last_traded_price(self.trading_pair)
        self.assertEqual(30123.45, price)

    # ---- leverage / position mode / funding -----------------------------------

    @aioresponses()
    async def test_set_trading_pair_leverage_sets_margin_mode_then_leverage(self, mock_api):
        # v1.1: 先 POST /account/margin-mode 设逐仓(改保证金模式唯一入口),
        # 再 POST /account/leverage 设杠杆(其 marginMode 字段必填但不生效)。
        margin_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_MARGIN_MODE_PATH_URL, domain=self.domain)
        leverage_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_LEVERAGE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(margin_url),
            body=json.dumps({"code": 0, "msg": "success",
                             "data": {"symbol": self.symbol, "marginMode": 2}}),
        )
        mock_api.post(
            self._regex(leverage_url),
            body=json.dumps({"code": 0, "msg": "success",
                             "data": {"symbol": self.symbol, "leverage": "7", "marginMode": 2}}),
        )
        success, msg = await self.exchange._set_trading_pair_leverage(self.trading_pair, 7)
        self.assertTrue(success)
        self.assertEqual("", msg)

        margin_body = json.loads(self._last_request_body(mock_api, margin_url))
        self.assertEqual(self.symbol, margin_body["symbol"])
        self.assertEqual(CONSTANTS.MARGIN_MODE_ISOLATED, margin_body["marginMode"])

        leverage_body = json.loads(self._last_request_body(mock_api, leverage_url))
        self.assertEqual(self.symbol, leverage_body["symbol"])
        self.assertEqual("7", leverage_body["leverage"])
        self.assertEqual(CONSTANTS.MARGIN_MODE_ISOLATED, leverage_body["marginMode"])

    @aioresponses()
    async def test_set_trading_pair_leverage_returns_error_on_margin_mode_failure(self, mock_api):
        margin_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_MARGIN_MODE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(margin_url),
            body=json.dumps({"code": 8012, "message": "保证金模式异常"}),
        )
        success, msg = await self.exchange._set_trading_pair_leverage(self.trading_pair, 7)
        self.assertFalse(success)
        self.assertIn("保证金模式异常", msg)

    @aioresponses()
    async def test_set_trading_pair_leverage_returns_error_on_failure(self, mock_api):
        margin_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_MARGIN_MODE_PATH_URL, domain=self.domain)
        leverage_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_LEVERAGE_PATH_URL, domain=self.domain)
        mock_api.post(
            self._regex(margin_url),
            body=json.dumps({"code": 0, "msg": "success",
                             "data": {"symbol": self.symbol, "marginMode": 2}}),
        )
        mock_api.post(
            self._regex(leverage_url),
            body=json.dumps({"code": 8011, "message": "杠杆异常"}),
        )
        success, msg = await self.exchange._set_trading_pair_leverage(self.trading_pair, 999)
        self.assertFalse(success)
        self.assertIn("杠杆异常", msg)

    async def test_trading_pair_position_mode_set_oneway_succeeds(self):
        success, _ = await self.exchange._trading_pair_position_mode_set(PositionMode.ONEWAY, self.trading_pair)
        self.assertTrue(success)

    async def test_trading_pair_position_mode_set_hedge_fails(self):
        success, _ = await self.exchange._trading_pair_position_mode_set(PositionMode.HEDGE, self.trading_pair)
        self.assertFalse(success)

    async def test_get_position_mode_returns_oneway(self):
        self.assertEqual(PositionMode.ONEWAY, await self.exchange._get_position_mode())

    async def test_fetch_last_fee_payment_no_funding_endpoint(self):
        ts, rate, payment = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual(0, ts)
        self.assertEqual(Decimal("-1"), rate)
        self.assertEqual(Decimal("-1"), payment)

    @aioresponses()
    async def test_check_network_connected(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL + "/" + self.symbol, domain=self.domain)
        mock_api.get(
            self._regex(url),
            body=json.dumps({"code": 200, "msg": "success", "data": {"symbol": self.symbol, "close": "1"}}),
        )
        status = await self.exchange.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)

    # ---- time-synchronizer error detection ------------------------------------

    def test_clock_skew_401_chinese_message_detected_as_time_sync(self):
        """MSX 时钟超窗返回 code=401 + 中文 message '时间戳过期'(通用响应格式_v1_1)。

        判定必须识别中文 '时间戳', 否则时钟漂移时 HB 不会触发重新校时, 签名请求
        持续 401 无法自愈。
        """
        exc = OSError(
            "Error executing request POST /order/create. HTTP status is 401. "
            'Error: {"code":401,"message":"时间戳过期"}'
        )
        self.assertTrue(
            self.exchange._is_request_exception_related_to_time_synchronizer(exc),
            "中文 '时间戳过期' 的 401 应被识别为时钟问题",
        )

    def test_missing_auth_401_not_treated_as_time_sync(self):
        """'缺少鉴权参数' 的 401 不是时钟问题, 不得触发校时(否则真缺鉴权却去校时)。"""
        exc = OSError(
            "Error executing request GET /ticker/BTCUSDT. HTTP status is 401. "
            'Error: {"code":401,"message":"缺少鉴权参数"}'
        )
        self.assertFalse(
            self.exchange._is_request_exception_related_to_time_synchronizer(exc),
            "'缺少鉴权参数' 不应被误判为时钟问题",
        )

    def test_clock_skew_401_english_timestamp_still_detected(self):
        """向后兼容: 含英文 'timestamp' 的 401 仍应被识别(不回归)。"""
        exc = OSError('HTTP status is 401. Error: {"message":"timestamp expired"}')
        self.assertTrue(
            self.exchange._is_request_exception_related_to_time_synchronizer(exc)
        )

    # ---- helpers --------------------------------------------------------------

    def _last_request_body(self, mock_api: aioresponses, url: str) -> str:
        for key, calls in mock_api.requests.items():
            if url in str(key[1]):
                return calls[-1].kwargs["data"]
        # fallback: last request recorded
        last = list(mock_api.requests.values())[-1][-1]
        return last.kwargs["data"]
