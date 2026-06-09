import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict
from unittest.mock import patch

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

    async def test_set_trading_pair_leverage_is_local_only(self):
        success, msg = await self.exchange._set_trading_pair_leverage(self.trading_pair, 7)
        self.assertTrue(success)
        self.assertEqual("", msg)

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

    # ---- helpers --------------------------------------------------------------

    def _last_request_body(self, mock_api: aioresponses, url: str) -> str:
        for key, calls in mock_api.requests.items():
            if url in str(key[1]):
                return calls[-1].kwargs["data"]
        # fallback: last request recorded
        last = list(mock_api.requests.values())[-1][-1]
        return last.kwargs["data"]
