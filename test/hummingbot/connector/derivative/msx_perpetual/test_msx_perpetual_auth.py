import asyncio
import base64
import hashlib
import hmac
import json
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.derivative.msx_perpetual import msx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.msx_perpetual.msx_perpetual_auth import MsxPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class MsxPerpetualAuthTests(TestCase):
    def setUp(self):
        self.api_key = "test_api_key"
        self.secret_key = "test_secret_key"
        self.timestamp_ms = 1705737600000
        self.auth = MsxPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.secret_key,
            time_provider=MagicMock(time=MagicMock(return_value=self.timestamp_ms / 1000.0)),
        )

    def _expected_sig(self, pre_hash: str) -> str:
        digest = hmac.new(self.secret_key.encode("utf-8"), pre_hash.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    def test_sign_payload_matches_hmac_base64(self):
        pre_hash = "1705737600000POST/api/v1/futures/open-api/order/create{}"
        sig = self.auth.sign_payload(pre_hash)
        self.assertEqual(sig, self._expected_sig(pre_hash))

    def test_rest_authenticate_post_signs_body(self):
        body = json.dumps({"symbol": "BTCUSDT", "side": 1}, separators=(",", ":"))
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api9528mystks.mystonks.org/api/v1/futures/open-api/order/create",
            data=body,
            is_auth_required=True,
        )
        signed = asyncio.get_event_loop().run_until_complete(self.auth.rest_authenticate(request))

        self.assertEqual(signed.headers[CONSTANTS.HEADER_ACCESS_KEY], self.api_key)
        self.assertEqual(signed.headers[CONSTANTS.HEADER_ACCESS_TIMESTAMP], str(self.timestamp_ms))

        expected_pre_hash = f"{self.timestamp_ms}POST/api/v1/futures/open-api/order/create{body}"
        self.assertEqual(signed.headers[CONSTANTS.HEADER_ACCESS_SIGN], self._expected_sig(expected_pre_hash))
        # body 不被改写
        self.assertEqual(signed.data, body)

    def test_rest_authenticate_get_signs_sorted_query(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api9528mystks.mystonks.org/api/v1/futures/open-api/orderbook/BTCUSDT",
            params={"depth": 20, "with_id": "true"},
            is_auth_required=True,
        )
        signed = asyncio.get_event_loop().run_until_complete(self.auth.rest_authenticate(request))

        # query 按 key 排序: depth 在 with_id 前
        expected_qs = "?depth=20&with_id=true"
        expected_pre_hash = (
            f"{self.timestamp_ms}GET/api/v1/futures/open-api/orderbook/BTCUSDT{expected_qs}"
        )
        self.assertEqual(signed.headers[CONSTANTS.HEADER_ACCESS_SIGN], self._expected_sig(expected_pre_hash))
        self.assertEqual(signed.headers[CONSTANTS.HEADER_ACCESS_KEY], self.api_key)

    def test_rest_authenticate_get_no_params_empty_query(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api9528mystks.mystonks.org/api/v1/futures/open-api/ticker/BTCUSDT",
            is_auth_required=True,
        )
        signed = asyncio.get_event_loop().run_until_complete(self.auth.rest_authenticate(request))
        expected_pre_hash = f"{self.timestamp_ms}GET/api/v1/futures/open-api/ticker/BTCUSDT"
        self.assertEqual(signed.headers[CONSTANTS.HEADER_ACCESS_SIGN], self._expected_sig(expected_pre_hash))

    def test_ws_authenticate_passthrough(self):
        ws_request = MagicMock()
        result = asyncio.get_event_loop().run_until_complete(self.auth.ws_authenticate(ws_request))
        self.assertIs(result, ws_request)

    def test_ws_login_payload_has_sign(self):
        payload = self.auth.ws_login_payload()
        self.assertEqual(payload["action"], "login")
        self.assertEqual(payload["apiKey"], self.api_key)
        self.assertEqual(payload["timestamp"], self.timestamp_ms)
        expected_pre_hash = f"{self.timestamp_ms}"
        self.assertEqual(payload["sign"], self._expected_sig(expected_pre_hash))
