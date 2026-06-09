import base64
import hashlib
import hmac
from typing import Any, Dict
from urllib.parse import urlencode, urlsplit

from hummingbot.connector.derivative.msx_perpetual import msx_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class MsxPerpetualAuth(AuthBase):
    """MSX 合约签名(docs/connectors/msx-api-notes.md 第1节)。

    - HMAC-SHA256, 结果 Base64。
    - preHash = timestamp(ms) + method + requestPath + queryString + body
    - requestPath 为完整 path(含 /api/v1/futures/open-api 前缀)。
    - GET: queryString = "?k1=v1&k2=v2"(按 key 排序, urlencode), 无参则空串;
      签名用的 query 必须与实际 URL query 完全一致。
    - POST: queryString 为空; body 为紧凑 JSON, 签名与发送同一字节。
    - Header: ACCESS-KEY / ACCESS-SIGN / ACCESS-TIMESTAMP(ms)。
    """

    def __init__(self, api_key: str, api_secret: str, time_provider: TimeSynchronizer):
        self._api_key: str = api_key
        self._api_secret: str = api_secret
        self._time_provider: TimeSynchronizer = time_provider

    def _timestamp_ms(self) -> int:
        return int(self._time_provider.time() * 1e3)

    def sign_payload(self, pre_hash: str) -> str:
        secret = self._api_secret.encode("utf-8")
        digest = hmac.new(secret, pre_hash.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    @staticmethod
    def _sorted_query_string(params: Dict[str, Any]) -> str:
        if not params:
            return ""
        sorted_items = sorted((str(k), str(v)) for k, v in params.items())
        return "?" + urlencode(sorted_items)

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp_ms = self._timestamp_ms()
        request_path = urlsplit(request.url).path

        if request.method == RESTMethod.GET:
            query_string = self._sorted_query_string(request.params or {})
            body = ""
        else:
            query_string = ""
            body = request.data if request.data is not None else ""

        pre_hash = f"{timestamp_ms}{request.method.value}{request_path}{query_string}{body}"
        signature = self.sign_payload(pre_hash)

        headers = dict(request.headers or {})
        headers[CONSTANTS.HEADER_ACCESS_KEY] = self._api_key
        headers[CONSTANTS.HEADER_ACCESS_SIGN] = signature
        headers[CONSTANTS.HEADER_ACCESS_TIMESTAMP] = str(timestamp_ms)
        if request.method != RESTMethod.GET:
            headers.setdefault("Content-Type", "application/json")
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # WS 鉴权走单独的 login 消息(ws_login_payload), 这里透传。
        return request

    def ws_login_payload(self) -> Dict[str, Any]:
        """私有 WS 订阅前的 login 消息。

        MSX 文档未给出 WS 私有鉴权的确切字段; 按签名规则用 timestamp 作 preHash,
        字段名 (action/apiKey/timestamp/sign) 为占位, 冒烟时按实际响应校正。
        """
        timestamp_ms = self._timestamp_ms()
        sign = self.sign_payload(f"{timestamp_ms}")
        return {
            "action": "login",
            "apiKey": self._api_key,
            "timestamp": timestamp_ms,
            "sign": sign,
        }
