from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.connector.derivative.msx_perpetual import msx_perpetual_constants as CONSTANTS
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# 费率: 以 MSX 实际做市商费率为准, 暂用占位(冒烟时校正)。
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0005"),
    buy_percent_fee_deducted_from_returns=True,
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

# 已知 quote 资产后缀(用于 BTCUSDT -> BTC-USDT 切分)。
_QUOTE_ASSETS = ("USDT", "USDC", "USD")


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    """MSX symbol(如 BTCUSDT) -> Hummingbot trading pair(BTC-USDT)。"""
    if "-" in exchange_trading_pair:
        return exchange_trading_pair
    for quote in _QUOTE_ASSETS:
        if exchange_trading_pair.endswith(quote) and len(exchange_trading_pair) > len(quote):
            base = exchange_trading_pair[: -len(quote)]
            return f"{base}-{quote}"
    raise ValueError(f"Unrecognized MSX symbol: {exchange_trading_pair}")


def convert_to_exchange_trading_pair(trading_pair: str) -> str:
    """Hummingbot trading pair(BTC-USDT) -> MSX symbol(BTCUSDT)。"""
    return trading_pair.replace("-", "")


def is_exchange_information_valid(exchange_info: dict) -> bool:
    """Phase 1 仅数字货币合约(coType=3)。"""
    return exchange_info.get("coType") == CONSTANTS.CO_TYPE_CRYPTO


class MsxPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "msx_perpetual"
    msx_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MSX Perpetual API key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    msx_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MSX Perpetual API secret",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )


KEYS = MsxPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["msx_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"msx_perpetual_testnet": "msx_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"msx_perpetual_testnet": "BTC-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"msx_perpetual_testnet": [0.02, 0.05]}


class MsxPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "msx_perpetual_testnet"
    msx_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MSX Perpetual testnet API key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    msx_perpetual_testnet_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MSX Perpetual testnet API secret",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    model_config = ConfigDict(title="msx_perpetual")


OTHER_DOMAINS_KEYS = {"msx_perpetual_testnet": MsxPerpetualTestnetConfigMap.model_construct()}
