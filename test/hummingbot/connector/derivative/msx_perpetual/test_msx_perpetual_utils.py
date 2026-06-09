from hummingbot.connector.derivative.msx_perpetual import msx_perpetual_utils as utils


def test_convert_from_exchange_usdt():
    assert utils.convert_from_exchange_trading_pair("BTCUSDT") == "BTC-USDT"


def test_convert_from_exchange_usdc():
    assert utils.convert_from_exchange_trading_pair("ETHUSDC") == "ETH-USDC"


def test_convert_from_exchange_already_dashed():
    assert utils.convert_from_exchange_trading_pair("BTC-USDT") == "BTC-USDT"


def test_convert_to_exchange():
    assert utils.convert_to_exchange_trading_pair("BTC-USDT") == "BTCUSDT"


def test_is_exchange_information_valid_true():
    info = {"symbol": "BTCUSDT", "coType": 3}
    assert utils.is_exchange_information_valid(info) is True


def test_is_exchange_information_valid_non_crypto():
    info = {"symbol": "AAPLUSD", "coType": 1}
    assert utils.is_exchange_information_valid(info) is False


def test_keys_field_names():
    # 配置 schema 字段以 msx_perpetual 前缀
    fields = utils.MsxPerpetualConfigMap.model_fields
    assert "msx_perpetual_api_key" in fields
    assert "msx_perpetual_api_secret" in fields
