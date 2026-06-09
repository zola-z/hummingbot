from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# 来源: docs/connectors/msx-api-notes.md (从 msx_api_docs/ 摘录)

EXCHANGE_NAME = "msx_perpetual"
# MSX 无 broker/client order id 概念; 订单标识为服务端 orderId。
MAX_ORDER_ID_LEN = 32

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "msx_perpetual_testnet"

# REST: 合约 base path /api/v1/futures/open-api。host 以申请账户时下发为准。
REST_HOST = "https://api9528mystks.mystonks.org"
TESTNET_REST_HOST = "https://test.test9527.xyz"
PERPETUAL_BASE_PATH = "/api/v1/futures/open-api"

PERPETUAL_BASE_URL = f"{REST_HOST}{PERPETUAL_BASE_PATH}"
TESTNET_BASE_URL = f"{TESTNET_REST_HOST}{PERPETUAL_BASE_PATH}"

# WebSocket: 单一端点, 公共行情与私有共用(私有需签名订阅)。
PERPETUAL_WS_URL = "wss://api9528mystks.mystonks.org/api/v1/futures/ws"
TESTNET_WS_URL = "wss://api9528mystks.mystonks.org/api/v1/futures/ws"

# 鉴权 header (公共/认证.md)
HEADER_ACCESS_KEY = "ACCESS-KEY"
HEADER_ACCESS_SIGN = "ACCESS-SIGN"
HEADER_ACCESS_TIMESTAMP = "ACCESS-TIMESTAMP"

# 合约类型 coType
CO_TYPE_US_STOCK = 1
CO_TYPE_HK_STOCK = 2
CO_TYPE_CRYPTO = 3
DEFAULT_CO_TYPE = CO_TYPE_CRYPTO

# 订单类型 orderType
ORDER_TYPE_LIMIT = 1
ORDER_TYPE_MARKET = 2

# 开平仓 openType / openFlag
OPEN_TYPE_OPEN = 1
OPEN_TYPE_CLOSE = 2

# 方向 side / longFlag
SIDE_LONG = 1   # 多(买)
SIDE_SHORT = 2  # 空(卖)

# 保证金模式 marginMode
MARGIN_MODE_CROSS = 1
MARGIN_MODE_ISOLATED = 2

# 触发类型 triggerType
TRIGGER_NORMAL = 1
TRIGGER_TAKE_PROFIT = 2
TRIGGER_STOP_LOSS = 3
TRIGGER_LIQUIDATION = 4

# REST Endpoints (path 相对 PERPETUAL_BASE_PATH; web_utils 拼成完整 url)
# 市场数据(公开, GET)
SNAPSHOT_PATH_URL = "/orderbook"          # GET /orderbook/{symbol}
KLINE_PATH_URL = "/kline"                 # GET /kline
TICKER_PATH_URL = "/ticker"               # GET /ticker/{symbol}
PRICE_STEPS_PATH_URL = "/price-steps"     # GET /price-steps/{symbol}

# 订单/持仓(私有, POST, 需签名)
ORDER_CREATE_PATH_URL = "/order/create"
ORDER_CANCEL_PATH_URL = "/order/cancel"
ORDER_LIMIT_PATH_URL = "/order/limit"             # 当前未成交委托
ORDER_HISTORY_PATH_URL = "/order/history"
ORDER_ENTRUST_HISTORY_PATH_URL = "/order/entrust-history"
POSITION_CURRENT_PATH_URL = "/position/current"
POSITION_HISTORY_PATH_URL = "/position/history"

# 限流标识(用于 throttler)
LIMIT_ID_PUBLIC = "PUBLIC"
LIMIT_ID_PRIVATE = "PRIVATE"

# Funding Settlement Time Span
FUNDING_SETTLEMENT_DURATION = (0, 30)  # seconds before/after snapshot

# 订单状态 status -> Hummingbot OrderState (msx-api-notes 第5节)
# 0=init 1=new 2=filled 3=part_filled 4=canceled 5=pending_cancel 6=expired
ORDER_STATE = {
    0: OrderState.OPEN,
    1: OrderState.OPEN,
    2: OrderState.FILLED,
    3: OrderState.PARTIALLY_FILLED,
    4: OrderState.CANCELED,
    5: OrderState.PENDING_CANCEL,
    6: OrderState.FAILED,
}

# WS stream id (内部区分订阅)
DIFF_STREAM_ID = 1
TRADE_STREAM_ID = 2
FUNDING_INFO_STREAM_ID = 3

# WS 心跳: 70s 无消息断开, 建议 20s 发 ping
HEARTBEAT_TIME_INTERVAL = 20.0
WS_HEARTBEAT_TIMEOUT = 70.0

# WS 信封/动作
WS_ACTION_SUBSCRIBE = "subscribe"
WS_ACTION_UNSUBSCRIBE = "unsubscribe"
WS_ACTION_PING = "ping"
WS_EVENT_PONG = "pong"

# WS channel/action 名 (msx-api-notes 第3节)
WS_ORDER_BOOK_UPDATE = "order_book_update"   # Shape A: action/result
WS_BOOK_TICKER = "book_ticker"               # Shape A
WS_CHANNEL_ORDER_BOOK = "order_book"         # Shape B: event/channel/data
WS_CHANNEL_TICKER = "ticker"                 # Shape B
WS_CHANNEL_KLINE = "kline"                   # Shape B

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1

# 做市商档限流: RPS=100, RPM=3000 (公共/限流规则.md)
MAX_RPS = 100
MAX_RPM = 3000

RATE_LIMITS = [
    # 全局池(按做市商档)
    RateLimit(limit_id=LIMIT_ID_PUBLIC, limit=MAX_RPM, time_interval=ONE_MINUTE),
    RateLimit(limit_id=LIMIT_ID_PRIVATE, limit=MAX_RPM, time_interval=ONE_MINUTE),
    # 每秒上限
    RateLimit(limit_id="PUBLIC_RPS", limit=MAX_RPS, time_interval=ONE_SECOND),
    RateLimit(limit_id="PRIVATE_RPS", limit=MAX_RPS, time_interval=ONE_SECOND),
    # 各 endpoint 权重 1, 链接到对应池 + 每秒池
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PUBLIC), LinkedLimitWeightPair("PUBLIC_RPS")]),
    RateLimit(limit_id=KLINE_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PUBLIC), LinkedLimitWeightPair("PUBLIC_RPS")]),
    RateLimit(limit_id=TICKER_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PUBLIC), LinkedLimitWeightPair("PUBLIC_RPS")]),
    RateLimit(limit_id=PRICE_STEPS_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PUBLIC), LinkedLimitWeightPair("PUBLIC_RPS")]),
    RateLimit(limit_id=ORDER_CREATE_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
    RateLimit(limit_id=ORDER_CANCEL_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
    RateLimit(limit_id=ORDER_LIMIT_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
    RateLimit(limit_id=ORDER_HISTORY_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
    RateLimit(limit_id=ORDER_ENTRUST_HISTORY_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
    RateLimit(limit_id=POSITION_CURRENT_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
    RateLimit(limit_id=POSITION_HISTORY_PATH_URL, limit=MAX_RPM, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(LIMIT_ID_PRIVATE), LinkedLimitWeightPair("PRIVATE_RPS")]),
]

# 成功码 (合约 code==200; 错误用 message 字段)
SUCCESS_CODE = 200

# 常见错误码 (公共/通用响应格式.md)
ERR_PARAM = 400
ERR_AUTH = 401
ERR_FORBIDDEN = 403
ERR_NOT_FOUND = 404
ERR_RATE_LIMIT = 429
ERR_SERVER = 500
