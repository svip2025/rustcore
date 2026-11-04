# Round 2A funding and user-stream candidate

This candidate supplies offline engineering support. Exchange settlement shape,
settlement publication delay, real listenKey expiry and post-trigger cleanup
remain **pending testnet verification**. No production or round-one deployment
is authorized by these changes.

`GetFundingHistory` (v2 service, message schema 1) queries a closed inclusive
BTCUSDT time window, up to 48 hours, from the last 89 days. It signs only
`GET /fapi/v1/income` with `incomeType=FUNDING_FEE`, `limit=1000` and explicit
start/end times. The current official [income API documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account#get-income-history-user_data)
was checked on 2026-11-04: request weight 30, inclusive boundaries, maximum 1000
records, `tranId` unique within user/income type, three-month retention. The
documented query has no page parameter. A saturated page is split into disjoint
inclusive time intervals; saturation at one millisecond, more than 63 reads,
malformed rows or an exchange error produce an error and no complete marker.

All reads and validation finish before one transaction imports immutable income
identities and the complete-window observation. Repeated imports compare the
persisted identity and exact decimal amount. A conflicting identity rolls back
the entire observation, including earlier rows. Failed queries retain all
previous successful observations. Raw exchange `tranId` is private in the
ledger; the RPC returns an opaque digest and decimal amount for each cash flow.
Public callers must persist their round/time-to-public-ID mapping for later
operator-side reconciliation. The RPC never returns an account identifier,
listenKey or unfiltered venue response.

Equity continues to use the exchange wallet balance plus unrealized PnL. The
wallet already incorporates funding cash flows. Importing the ledger **does not
credit equity a second time**. Caller round PnL adds the observed funding sum to
execution PnL less observed commissions. Missing evidence stays UNKNOWN.

`GetRuntimeDiagnostics` reports separate persisted reconnect attempts/cycles
for `USER_DATA_STREAM`, `TRADE_WS_API` and `MARKET_DATA`, with total and
last-minute counts and current user-stream readiness. Initial connections are
excluded from reconnect counts; all attempts are logged so an observed zero is
distinct from an unobserved stream. `websocket_scopes` carries `OBSERVED`,
`DISABLED` or `NOT_OBSERVED`; count fields are optional and absent for the last
two states. Historical observations remain visible even if a stream is later
disabled. Market-data counts aggregate the account's subscriptions. All three
counters survive a process restart. Before-network diagnostic write failures
retry before connecting, so a missing write cannot silently become a measured
zero. The older user-only count fields are compatibility conveniences; consumers
must read scope status before interpreting zero. Same-minute >5 checks apply to
each scope independently. `InjectUserStreamFault` accepts
`disconnect` and `listen_key_expired` only with explicit credential venue
`testnet`, `ENGINE_STAGE_MAX=demo`, `ENGINE_LIVE_ENABLED=false` (or `0`) and
`ROUND2_TESTNET_FAULTS=1`. It queues a command to the active reader without
accepting or exposing its listenKey. Both branches drop readiness before
returning, then reuse the normal backfill/reconcile/reconnect workflow.

F-15 is a reproduced baseline defect: a well-formed `listenKeyExpired` message
was parsed successfully and then silently ignored by the event sink. The fix
handles it in the reader envelope path and advances the recovery generation,
so a stale ready write cannot reopen entry admission. The malformed-frame and
other unknown-event behavior remains covered by the existing suite.
