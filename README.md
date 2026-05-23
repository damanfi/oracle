# daman-oracle

On-platform indexer for a Daman deployment. Reads only the deployment's own `IDamanCopyBond` contract; emits decoded events as NDJSON on stdout.

## ADR-001

No off-platform leaderboards. No third-party performance feeds. No external trader-PnL signals. The two event topics this binary subscribes to are `TradeExecuted` and `SettlementCompleted` on the deployment's own contract address. Hum is the transport layer for bee coordination; the chain is the truth.

## Configure

| env | required | default | what |
|---|---|---|---|
| `DAMAN_COPY_BOND_ADDR` | yes | none | the deployed `IDamanCopyBond` contract address |
| `DAMAN_ORACLE_RPC` | no | `https://rpc.testnet.arc.network` | JSON-RPC endpoint |
| `DAMAN_ORACLE_POLL_MS` | no | `4000` | poll interval in ms |
| `DAMAN_ORACLE_START_BLOCK` | no | latest | initial block cursor |

## Run

```
DAMAN_COPY_BOND_ADDR=0xYourCopyBond cargo run --release
```

Each decoded event prints as a single NDJSON line. Pipe to the bridge forager, a log collector, or a postgres COPY for downstream analytics.

## Wire

The two topics this binary subscribes to:

```
keccak256("TradeExecuted(address,address,uint256,bool,uint64)")
keccak256("SettlementCompleted(address,uint256,int256,uint64)")
```

Topic0 hashes are constants in `src/main.rs`. They are computed from the canonical event signature in `damanfi/protocol::IDamanCopyBond`.

## Output shape

```json
{"event":"TradeExecuted","leader":"0x...","asset":"0x...","amount":"0x...","is_long":true,"timestamp":1715000000,"block_number":42,"tx_hash":"0x..."}
{"event":"SettlementCompleted","leader":"0x...","trade_id":"0x...","pnl":"0x...","timestamp":1715000000,"block_number":42,"tx_hash":"0x..."}
```

`amount`, `pnl`, and `trade_id` are emitted as 32-byte hex words so consumers can choose their own integer-decoding policy (signed vs unsigned, big-decimal vs native u64).

## What it doesn't do

No on-chain writes. No mesh participation. No reads outside the configured contract address. The companion bridge bee (`damanfi/bridge`) is the surface that publishes these events onto hum as chi tones.

## License

Apache-2.0.
