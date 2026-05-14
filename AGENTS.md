# AGENTS.md

## Response Style
- User-facing replies stay terse "smart caveman". Switch to normal wording for security warnings, irreversible actions, and code/commit/PR text.

## Verify
- No repo CI/workflow files. Pre-handoff check: `cargo fmt --check && cargo test`.

## Run
- `cargo run` expects MongoDB and Redis already up. Startup connects to Mongo, creates indexes, and starts Redis Stream worker immediately.
- Config comes from `std::env`. `.env.example` is reference only; `.env` is gitignored but not auto-loaded.
- Use broker base URLs ending in `/ngsi-ld/v1` for `BROKER_PUBLIC_ENDPOINT` and `BROKER_P2P_SEEDS`. Internal broker sync routes now live at root paths such as `/internal/swim`.

## Architecture
- `src/api.rs` only wires Actix routes. Real behavior lives in `src/services/*`; Mongo filter building lives in `src/query/planner.rs`; tenant/`Link`/`Via` handling lives in `src/context/headers.rs`.
- Persisted entities are wrappers, not raw NGSI-LD payloads: entities use `{ tenant, id, doc }`; temporals use `{ tenant, id, doc, history }`. Mongo filters/indexes use top-level `id` plus nested `doc.*` fields.
- Every repository key/index includes `tenant`. `NGSILD-Tenant` defaults to `default`; forgetting tenant filters causes cross-tenant bugs.
- `Link` header backfills `@context` only when payload lacks it. `Via` header is federation/P2P loop-avoidance state; preserve and extend it when changing forwarding code.
- `services::entities::{query,get}` read local Mongo only. Write paths persist locally first, then enqueue notifications and swarm side effects.
- Geo queries can target arbitrary `geoproperty`, but startup creates 2dsphere index only for `doc.location.value`.

## P2P And Delivery Quirks
- Runtime launches `services::federation::start_swarm_sync_worker` at startup.
- Current P2P model is SWIM-only. Redis stays internal; broker-to-broker sync uses root-level internal endpoints `/internal/swim` and `/internal/swim/mutations`.
- `RedisEventQueue` ACKs stream messages after each processing attempt, even failures. Current outbound SWIM/notification delivery is best-effort; no retry/dead-letter flow.

## Tests
- API tests mirror `api::tests::test_state()`: `AppConfig::for_tests()`, `p2p_enabled = false`, `InMemoryEventQueue`, `MongoRepositories::new_without_indexes()`.
- `cargo test` still needs MongoDB on `127.0.0.1:27017` for API/swarm-mutation tests. Redis is not required by current test suite.
