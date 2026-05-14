# Cognets Broker

`cognets_broker` is a modular NGSI-LD Context Broker written in Rust with `actix-web`.

Current runtime pieces:

- `actix-web` for the HTTP API
- MongoDB for persisted entities, temporal entities, subscriptions, peers, and swarm mutations
- Redis Streams for internal queueing of notifications and outbound SWIM delivery
- SWIM-style P2P membership and mutation synchronization between brokers

The codebase is being aligned to the ETSI NGSI-LD API surface:

- https://cim.etsi.org/NGSI-LD/official/front-page.html
- https://forge.etsi.org/rep/cim/ngsi-ld-openapi/-/raw/v1.8.1/openapi-3.0.3/ngsi-ld-api.yaml

This repository is no longer the original in-memory skeleton. It now uses MongoDB-backed repositories and a split module layout, but it is still not fully ETSI-complete. See `IMPLEMENTATION_STATUS.md` for the current implemented vs missing list.

## Modules

- `src/api.rs`: Actix route wiring
- `src/app`: shared application state
- `src/context`: tenant, `Via`, and `Link` header handling
- `src/domain`: persisted document and result types
- `src/federation`: queue and P2P message models
- `src/persistence`: repository traits and MongoDB implementations
- `src/query`: entity and temporal query DTOs plus Mongo query planning
- `src/services`: entity, temporal, subscription, notification, discovery, and federation logic
- `src/utils`: JSON and time helpers

## How It Works

At runtime the broker handles each request in four layers:

1. `src/api.rs` maps HTTP routes under `/ngsi-ld/v1` to service functions and normalizes request metadata.
2. `src/context/headers.rs` extracts `NGSILD-Tenant`, `Link`, and `Via`. `Link` can backfill `@context`; `Via` remains available for internal broker-to-broker metadata.
3. `src/services/*` performs validation, applies NGSI-LD semantics, persists state locally, and triggers side effects.
4. `src/persistence/mongo.rs` stores wrappers such as `{ tenant, id, doc }` in MongoDB. Redis Streams are used only for internal queued delivery of notifications and outbound SWIM messages.

Write flow:

- persist entity/temporal/subscription locally in MongoDB
- enqueue notifications when applicable
- record swarm mutation snapshots for entity and temporal writes when P2P is enabled

Read flow:

- query local MongoDB first using filters built in `src/query/planner.rs`
- apply output projection (`normalized`, `keyValues`, `GeoJSON`, temporal formats) before returning the response

Current decentralized sync model:

- local writes create durable mutation records in MongoDB
- background SWIM worker updates peer liveness, pushes outbound SWIM events, and pulls remote mutation snapshots over internal HTTP endpoints
- Redis Streams stay local to each broker and are not shared across nodes

## Implemented Routes

- `POST /ngsi-ld/v1/entities`
- `GET /ngsi-ld/v1/entities`
- `GET /ngsi-ld/v1/entities/{entityId}`
- `DELETE /ngsi-ld/v1/entities/{entityId}`
- `PATCH /ngsi-ld/v1/entities/{entityId}`
- `PUT /ngsi-ld/v1/entities/{entityId}`
- `POST /ngsi-ld/v1/entities/{entityId}/attrs`
- `PATCH /ngsi-ld/v1/entities/{entityId}/attrs`
- `PATCH /ngsi-ld/v1/entities/{entityId}/attrs/{attrId}`
- `DELETE /ngsi-ld/v1/entities/{entityId}/attrs/{attrId}`
- `PUT /ngsi-ld/v1/entities/{entityId}/attrs/{attrId}`
- `POST /ngsi-ld/v1/entityOperations/create`
- `POST /ngsi-ld/v1/entityOperations/upsert`
- `POST /ngsi-ld/v1/entityOperations/update`
- `POST /ngsi-ld/v1/entityOperations/delete`
- `POST /ngsi-ld/v1/entityOperations/query`
- `POST /ngsi-ld/v1/subscriptions`
- `GET /ngsi-ld/v1/subscriptions`
- `GET /ngsi-ld/v1/subscriptions/{subscriptionId}`
- `PATCH /ngsi-ld/v1/subscriptions/{subscriptionId}`
- `DELETE /ngsi-ld/v1/subscriptions/{subscriptionId}`
- `POST /ngsi-ld/v1/temporal/entities`
- `GET /ngsi-ld/v1/temporal/entities`
- `GET /ngsi-ld/v1/temporal/entities/{entityId}`
- `GET /ngsi-ld/v1/temporal/entities/{entityId}/attrs`
- `GET /ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}`
- `GET /ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}`
- `DELETE /ngsi-ld/v1/temporal/entities/{entityId}`
- `POST /ngsi-ld/v1/temporal/entities/{entityId}/attrs`
- `DELETE /ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}`
- `PATCH /ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}`
- `DELETE /ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}`
- `POST /ngsi-ld/v1/temporal/entityOperations/query`
- `GET /ngsi-ld/v1/types`
- `GET /ngsi-ld/v1/types/{type}`
- `GET /ngsi-ld/v1/attributes`
- `GET /ngsi-ld/v1/attributes/{attrId}`
- `POST /internal/swim`
- `GET /internal/swim/mutations`

## Query Support

Implemented entity query features:

- `id`, `type`, `idPattern`, `attrs`, `pick`, `omit`, `limit`, `count`, `local`
- `q` expressions with `==`, `!=`, `>`, `>=`, `<`, `<=`
- simple range expressions in the form `attr..lower,upper`
- top-level `;` and `|` composition for AND and OR
- normalized JSON, `keyValues`, and GeoJSON response projections

Implemented discovery features:

- `/types` returns `EntityTypeList` or detailed `EntityTypeInfo[]` with `details=true`
- `/types/{type}` returns `EntityTypeInfo`
- `/attributes` returns `AttributeList` or detailed `Attribute[]` with `details=true`
- `/attributes/{attrId}` returns `Attribute`

Implemented geo query features:

- `geometry`, `georel`, `coordinates`, `geoproperty`
- geometries: `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`
- `georel`: `within`, `intersects`, `contains`, `overlaps`, `equals`, and `near` with `minDistance` and `maxDistance`

Implemented temporal query features:

- `timerel=before|after|between`
- `timeAt`, `endTimeAt`, `timeproperty`, `lastN`
- `format=temporalValues|aggregatedValues`
- basic aggregation over numeric values for `sum`, `avg`, `min`, `max`, `totalCount`, and `distinctCount`

## Delivery And Sync

Implemented delivery and sync pieces:

- Redis-backed worker for outbound notification delivery and outbound SWIM HTTP delivery
- peer persistence in MongoDB
- SWIM-style membership with `alive`, `suspect`, and `dead` peer states
- durable swarm mutation log stored in MongoDB
- duplicate suppression by `mutationId`
- local mutation recording for entity and temporal write paths as full-state snapshots
- remote mutation application followed by local NGSI-LD subscription notifications
- root-level internal broker endpoints for SWIM event intake and mutation anti-entropy reads

## Swarm Model

Current P2P model is SWIM-only.

Each broker:

- persists peer membership state in MongoDB
- publishes outbound SWIM `alive`, `suspect`, `dead`, and mutation events through its local Redis worker
- records local mutations into durable MongoDB mutation log
- exchanges mutation snapshots broker-to-broker through internal HTTP endpoints
- applies unseen remote mutations only when incoming version is newer

This keeps convergence state-based. Peers synchronize resource snapshots and membership state, not public write requests.

## Configuration

Environment variables:

- `BROKER_HOST`: bind host, default `127.0.0.1`
- `BROKER_PORT`: bind port, default `8080`
- `BROKER_ID`: broker identifier, default `cognets-broker`
- `BROKER_PUBLIC_ENDPOINT`: public broker base URL, default `http://127.0.0.1:8080/ngsi-ld/v1`
- `BROKER_MONGO_URL`: MongoDB connection string, default `mongodb://127.0.0.1:27017`
- `BROKER_MONGO_DATABASE`: MongoDB database name, default `cognets_broker`
- `BROKER_REDIS_URL`: Redis connection string, default `redis://127.0.0.1/`
- `BROKER_REDIS_STREAM`: Redis Stream name, default `ngsild:internal`
- `BROKER_REDIS_CONSUMER_GROUP`: Redis consumer group, default `ngsild-brokers`
- `BROKER_REDIS_CONSUMER_NAME`: Redis consumer name, default value of `BROKER_ID`
- `BROKER_OUTBOUND_TIMEOUT_MS`: outbound HTTP timeout in milliseconds, default `5000`
- `BROKER_P2P_ENABLED`: enable P2P logic, default `true`
- `BROKER_P2P_SYNC_INTERVAL_MS`: SWIM probe and maintenance interval in milliseconds, default `10000`
- `BROKER_P2P_SWIM_SUSPECT_TIMEOUT_MS`: timeout before alive peers degrade to suspect, default `15000`
- `BROKER_P2P_SEEDS`: comma-separated peer endpoints, default empty

For P2P peers, the recommended value format is a broker base URL such as `http://127.0.0.1:8081/ngsi-ld/v1`.

## Run

Start MongoDB and Redis first, then run:

```bash
cargo run
```

Default public base URL:

```text
http://127.0.0.1:8080/ngsi-ld/v1
```

OpenAPI docs:

```text
http://127.0.0.1:8080/api-docs/openapi.json
http://127.0.0.1:8080/swagger-ui/
```

## Test

Run:

```bash
cargo test
```

Current automated coverage includes:

- query planner parsing and BSON generation
- geo and temporal validation paths
- temporal filtering and aggregation helpers
- internal URL construction and SWIM membership helper behavior
- notification matching rules
- handler-level validation responses for bad requests

The current suite does not include live end-to-end tests against running MongoDB and Redis services.

## Current Gaps

Major gaps are tracked in `IMPLEMENTATION_STATUS.md`.

Important current limits:

- several NGSI-LD query parameters are parsed but not yet implemented end-to-end
- JSON-LD context cache APIs are not implemented
- the swarm model still relies on wall-clock ordering via `modifiedAt`; there is no vector-clock or CRDT conflict model yet
- mutation-log retention, pruning, and compaction policies are not implemented yet
- peer authentication, authorization, and trust scoring are not implemented for SWIM participants
- notification retry scheduling, backoff, and dead-letter handling are not implemented
