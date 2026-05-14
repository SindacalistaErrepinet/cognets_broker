#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${SWARM_PROJECT:-cognets-swarm}"
KEEP_SWARM="${KEEP_SWARM:-0}"
BROKER_COUNT=10
ENTITY_ID="urn:ngsi-ld:Vehicle:swarm-e2e"
CREATE_SPEED=10
UPDATE_SPEED=55
SINK_URL="http://127.0.0.1:19080"
COMPOSE=(docker-compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
DEFRA_SERVICES=()
BROKER_SERVICES=()

for index in $(seq 1 "$BROKER_COUNT"); do
  DEFRA_SERVICES+=("defra${index}")
  BROKER_SERVICES+=("broker${index}")
done

cleanup() {
  local status=$?
  trap - EXIT

  if [[ $status -ne 0 ]]; then
    printf 'Swarm test failed. Recent compose logs:\n' >&2
    "${COMPOSE[@]}" logs --tail=200 >&2 || true
  fi

  if [[ "$KEEP_SWARM" != "1" ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi

  exit "$status"
}

trap cleanup EXIT

broker_port() {
  printf '%s' "$((18080 + $1))"
}

broker_api_base() {
  printf 'http://127.0.0.1:%s' "$(broker_port "$1")"
}

broker_url() {
  printf '%s/ngsi-ld/v1' "$(broker_api_base "$1")"
}

subscription_id() {
  printf 'urn:ngsi-ld:Subscription:swarm-%s' "$1"
}

urlencode() {
  printf '%s' "$1" | jq -sRr @uri
}

wait_until() {
  local description=$1
  local timeout_seconds=$2
  shift 2

  for attempt in $(seq 1 "$timeout_seconds"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s\n' "$description" >&2
  return 1
}

sink_ready() {
  curl -fsS "$SINK_URL/health" >/dev/null 2>&1
}

broker_ready() {
  local index=$1
  curl -fsS "$(broker_api_base "$index")/api-docs/openapi.json" >/dev/null 2>&1
}

channel_messages() {
  local channel=$1
  curl -fsS "$SINK_URL/messages?channel=$channel"
}

all_channels_match() {
  local expected_count=$1
  local expected_speed=$2
  local payload

  for index in $(seq 1 "$BROKER_COUNT"); do
    payload="$(channel_messages "node${index}")" || return 1
    [[ "$(printf '%s' "$payload" | jq 'length')" == "$expected_count" ]] || return 1
    [[ "$(printf '%s' "$payload" | jq -r '.[-1].body.data[0].id // empty')" == "$ENTITY_ID" ]] || return 1
    [[ "$(printf '%s' "$payload" | jq -r '.[-1].body.data[0].speed.value // empty')" == "$expected_speed" ]] || return 1
  done

  return 0
}

all_entities_match_speed() {
  local expected_speed=$1
  local encoded_id
  local body_file
  local status_code

  encoded_id="$(urlencode "$ENTITY_ID")"

  for index in $(seq 1 "$BROKER_COUNT"); do
    body_file="$(mktemp)"
    status_code="$(curl -sS -o "$body_file" -w '%{http_code}' "$(broker_url "$index")/entities/$encoded_id")" || {
      rm -f "$body_file"
      return 1
    }

    if [[ "$status_code" != "200" ]]; then
      rm -f "$body_file"
      return 1
    fi

    if [[ "$(jq -r '.speed.value // empty' "$body_file")" != "$expected_speed" ]]; then
      rm -f "$body_file"
      return 1
    fi

    rm -f "$body_file"
  done

  return 0
}

all_entities_deleted() {
  local encoded_id
  local body_file
  local status_code

  encoded_id="$(urlencode "$ENTITY_ID")"

  for index in $(seq 1 "$BROKER_COUNT"); do
    body_file="$(mktemp)"
    status_code="$(curl -sS -o "$body_file" -w '%{http_code}' "$(broker_url "$index")/entities/$encoded_id")" || {
      rm -f "$body_file"
      return 1
    }

    rm -f "$body_file"
    [[ "$status_code" == "404" ]] || return 1
  done

  return 0
}

subscription_locality_holds() {
  local owner_index=$1
  local encoded_id
  local body_file
  local status_code

  encoded_id="$(urlencode "$(subscription_id "$owner_index")")"

  for index in $(seq 1 "$BROKER_COUNT"); do
    body_file="$(mktemp)"
    status_code="$(curl -sS -o "$body_file" -w '%{http_code}' "$(broker_url "$index")/subscriptions/$encoded_id")" || {
      rm -f "$body_file"
      return 1
    }
    rm -f "$body_file"

    if [[ "$index" == "$owner_index" ]]; then
      [[ "$status_code" == "200" ]] || return 1
    else
      [[ "$status_code" == "404" ]] || return 1
    fi
  done

  return 0
}

subscriptions_remain_local() {
  local owner_index

  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    subscription_locality_holds "$owner_index" || return 1
  done

  return 0
}

create_subscription() {
  local index=$1
  local payload

  payload="$(jq -nc \
    --arg id "$(subscription_id "$index")" \
    --arg endpoint "http://sink:8080/notify/node${index}" \
    '{
      id: $id,
      type: "Subscription",
      entities: [{type: "Vehicle"}],
      watchedAttributes: ["speed"],
      notificationTrigger: [
        "entityCreated",
        "entityUpdated",
        "entityDeleted",
        "attributeCreated",
        "attributeUpdated",
        "attributeDeleted"
      ],
      notification: {
        endpoint: {
          uri: $endpoint
        }
      }
    }')"

  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url "$index")/subscriptions" \
    -o /dev/null
}

create_entity() {
  local payload

  payload="$(jq -nc \
    --arg id "$ENTITY_ID" \
    --argjson speed "$CREATE_SPEED" \
    '{
      id: $id,
      type: "Vehicle",
      speed: {
        type: "Property",
        value: $speed
      }
    }')"

  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url 1)/entities" \
    -o /dev/null
}

update_entity() {
  local payload
  local encoded_id

  payload="$(jq -nc --argjson speed "$UPDATE_SPEED" '{type: "Property", value: $speed}')"
  encoded_id="$(urlencode "$ENTITY_ID")"

  curl -fsS \
    -X PUT \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url 5)/entities/$encoded_id/attrs/speed" \
    -o /dev/null
}

delete_entity() {
  local encoded_id

  encoded_id="$(urlencode "$ENTITY_ID")"
  curl -fsS \
    -X DELETE \
    "$(broker_url 9)/entities/$encoded_id" \
    -o /dev/null
}

printf 'Starting 10-node swarm stack\n'
if [[ ! -x "$ROOT_DIR/target/debug/cognets_broker" ]]; then
  printf 'Building broker binary on host\n'
  cargo build >/dev/null
fi

"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d sink "${DEFRA_SERVICES[@]}"

wait_until 'notification sink' 60 sink_ready

printf 'Bootstrapping DefraDB P2P swarm\n'
bash "$ROOT_DIR/scripts/bootstrap_swarm.sh"

printf 'Starting broker nodes\n'
"${COMPOSE[@]}" up -d "${BROKER_SERVICES[@]}"

for index in $(seq 1 "$BROKER_COUNT"); do
  wait_until "broker${index}" 180 broker_ready "$index"
done

printf 'Resetting sink state\n'
curl -fsS -X DELETE "$SINK_URL/messages" -o /dev/null

printf 'Creating local subscriptions on all brokers\n'
for index in $(seq 1 "$BROKER_COUNT"); do
  create_subscription "$index"
done
wait_until 'subscriptions remain local' 20 subscriptions_remain_local

printf 'Creating entity on broker1\n'
create_entity
wait_until 'entity replication after create' 180 all_entities_match_speed "$CREATE_SPEED"
wait_until 'create notifications on all nodes' 180 all_channels_match 1 "$CREATE_SPEED"

printf 'Updating entity on broker5\n'
update_entity
wait_until 'entity replication after update' 180 all_entities_match_speed "$UPDATE_SPEED"
wait_until 'update notifications on all nodes' 180 all_channels_match 2 "$UPDATE_SPEED"

printf 'Deleting entity on broker9\n'
delete_entity
wait_until 'entity deletion replication' 180 all_entities_deleted
wait_until 'delete notifications on all nodes' 180 all_channels_match 3 "$UPDATE_SPEED"
wait_until 'subscriptions remain local after entity lifecycle' 20 subscriptions_remain_local

printf '10-node swarm integration test passed\n'
