#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE="${MODE:-embedded}"
CERTS_DIR="${CERTS_DIR:-${REPO_ROOT}/../clustor/examples/mini-cluster/certs}"
DATA_DIR="${DATA_DIR:-${REPO_ROOT}/target/ceptra-clustor}"
ADMIN_BIND="${ADMIN_BIND:-127.0.0.1:26020}"
RAFT_BIND="${RAFT_BIND:-127.0.0.1:26021}"
ADVERTISE_HOST="${ADVERTISE_HOST:-127.0.0.1}"
CONTROL_PLANE_URL="${CONTROL_PLANE_URL:-https://${ADVERTISE_HOST}:26020}"

if [[ "${1:-}" == "--external" ]]; then
  MODE="external"
  shift
fi

if [[ -n "${CEPTRA_CONTROL_PLANE_URL:-}" ]]; then
  MODE="external"
fi

mkdir -p "${DATA_DIR}"
export CEPTRA_TLS_IDENTITY_CERT="${CERTS_DIR}/node-a-chain.pem"
export CEPTRA_TLS_IDENTITY_KEY="${CERTS_DIR}/node-a-key.pem"
export CEPTRA_TLS_TRUST_BUNDLE="${CERTS_DIR}/ca.pem"
export CEPTRA_ADMIN_BIND="${ADMIN_BIND}"
export CEPTRA_RAFT_BIND="${RAFT_BIND}"
export CEPTRA_ADVERTISE_HOST="${ADVERTISE_HOST}"
export CEPTRA_DATA_DIR="${DATA_DIR}"
export CEPTRA_WAL_DIR="${DATA_DIR}/wal"
export CEPTRA_SNAPSHOT_DIR="${DATA_DIR}/snapshots"
if [[ "${MODE}" == "external" ]]; then
  export CEPTRA_CLUSTOR_MODE="external"
  export CEPTRA_CONTROL_PLANE_URL="${CEPTRA_CONTROL_PLANE_URL:-${CONTROL_PLANE_URL}}"
else
  export CEPTRA_CLUSTOR_MODE="embedded"
  if [[ "${CEPTRA_STUB_RUNTIME:-0}" == "1" ]]; then
    export CEPTRA_STUB_RUNTIME="1"
    export CEPTRA_ALLOW_SEED_PLACEMENTS="${CEPTRA_ALLOW_SEED_PLACEMENTS:-1}"
  else
    unset CEPTRA_STUB_RUNTIME
    unset CEPTRA_ALLOW_SEED_PLACEMENTS
  fi
fi

echo "Starting CEPtra (${MODE}) with admin=${CEPTRA_ADMIN_BIND} raft=${CEPTRA_RAFT_BIND} data_dir=${DATA_DIR}"
exec cargo run --bin ceptra "$@"
