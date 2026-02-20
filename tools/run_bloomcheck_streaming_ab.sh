#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <db_path> <common_db_bench_args...>"
  echo "Example:"
  echo "  $0 /mnt/kvafa/db_bench --benchmarks=readrandom --threads=128 --num=1000000 --reads=200000"
  exit 1
fi

DB_PATH="$1"
shift

COMMON_ARGS=("$@")
DB_BENCH_BIN="${DB_BENCH_BIN:-./db_bench}"

run_mode() {
  local mode="$1"
  local use_streaming="$2"
  local out_file="bloomcheck_${mode}.log"

  echo "[run] mode=${mode} BLOOMCHECK_USE_STREAMING=${use_streaming}"
  BLOOMCHECK_RPC_LOG=1 \
  BLOOMCHECK_RPC_LOG_SAMPLE_RATE=1 \
  BLOOMCHECK_USE_STREAMING="${use_streaming}" \
  "${DB_BENCH_BIN}" --db="${DB_PATH}" "${COMMON_ARGS[@]}" \
    2>&1 | tee "${out_file}"

  echo "[extract] ${out_file}"
  grep -E "\\[BloomCheckRPC\\]" "${out_file}" | tail -n 20 || true
}

run_mode "unary" 0
run_mode "streaming" 1

echo "[done] Compare bloomcheck_unary.log vs bloomcheck_streaming.log"

