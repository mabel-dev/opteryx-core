#!/usr/bin/env bash
# One-off: boot the Confluent stack in Colima, write Kafka Connect Avro files, tear down.
#   [UV=/path/to/uv] dev/kafka_avro/run.sh <rows> <out-dir> [flush-size]
#   small (committed):  dev/kafka_avro/run.sh 500 testdata/avro/kafka_connect/confluent 100
#   large (gitignored): dev/kafka_avro/run.sh 1000000 testdata/avro_kafka_large/confluent 50000
set -euo pipefail
ROWS=${1:?rows}; OUT=${2:?out dir}; FLUSH=${3:-100}
HERE=$(cd "$(dirname "$0")" && pwd)
colima status >/dev/null 2>&1 || colima start --cpu 4 --memory 8
docker compose -f "$HERE/docker-compose.yml" -p kafka_avro up -d
trap 'docker compose -f "$HERE/docker-compose.yml" -p kafka_avro down -v' EXIT
"${UV:-uv}" run --no-project --with 'confluent-kafka[avro,schemaregistry]' --with boto3 --with cramjam \
    python "$HERE/generate.py" --rows "$ROWS" --flush-size "$FLUSH" --out "$OUT"
