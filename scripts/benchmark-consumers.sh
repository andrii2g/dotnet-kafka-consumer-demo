#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="localhost:9092"
topic=""
count="100000"
commit_batch="1000"
poll_timeout_ms="100"
runtime_identifier=""
skip_build="false"

usage() {
  cat <<'EOF'
Usage: scripts/benchmark-consumers.sh [options]

Options:
  --bootstrap HOST:PORT       Kafka bootstrap servers. Default: localhost:9092
  --topic TOPIC               Benchmark topic. Default: generated unique topic
  --count N                   Messages to produce and consume. Default: 100000
  --commit-batch N            Consumer commit batch size. Default: 1000
  --poll-timeout-ms N         Consumer poll timeout. Default: 100
  --rid RID                   Runtime identifier. Default: linux-x64 on Linux, win-x64 on Git Bash/Windows
  --skip-build                Reuse existing build/publish outputs
  -h, --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap)
      bootstrap_servers="$2"
      shift 2
      ;;
    --topic)
      topic="$2"
      shift 2
      ;;
    --count)
      count="$2"
      shift 2
      ;;
    --commit-batch)
      commit_batch="$2"
      shift 2
      ;;
    --poll-timeout-ms)
      poll_timeout_ms="$2"
      shift 2
      ;;
    --rid)
      runtime_identifier="$2"
      shift 2
      ;;
    --skip-build)
      skip_build="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

if [[ -z "$runtime_identifier" ]]; then
  case "$(uname -s)" in
    Linux*) runtime_identifier="linux-x64" ;;
    MINGW*|MSYS*|CYGWIN*) runtime_identifier="win-x64" ;;
    *)
      echo "Unable to infer runtime identifier for $(uname -s). Pass --rid explicitly." >&2
      exit 2
      ;;
  esac
fi

if [[ -z "$topic" ]]; then
  topic="orders.created.bench.$(date -u +%Y%m%d%H%M%S)"
fi

host="${bootstrap_servers%%:*}"
port="${bootstrap_servers##*:}"

if command -v nc >/dev/null 2>&1; then
  nc -z "$host" "$port" || {
    echo "Kafka is not reachable at $bootstrap_servers." >&2
    exit 1
  }
elif command -v timeout >/dev/null 2>&1; then
  timeout 2 bash -c "cat < /dev/null > /dev/tcp/$host/$port" || {
    echo "Kafka is not reachable at $bootstrap_servers." >&2
    exit 1
  }
else
  echo "Skipping TCP reachability check because neither nc nor timeout is available."
fi

if [[ "$skip_build" != "true" ]]; then
  dotnet restore "$repo_root/DotNetKafkaConsumerLab.slnx"
  dotnet build "$repo_root/DotNetKafkaConsumerLab.slnx" --configuration Release --no-restore -v minimal
  dotnet publish "$repo_root/src/KafkaConsumerLab.AotProducer/KafkaConsumerLab.AotProducer.csproj" -c Release -r "$runtime_identifier" --no-restore -v minimal
  dotnet publish "$repo_root/src/KafkaConsumerLab.AotConsumer/KafkaConsumerLab.AotConsumer.csproj" -c Release -r "$runtime_identifier" --no-restore -v minimal
fi

publish_dir="$repo_root/src/KafkaConsumerLab.AotConsumer/bin/Release/net10.0/$runtime_identifier/publish"
producer_publish_dir="$repo_root/src/KafkaConsumerLab.AotProducer/bin/Release/net10.0/$runtime_identifier/publish"
jit_consumer_dll="$repo_root/src/KafkaConsumerLab.AotConsumer/bin/Release/net10.0/$runtime_identifier/KafkaConsumerLab.AotConsumer.dll"

case "$runtime_identifier" in
  win-*)
    producer_exe="$producer_publish_dir/KafkaConsumerLab.AotProducer.exe"
    aot_consumer_exe="$publish_dir/KafkaConsumerLab.AotConsumer.exe"
    ;;
  *)
    producer_exe="$producer_publish_dir/KafkaConsumerLab.AotProducer"
    aot_consumer_exe="$publish_dir/KafkaConsumerLab.AotConsumer"
    ;;
esac

if [[ ! -x "$producer_exe" ]]; then
  echo "Producer executable not found or not executable: $producer_exe" >&2
  exit 1
fi

if [[ ! -x "$aot_consumer_exe" ]]; then
  echo "AOT consumer executable not found or not executable: $aot_consumer_exe" >&2
  exit 1
fi

if [[ ! -f "$jit_consumer_dll" ]]; then
  echo "JIT consumer DLL not found: $jit_consumer_dll" >&2
  exit 1
fi

event_prefix="bench-$(date -u +%Y%m%d%H%M%S)"
jit_group="bench-jit-$(date -u +%Y%m%d%H%M%S)-$$"
aot_group="bench-aot-$(date -u +%Y%m%d%H%M%S)-$$"

echo "> producing $count messages to $topic"
"$producer_exe" \
  --bootstrap "$bootstrap_servers" \
  --topic "$topic" \
  --count "$count" \
  --event-prefix "$event_prefix" \
  --create-topic true \
  --partitions 1 \
  --replication-factor 1

echo "> JIT consumer"
jit_output="$(
  dotnet "$jit_consumer_dll" \
    --bootstrap "$bootstrap_servers" \
    --topic "$topic" \
    --group-id "$jit_group" \
    --count "$count" \
    --commit-batch "$commit_batch" \
    --from-beginning true \
    --deserialize true \
    --poll-timeout-ms "$poll_timeout_ms"
)"
echo "$jit_output"

echo "> AOT consumer"
aot_output="$(
  "$aot_consumer_exe" \
    --bootstrap "$bootstrap_servers" \
    --topic "$topic" \
    --group-id "$aot_group" \
    --count "$count" \
    --commit-batch "$commit_batch" \
    --from-beginning true \
    --deserialize true \
    --poll-timeout-ms "$poll_timeout_ms"
)"
echo "$aot_output"

echo
echo "Benchmark topic: $topic"
echo "JIT consumer: $jit_output"
echo "AOT consumer: $aot_output"
