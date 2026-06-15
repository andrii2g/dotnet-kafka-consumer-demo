#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="localhost:9092"
topic=""
count="100000"
commit_batch="1000"
poll_timeout_ms="100"
runtime_identifier=""
skip_build="false"
skip_clean="false"
start_docker="false"
cleanup_docker="false"
cleanup_images="none"
compose_file="docker/docker-compose.yml"
report_dir="reports"

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
  --skip-clean                Do not clean Release outputs before building
  --start-docker              Run docker compose up -d before benchmarking
  --cleanup-docker            Run docker compose down after benchmarking
  --cleanup-images MODE       Docker image cleanup mode: none, local, or all. Default: none
  --compose-file PATH         Docker Compose file. Default: docker/docker-compose.yml
  --report-dir PATH           Report output directory. Default: reports
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
    --skip-clean)
      skip_clean="true"
      shift
      ;;
    --start-docker)
      start_docker="true"
      shift
      ;;
    --cleanup-docker)
      cleanup_docker="true"
      shift
      ;;
    --cleanup-images)
      cleanup_images="$2"
      shift 2
      ;;
    --compose-file)
      compose_file="$2"
      shift 2
      ;;
    --report-dir)
      report_dir="$2"
      shift 2
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
compose_path="$repo_root/$compose_file"

case "$cleanup_images" in
  none|local|all) ;;
  *)
    echo "Invalid --cleanup-images value: $cleanup_images. Use none, local, or all." >&2
    exit 2
    ;;
esac

cleanup() {
  if [[ "$cleanup_docker" != "true" ]]; then
    return
  fi

  echo "> cleaning Docker Compose resources"
  cleanup_args=(compose -f "$compose_path" down --volumes --remove-orphans)
  case "$cleanup_images" in
    local) cleanup_args+=(--rmi local) ;;
    all) cleanup_args+=(--rmi all) ;;
  esac

  docker "${cleanup_args[@]}" || echo "Docker cleanup failed." >&2
}

trap cleanup EXIT

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

if [[ "$start_docker" == "true" ]]; then
  echo "> starting Docker Compose stack"
  docker compose -f "$compose_path" up -d
fi

host="${bootstrap_servers%%:*}"
port="${bootstrap_servers##*:}"

deadline=$((SECONDS + 120))
reachable="false"
if command -v nc >/dev/null 2>&1; then
  until nc -z "$host" "$port"; do
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 2
  done
  nc -z "$host" "$port" && reachable="true"
elif command -v timeout >/dev/null 2>&1; then
  until timeout 2 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 2
  done
  timeout 2 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null && reachable="true"
else
  echo "Skipping TCP reachability check because neither nc nor timeout is available."
  reachable="true"
fi

if [[ "$reachable" != "true" ]]; then
  echo "Kafka is not reachable at $bootstrap_servers." >&2
  exit 1
fi

if [[ "$skip_build" != "true" ]]; then
  if [[ "$skip_clean" != "true" ]]; then
    find "$repo_root/src" "$repo_root/tests" \
      -type d \( -name bin -o -name obj \) \
      -prune -exec rm -rf {} +

    dotnet clean "$repo_root/DotNetKafkaConsumerLab.slnx" --configuration Release -v minimal
  fi

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
producer_output="$(
  "$producer_exe" \
  --bootstrap "$bootstrap_servers" \
  --topic "$topic" \
  --count "$count" \
  --event-prefix "$event_prefix" \
  --create-topic true \
  --partitions 1 \
  --replication-factor 1
)"
echo "$producer_output"

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

report_root="$repo_root/$report_dir"
mkdir -p "$report_root"
report_file="$report_root/benchmark-$(date -u +%Y%m%d%H%M%S).md"

cat > "$report_file" <<EOF
# Kafka Consumer Benchmark

Generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

## Configuration

| Setting | Value |
| --- | --- |
| Bootstrap servers | \`$bootstrap_servers\` |
| Topic | \`$topic\` |
| Message count | \`$count\` |
| Commit batch | \`$commit_batch\` |
| Poll timeout ms | \`$poll_timeout_ms\` |
| Runtime identifier | \`$runtime_identifier\` |
| Start Docker | \`$start_docker\` |
| Cleanup Docker | \`$cleanup_docker\` |
| Cleanup images | \`$cleanup_images\` |

## Producer

\`\`\`text
$producer_output
\`\`\`

## CoreCLR/JIT Consumer

\`\`\`text
$jit_output
\`\`\`

## Native AOT Consumer

\`\`\`text
$aot_output
\`\`\`

## Commands

\`\`\`bash
dotnet "$jit_consumer_dll" --bootstrap "$bootstrap_servers" --topic "$topic" --group-id "$jit_group" --count "$count" --commit-batch "$commit_batch" --from-beginning true --deserialize true --poll-timeout-ms "$poll_timeout_ms"
"$aot_consumer_exe" --bootstrap "$bootstrap_servers" --topic "$topic" --group-id "$aot_group" --count "$count" --commit-batch "$commit_batch" --from-beginning true --deserialize true --poll-timeout-ms "$poll_timeout_ms"
\`\`\`
EOF

echo "Report: $report_file"
