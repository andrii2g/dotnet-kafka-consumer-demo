# Consumer Benchmark

Quick run that starts Kafka, runs the benchmark, and removes containers, volumes, and images afterward:

```bash
bash scripts/benchmark-consumers.sh --count 100000 --start-docker --cleanup-docker --cleanup-images all
```

Windows PowerShell:

```powershell
.\scripts\benchmark-consumers.ps1 -Count 100000 -StartDocker -CleanupDocker -CleanupImages all
```

Use `--cleanup-images local` / `-CleanupImages local` if you only want Docker Compose to remove images built by this project. Use `all` to also remove pulled service images such as Kafka and Kafka UI.

The Docker Compose stack uses `apache/kafka:4.2.1` and exposes Kafka to benchmark clients at `localhost:9092`.

The repository `global.json` asks for .NET SDK `10.0.100` with `latestFeature` roll-forward, so any installed .NET 10 SDK at `10.0.100` or newer should work.

The benchmark scripts:

- remove `bin` and `obj` build outputs by default, which avoids stale Windows/WSL restore artifacts
- restore and build the .NET 10 solution
- publish the AOT producer and AOT consumer
- create a fresh benchmark topic
- produce the requested number of messages
- consume the same topic once with CoreCLR/JIT
- consume the same topic once with Native AOT
- print elapsed time and messages per second for both consumers
- write a Markdown report under `reports/`

## Bash

Default run:

```bash
bash scripts/benchmark-consumers.sh --count 100000
```

Start Docker Compose and clean up everything after the benchmark:

```bash
bash scripts/benchmark-consumers.sh --count 100000 --start-docker --cleanup-docker --cleanup-images all
```

With common options:

```bash
bash scripts/benchmark-consumers.sh \
  --bootstrap localhost:9092 \
  --count 100000 \
  --commit-batch 1000 \
  --poll-timeout-ms 100 \
  --rid linux-x64 \
  --start-docker \
  --cleanup-docker \
  --cleanup-images all
```

Repeat run with existing binaries:

```bash
bash scripts/benchmark-consumers.sh --count 100000 --skip-build
```

## PowerShell

Default run:

```powershell
.\scripts\benchmark-consumers.ps1 -Count 100000
```

Start Docker Compose and clean up everything after the benchmark:

```powershell
.\scripts\benchmark-consumers.ps1 -Count 100000 -StartDocker -CleanupDocker -CleanupImages all
```

With common options:

```powershell
.\scripts\benchmark-consumers.ps1 `
  -BootstrapServers localhost:9092 `
  -Count 100000 `
  -CommitBatch 1000 `
  -PollTimeoutMs 100 `
  -RuntimeIdentifier win-x64 `
  -StartDocker `
  -CleanupDocker `
  -CleanupImages all
```

Repeat run with existing binaries:

```powershell
.\scripts\benchmark-consumers.ps1 -Count 100000 -SkipBuild
```

## Options

| Option | Bash | PowerShell | Default |
| --- | --- | --- | --- |
| Kafka bootstrap servers | `--bootstrap` | `-BootstrapServers` | `localhost:9092` |
| Benchmark topic | `--topic` | `-Topic` | generated unique topic |
| Message count | `--count` | `-Count` | `100000` |
| Consumer commit batch | `--commit-batch` | `-CommitBatch` | `1000` |
| Poll timeout | `--poll-timeout-ms` | `-PollTimeoutMs` | `100` |
| Runtime identifier | `--rid` | `-RuntimeIdentifier` | `linux-x64` in Bash on Linux, `win-x64` in PowerShell |
| Reuse existing build outputs | `--skip-build` | `-SkipBuild` | disabled |
| Keep existing build outputs before build | `--skip-clean` | `-SkipClean` | disabled |
| Start Docker Compose | `--start-docker` | `-StartDocker` | disabled |
| Stop Docker Compose and remove volumes | `--cleanup-docker` | `-CleanupDocker` | disabled |
| Remove Compose images on cleanup | `--cleanup-images none\|local\|all` | `-CleanupImages none\|local\|all` | `none` |
| Compose file | `--compose-file` | `-ComposeFile` | `docker/docker-compose.yml` |
| Report directory | `--report-dir` | `-ReportDir` | `reports` |

## Reports

Each completed run writes a Markdown report:

```text
reports/benchmark-YYYYMMDDHHMMSS.md
```

The report includes the benchmark settings, producer output, CoreCLR/JIT consumer output, Native AOT consumer output, and the exact consumer commands used.

## Manual Docker Cleanup

If you ran Docker Compose yourself, clean up the benchmark stack with:

```bash
docker compose -f docker/docker-compose.yml down --volumes --remove-orphans --rmi all
```

PowerShell uses the same Docker command:

```powershell
docker compose -f docker/docker-compose.yml down --volumes --remove-orphans --rmi all
```

Use `--rmi local` instead of `--rmi all` to keep pulled images cached.
