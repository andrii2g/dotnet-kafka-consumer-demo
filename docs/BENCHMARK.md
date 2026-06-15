# Consumer Benchmark

Quick run:

```bash
docker compose -f docker/docker-compose.yml up -d
bash scripts/benchmark-consumers.sh --count 100000
```

Windows PowerShell:

```powershell
docker compose -f docker/docker-compose.yml up -d
.\scripts\benchmark-consumers.ps1 -Count 100000
```

The benchmark scripts:

- restore and build the .NET 10 solution
- publish the AOT producer and AOT consumer
- create a fresh benchmark topic
- produce the requested number of messages
- consume the same topic once with CoreCLR/JIT
- consume the same topic once with Native AOT
- print elapsed time and messages per second for both consumers

## Bash

Default run:

```bash
bash scripts/benchmark-consumers.sh --count 100000
```

With common options:

```bash
bash scripts/benchmark-consumers.sh \
  --bootstrap localhost:9092 \
  --count 100000 \
  --commit-batch 1000 \
  --poll-timeout-ms 100 \
  --rid linux-x64
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

With common options:

```powershell
.\scripts\benchmark-consumers.ps1 `
  -BootstrapServers localhost:9092 `
  -Count 100000 `
  -CommitBatch 1000 `
  -PollTimeoutMs 100 `
  -RuntimeIdentifier win-x64
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
