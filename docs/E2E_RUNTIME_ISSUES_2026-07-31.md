# E2E Runtime Issues - 2026-07-31

## Scope

- Environment: Docker Compose stack in `/home/phucle/Desktop/fps-anticheat`.
- Real data source: MinIO bucket `fps-anticheat-datalake`.
- Dataset manifest: `manifests/dataset-manifest.json`.
- Selected source file: `raw-sources/skihikingkevin-pubg-match-deaths/kill_match_stats_final_0.csv`.
- Final E2E raw topic: `pubg.v1.kill-event.raw.e2e.20260731T051607Z`.
- Final E2E consumer group: `rust-processor-e2e-20260731T051607Z`.

## Final E2E Result

Status: PASS.

Replay produced 120 real records into Kafka. Rust processor consumed, wrote Bronze, Silver, Gold artifacts to MinIO, published `data.dataset.gold.ready`, and committed Kafka offsets only after durable artifacts were written.

Evidence:

- Kafka consumer group: `CURRENT-OFFSET=120`, `LOG-END-OFFSET=120`, `LAG=0`.
- Final processor batches: `50`, `50`, `20`.
- Final batch ID: `2521d9e2a67916827c62ad9136b71c0899d2155b2283c3ec9f3de7fefeb21a03`.
- Final Gold artifact: `gold/player-match-features/features_2521d9e2a67916827c62ad9136b71c0899d2155b2283c3ec9f3de7fefeb21a03.parquet`.
- Final manifest: `manifests/year=2026/month=07/day=31/manifest_2521d9e2a67916827c62ad9136b71c0899d2155b2283c3ec9f3de7fefeb21a03.json`.
- Kafka `gold.ready` event was present for the final batch with checksum `fbda12301be383588cc22e776355441335c093746b3c2ec4457261af0dc84d0b`.
- Error scan for the final processor run found no `ERROR`, `WARN`, `thất bại`, or `panic` lines.

## Fixed In This Changeset

1. Rust processor partial batch could stall forever.
   - Symptom: first E2E run produced 120 messages, but consumer stayed at `CURRENT-OFFSET=101`, `LOG-END-OFFSET=120`, `LAG=19`.
   - Cause: the timer check happened after `select!`; when Kafka was quiet, `recv().await` did not wake the loop, so the final partial batch did not flush.
   - Fix: move flush ticker into `tokio::select!` and flush only when the accumulator timer is actually due.

2. Rust accumulator timer started too early after idle.
   - Symptom: after the first timer fix, E2E passed but batches were split as `1`, `50`, `1`, `50`, `18`.
   - Cause: batch age was measured from app startup/last flush, not from the first pending record after an idle period.
   - Fix: reset the timer when the accumulator transitions from empty to pending. Added a unit test for this invariant.

3. R Gold Engine crashed on real data with all missing kill-distance coordinates.
   - Symptom: R worker failed with `no rows to aggregate` after Bronze/manifest were already durable, creating a retry loop before offset commit.
   - Cause: base R `aggregate(... ~ ..., data=...)` defaulted to `na.omit`, dropping all rows when the distance column was all `NA`.
   - Fix: use `na.action = na.pass`, return `NA_real_` for all-NA distance groups, and create an empty aggregate frame for no-killer batches.

4. R Silver Preprocessor could drop groups with all-NA optional fields.
   - Cause: similar `aggregate` defaults around kill time, weapon, and placement fields.
   - Fix: use `na.action = na.pass` for optional-field aggregates.

5. Go replay producer log reported the wrong compression.
   - Symptom: source used Sarama `compress.Gzip`, but startup log said `compression=zstd`.
   - Fix: log `compression=gzip`.
   - Note: the currently running replay Docker image still logs `zstd` until the image is rebuilt.

## Findings Still To Clean Up

1. Compose `pubg-rust-processor` service was running a stale image.
   - Observed failure: container restart loop with `Unsupported value "zstd" for configuration property "compression.codec": libzstd not available at build time`.
   - Source code already uses Kafka producer `compression.type=none`, so the running image was stale.
   - Required cleanup: rebuild/recreate the compose service image after these commits.

2. Full `docker compose build rust-processor` is too slow.
   - Observed behavior: runtime stage installs R `arrow` and dependencies from source, despite comments suggesting a fast binary install path.
   - Risk: slow or flaky rebuilds; poor CI feedback loop.
   - Suggested cleanup: pre-bake an R runtime base image with `arrow`, or use a reliable binary package repository for the target image.

3. Docker buildx plugin symlink was broken on the host.
   - Observed failure: `fork/exec /home/phucle/.docker/cli-plugins/docker-buildx: no such file or directory`.
   - Local repair performed: repointed the symlink to `/usr/libexec/docker/cli-plugins/docker-buildx`.
   - This was an environment repair, not a repository change.

4. Host-side Kafka producer cannot use the compose Kafka listener directly.
   - Observed failure when running host `go run`: DNS lookup for `kafka` failed.
   - Cause: Kafka advertises `kafka:9092`, which is resolvable inside Docker network, not from host.
   - Suggested cleanup: either run producers in the Docker network or add a proper host advertised listener.

5. Compose project ownership is inconsistent for existing network/volumes.
   - Observed warnings: `pubg-platform-net`, `pubg_kafka_data`, and `pubg_minio_data` were created under project `compose`, while current project is `fps-anticheat`.
   - Suggested cleanup: mark these resources as `external: true` or recreate them under one consistent compose project name.

6. Rust processor still emits unused/dead-code warnings.
   - Tests pass, but `cargo test` and builder-stage compile report 12 warnings.
   - Suggested cleanup: remove stale exports/fields or intentionally allow them with comments where they are future API surface.

## Verification Commands Run

- `docker compose up -d`
- `go test ./...` from `apps/go-ingestor`
- `cargo test --manifest-path apps/rust-processor/Cargo.toml`
- R smoke test for all-NA kill distance aggregate from `apps/rust-processor/r-processor`
- Builder-stage Docker build: `DOCKER_BUILDKIT=1 docker buildx build --target builder --load -t fps-anticheat-rust-processor-builder ./apps/rust-processor`
- Final E2E replay: 120 real records from MinIO-backed dataset into Kafka topic `pubg.v1.kill-event.raw.e2e.20260731T051607Z`
- Final MinIO object checks for Gold artifact and manifest
- Final Kafka `gold.ready` event check for batch `2521d9e2a67916827c62ad9136b71c0899d2155b2283c3ec9f3de7fefeb21a03`
