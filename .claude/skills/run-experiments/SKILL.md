---
name: run-experiments
description: Run sluice's four experiments (durability, queues, replication, zero-copy) and refresh the README tables from their output. Use when asked to benchmark, re-measure, or update numbers.
---

# Run the experiments and publish the numbers honestly

1. Make sure the tree is clean and tests pass: `make race`.
2. Run on Linux, which is what the README reports:
   `make docker && docker run --rm -v sluice-bench:/data --entrypoint /sluice-bench sluice:dev experiment all -dir /data/bench > docs/results/docker-linux-arm64.md`
   For a native macOS run of one experiment: `go run ./cmd/sluice-bench experiment durability > docs/results/macos-durability.md`.
3. Copy tables into README.md **verbatim** from the result files. Keep the environment lines (OS, CPUs,
   kernel, Docker) next to them.
4. Never edit, round or cherry-pick a number. If a run looks wrong (noisy neighbour, laptop on battery),
   rerun the whole experiment and replace the file; say in the commit message why.
5. If a result changes the story (e.g. the gap between ISR and quorum shrinks), update the prose that
   interprets it in the same commit.
6. Commit the result files with the README so every number has its source in git.
