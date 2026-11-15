# sluice experiments

- linux/arm64, 4 CPUs, go1.27.4
- Linux kernel 7.0.14-linuxkit
- inside a Docker container
- run at 2026-11-15 00:24 UTC


## E1: durability policies

1 KiB records, acks=1, one partition, each producer has its own connection and one record in flight; 3s per row.

| fsync policy | producers | records/s | MiB/s | p50 | p99 | appends per fsync |
|---|---:|---:|---:|---:|---:|---:|
| none | 1 | 35k | 34.0 | 0.02 ms | 0.08 ms | only at segment roll |
| none | 16 | 129k | 126.0 | 0.05 ms | 0.55 ms | only at segment roll |
| none | 64 | 126k | 122.8 | 0.06 ms | 2.04 ms | only at segment roll |
| interval | 1 | 32k | 31.4 | 0.02 ms | 0.10 ms | 3212.8 |
| interval | 16 | 125k | 122.4 | 0.06 ms | 0.61 ms | 10440.7 |
| interval | 64 | 133k | 130.2 | 0.07 ms | 2.13 ms | 10810.2 |
| always | 1 | 2271 | 2.2 | 0.43 ms | 0.78 ms | 1.0 |
| always | 16 | 2513 | 2.5 | 6.20 ms | 9.87 ms | 1.0 |
| always | 64 | 3036 | 3.0 | 19.0 ms | 36.2 ms | 1.0 |
| group | 1 | 2909 | 2.8 | 0.33 ms | 0.61 ms | 1.0 |
| group | 16 | 21k | 20.6 | 0.74 ms | 1.21 ms | 8.0 |
| group | 64 | 56k | 54.5 | 1.07 ms | 3.22 ms | 31.8 |

## E2: consumer groups vs share groups

A topic with 4 partitions gets 1000 records/s for 5s. Each consumer handles one record at a time: 1ms of work, except every 100th record, which takes 200ms. Latency is from produce to the end of processing, for the fast records only: it shows how long they wait behind slow ones.

| model | consumers | drained in | fast p50 | fast p99 | fast max | records out of partition order |
|---|---:|---:|---:|---:|---:|---:|
| consumer group | 4 | 12.9s | 3.00 ms | 7504 ms | 7778 ms | 0 |
| consumer group | 8 | 13s | 3.18 ms | 7569 ms | 7840 ms | 0 |
| share group | 4 | 5.2s | 3.72 ms | 203 ms | 207 ms | 330 |
| share group | 8 | 5.2s | 2.37 ms | 192 ms | 205 ms | 204 |
| share group | 16 | 5.2s | 2.28 ms | 158 ms | 203 ms | 162 |

## E3: commit rule with a slow follower

Three brokers, replication factor 3, fsync=group, 1 KiB records, 8 producers with one record in flight each; the slow follower sleeps 20ms before every fetch; 3s per row.

| commit rule | followers | acks | records/s | p50 | p99 |
|---|---|---|---:|---:|---:|
| isr | healthy | 1 | 5358 | 1.29 ms | 5.22 ms |
| isr | healthy | all | 4849 | 1.42 ms | 5.74 ms |
| quorum | healthy | all | 5656 | 1.25 ms | 4.70 ms |
| isr | one slow | all | 149 | 54.7 ms | 66.8 ms |
| quorum | one slow | all | 5854 | 1.15 ms | 3.91 ms |

## E4: zero-copy fetch

One partition holding 512 MiB of 1 KiB records, read end to end by a consumer fetching 1 MiB at a time (records not decoded); best of 3 passes over a warm page cache. CPU is the whole process (broker and consumer).

| fetch path | GiB/s | CPU seconds per GiB |
|---|---:|---:|
| sendfile(2) | 3.90 | 0.46 |
| read + write | 3.46 | 0.51 |
