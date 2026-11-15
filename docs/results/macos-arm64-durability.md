# sluice experiments

- darwin/arm64, 8 CPUs, go1.27.1
- run at 2026-11-15 00:26 UTC


## E1: durability policies

1 KiB records, acks=1, one partition, each producer has its own connection and one record in flight; 3s per row.

| fsync policy | producers | records/s | MiB/s | p50 | p99 | appends per fsync |
|---|---:|---:|---:|---:|---:|---:|
| none | 1 | 22k | 21.2 | 0.04 ms | 0.12 ms | only at segment roll |
| none | 16 | 50k | 49.3 | 0.32 ms | 1.01 ms | only at segment roll |
| none | 64 | 55k | 53.9 | 1.17 ms | 3.42 ms | only at segment roll |
| interval | 1 | 21k | 20.2 | 0.04 ms | 0.14 ms | 1998.4 |
| interval | 16 | 49k | 47.9 | 0.32 ms | 0.98 ms | 4602.6 |
| interval | 64 | 52k | 50.6 | 1.17 ms | 7.23 ms | 4859.9 |
| always | 1 | 261 | 0.3 | 3.98 ms | 5.20 ms | 1.0 |
| always | 16 | 273 | 0.3 | 59.1 ms | 71.9 ms | 1.0 |
| always | 64 | 289 | 0.3 | 238 ms | 251 ms | 1.0 |
| group | 1 | 242 | 0.2 | 4.04 ms | 6.00 ms | 1.0 |
| group | 16 | 1720 | 1.7 | 8.88 ms | 19.5 ms | 5.4 |
| group | 64 | 3188 | 3.1 | 18.0 ms | 53.3 ms | 10.2 |
