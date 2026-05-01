# Broker Benchmark Report

## Hardware
| Property | Value |
|---|---|
| OS | linux |
| Logical CPU cores | 16 |
| Total RAM | 31 GB |
| Container | yes (Docker) |

## Test Setup
| Parameter | Value |
|---|---|
| Broker | Redpanda @ redpanda:29092 |
| Run mode | combined |
| GBPS target | 10.0 |
| Duration | 60s |
| Partitions | 16 |
| Producer tasks | 16 |
| Consumer tasks | 16 |
| Payload type | float32 correlated (LZ4-compressible) |
| Compression | LZ4 |
| Run ID | 1776211761 |

## Results
| Message Size | Target Gbps | Achieved Gbps | Efficiency | Messages | Lost | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|
| 24b | 2.50 | 0.000 | 0% | 3713 | 0 | 958.0 | 1529.8 | 3306.0 | 10215.9 |
| 256b | 2.50 | 0.000 | 0% | 4208 | 0 | 942.5 | 1269.4 | 3312.0 | 3317.8 |
| 4kb | 2.50 | 0.002 | 0% | 4318 | 0 | 942.9 | 1275.5 | 3317.9 | 3331.8 |
| 64kb | 2.50 | 0.037 | 1% | 4252 | 0 | 960.2 | 1443.8 | 3340.3 | 3393.8 |
| 512kb | 2.50 | 0.176 | 7% | 2516 | 0 | 1000.4 | 3027.5 | 6527.7 | 34990.1 |
| 4mb | 2.50 | 0.962 | 38% | 1721 | 0 | 1406.0 | 4689.9 | 10274.8 | 32743.4 |
| 16mb | 2.50 | 0.960 | 38% | 429 | 0 | 2620.9 | 17191.9 | 25736.2 | 26507.3 |
| 80mb | 2.50 | 1.163 | 46% | 104 | 0 | 7940.9 | 34365.4 | 47073.3 | 50679.8 |

## Summary
| Metric | Value |
|---|---|
| Target Gbps | 10.0 |
| Achieved Gbps | **3.301** |
| Efficiency | **33%** |
| Messages received | 21261 |
| Messages lost | **0** |
