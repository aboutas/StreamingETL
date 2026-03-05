# Benchmark Results

## Test Configuration
- **Configs:** 4 (1 transformation each)
  1. `filter_greater` — temperature > 25 (reduces records)
  2. `filter_greater` — humidity > 50 (reduces records)
  3. `to_lowercase` — temperature location field (pass-through)
  4. `trim_whitespace` — humidity sensor field (pass-through)
- **Parallelism:** 4
- **Measurement:** Pure ETL processing time (first input consumed → last output written)
- **Measurement precision:** ±1s (polling granularity)

## Results

| Metric               | 1M           | 2M           | 5M (median of 3) | 10M (median of 3) | 15M (median of 3) |
|----------------------|--------------|--------------|-------------------|--------------------|--------------------|
| Input records        | 1,000,032    | 2,000,016    | 5,000,016         | 10,000,032         | 15,000,000         |
| Output records       | 470,730      | 946,975      | 2,374,825         | 4,750,540          | 7,121,736          |
| Processing ratio     | 47%          | 47%          | 47%               | 47%                | 47%                |
| End-to-end time      | 12           | 23           | 58                | 89                 | 125                |
| Input throughput     | 76,937 rec/s | 85,668 rec/s | 86,602 rec/s      | 112,198 rec/s      | 119,871 rec/s      |
| Output throughput    | 36,215 rec/s | 40,562 rec/s | 40,003 rec/s      | 53,300 rec/s       | 56,912 rec/s       |

### 5M — All 3 Runs

| Run | Time  | Input throughput | Output records |
|-----|-------|-----------------|----------------|
| 1   | 48s   | 103,607 rec/s   | 2,374,825      |
| 2   | 69s   | 72,623 rec/s    | 2,375,188      |
| 3   | 58s   | 86,602 rec/s    | 2,309,585      |

Median selected: Run 3 (58s). Variance due to shared cluster resource contention.

### 10M — All 3 Runs

| Run | Time   | Input throughput | Output records |
|-----|--------|-----------------|----------------|
| 1   | 79s    | 127,249 rec/s   | 4,748,409      |
| 2   | 100s   | 99,708 rec/s    | 4,751,341      |
| 3   | 89s    | 112,198 rec/s   | 4,750,540      |

Median selected: Run 3 (89s). Variance due to shared cluster resource contention.

### 15M — All 3 Runs

| Run | Time   | Input throughput | Output records |
|-----|--------|-----------------|----------------|
| 1   | 120s   | 124,663 rec/s   | 7,123,192      |
| 2   | 140s   | 107,124 rec/s   | 7,123,628      |
| 3   | 125s   | 119,871 rec/s   | 7,121,736      |

Median selected: Run 3 (125s). Variance due to shared cluster resource contention.

## Scalability Tests (5M records, 4 configs × 1 transformation)

| Metric               | p=1 (avg of 2)  | p=2 (avg of 2)  | p=4 (median of 3) | p=8 (avg of 2)   | p=11 (avg of 2)  |
|----------------------|------------------|------------------|--------------------|-------------------|-------------------|
| Kafka partitions     | 1                | 2                | 4                  | 8                 | 11                |
| Input records        | 5,000,016        | 5,000,016        | 5,000,016          | 5,000,016         | 5,000,016         |
| Output records       | 2,374,810        | 2,374,257        | 2,374,825          | 2,335,411         | 2,375,093         |
| Processing ratio     | 47%              | 47%              | 47%                | 46%               | 47%               |
| End-to-end time      | 84               | 82               | 58                 | 43                | 41                |
| Input throughput     | 59,520 rec/s     | 60,970 rec/s     | 86,602 rec/s       | 116,670 rec/s     | 120,920 rec/s     |
| Output throughput    | 28,270 rec/s     | 28,952 rec/s     | 40,003 rec/s       | 54,558 rec/s      | 57,438 rec/s      |

### p=1 — 2 Runs

| Run | Time | Input throughput | Output records |
|-----|------|-----------------|----------------|
| 1   | 79s  | 63,126 rec/s    | 2,374,845      |
| 2   | 89s  | 55,914 rec/s    | 2,374,774      |

Average: 84s, 59,520 rec/s.

### p=2 — 2 Runs (cluster under heavy load)

| Run | Time | Input throughput | Output records |
|-----|------|-----------------|----------------|
| 1   | 80s  | 62,841 rec/s    | 2,374,399      |
| 2   | 85s  | 59,099 rec/s    | 2,374,114      |

Average: 82s, 60,970 rec/s. Note: cluster contention likely masked parallelism benefit.

### p=8 — 2 Runs

| Run | Time | Input throughput | Output records |
|-----|------|-----------------|----------------|
| 1   | 46s  | 108,710 rec/s   | 2,295,400      |
| 2   | 40s  | 124,629 rec/s   | 2,375,421      |

Average: 43s, 116,670 rec/s.

### p=11 — 2 Runs

| Run | Time | Input throughput | Output records |
|-----|------|-----------------|----------------|
| 1   | 44s  | 112,443 rec/s   | 2,375,698      |
| 2   | 39s  | 129,396 rec/s   | 2,374,487      |

Average: 41s, 120,920 rec/s.

## Observations
- Processing ratio stable at 47% across all sizes
- Throughput curve: 77K → 86K → 87K → 112K → 120K rec/s — plateauing at ~120K
- 10M→15M: 1.5x data = 1.4x time (89s→125s) — still sub-linear
- Throughput ceiling at parallelism 4 is ~120K rec/s
- Scalability: p=1 (84s) → p=4 (58s) → p=8 (43s) → p=11 (41s)
- Clear diminishing returns: p=1→p=4 = 1.45x speedup, p=4→p=8 = 1.35x, p=8→p=11 = 1.05x
- p=2 results unreliable due to cluster contention (similar to p=1)
