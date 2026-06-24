# Small-file problem: baseline vs. tuned

## Findings before tuning
- **[MEDIUM] small_files**: Stage 1: 150 tasks averaging 17.1 KB of input each. Per-task overhead (scheduling, file open/footer reads) is likely dominating actual compute time -- classic small-file problem.

## Findings after tuning
- none detected

## Metric comparison
| metric | baseline | tuned | change |
|---|---|---|---|
| tasks | 152 | 12 | ↓92% |
| shuffle read | 38.5KB | 2.6KB | ↓93% |
| shuffle write | 38.5KB | 2.6KB | ↓93% |
| spill | 0.0B | 0.0B | n/a |
| executor run time (sum across tasks) | 3243 | 2357 | ↓27% |
| longest single task (straggler) | 423 | 488 | ↑15% |

## What fixed it
- **Coalesce small input partitions after read**: coalesce(N) immediately after read merges narrow partitions without a shuffle (it only reduces partition count, unlike repartition()). Target N using the standard rule of thumb: total data size / 128MB. If this dataset is written by an upstream job you control, fix it at the source too -- writing with .coalesce(N) before .write() prevents every downstream consumer from hitting the same problem.