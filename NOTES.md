# Implementation notes

## Summary

Built a scheduler that shares instruments safely, runs independent tasks
concurrently, and handles failures with bounded retries and durable attempt history.

**Time:** A little under two hours, plus approximately 30 minutes for the visualiser.
AI assisted study, design, implementation and verification.

## Mental model

Each experiment is a dependency graph; all experiments share three instruments,
each with one execution slot. A command being accepted is not completion. Only a
successful result unlocks dependent tasks.

I traced the existing code first. The baseline sent no commands: one task was
ready and six were waiting.

<details>
<summary>Baseline observation</summary>

![Baseline: one ready task, six waiting](docs/design/starter-workflow-observation.png)

Columns represent dependency depth, not scheduled time.

</details>

<details>
<summary>Final workflow observation — clean live run</summary>

![Final workflow: all seven tasks completed](docs/design/final-workflow-observation.jpg)

Same workflow after implementation: seven tasks completed once each in 12 seconds,
with dependencies respected and independent work overlapping. All five acceptance
checks passed; errors and dropped results were disabled.

</details>

![Implemented scheduler: dependency scanning, instrument claims and result handling](docs/design/scheduler-architecture.png)

Incubation can overlap liquid handling; fill tasks take turns. Combining waits
for all three prerequisite branches.

## Components, decisions and tradeoffs

| Component | Choice over alternative | What we gain | What we pay |
| --- | --- | --- | --- |
| Dependencies | Scan tasks instead of maintaining Kahn’s counters. | Less derived state to keep consistent. | Recheck every active graph after events instead of updating only the completed task’s dependants; more CPU work and database reads as runs grow. |
| Coordination | Keep one lock through acknowledgement instead of releasing it after reservation. | Fewer interleavings to reason about during dispatch. | A slow acknowledgement blocks scheduling decisions for every run, even on unrelated instruments. |
| Busy handling | Poll after refusal instead of relying only on completion events. | Refused work progresses even if no further result arrives. | Extra scans while busy and up to 250 ms before noticing availability, plus scheduling time. |
| Retries | Allow one safe retry instead of failing immediately. | Recover from transient errors. | More execution time and instrument capacity spent repeating work; a permanent fault takes longer to report as final. |
| Final failure | Stop the run instead of continuing independent branches. | Avoid spending resources on an experiment that cannot complete. | Give up potentially useful partial results from branches that could still succeed. |
| History | Append attempt events instead of keeping only the latest outcome. | Explain failures and retries after state changes. | More database writes, storage and event-handling logic. |

For V tasks and E dependencies, scanning costs O(V + E) **each time**. Kahn’s
counters do O(V + E) dependency bookkeeping **across one successful traversal**;
that does not include instrument scheduling or database I/O. Simplicity here costs
repeated work, rather than offering the same efficiency.

## Operational boundaries

An acknowledgement timeout fails the run but retains the instrument reservation
until a result arrives: acceptance is unknown. A missing completion result leaves
the task in flight indefinitely with its reservation. Neither case triggers blind
replay; an unknown physical outcome needs confirmation before recovery.

The main weakness is a process crash between database claim and command delivery.
The scheduler operates in one process and does not resume stored runs on restart;
those cases need reconciliation. Attempt IDs correlate results, not deduplicate
commands at the driver.

## Verification

- Both workflows: **5/5 acceptance checks**; **12 pytest tests**, persistent-failure
  check and lint passed. Pytest uses a separate database.
- Four concurrent runs: **26 tasks completed**, with four injected transient
  failures and four successful retries.

## Exercise feedback

- I really enjoyed the realistic lab workflow; the problem statement was clear.
- As a visual thinker, the GitHub Actions-inspired visualiser made the flow click
  and was worth the time.
- Docker and real PostgreSQL removed uncertainty about infrastructure scope.
- My only suggested change: include a basic visualisation in the starter pack.
