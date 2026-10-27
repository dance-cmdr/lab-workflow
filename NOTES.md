# Implementation notes

## Executive summary

Built a scheduler that shares instruments safely, runs independent tasks
concurrently, and handles failures with bounded retries and durable attempt history.

**Time:** under two hours, plus approximately 30 minutes for the visualiser.
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

![Implemented scheduler: dependency scanning, instrument claims and result handling](docs/design/scheduler-architecture.png)

Incubation can overlap liquid handling; fill tasks take turns. Combining waits
for all three prerequisite branches.

## Components, decisions and tradeoffs

| Component | Decision and reason | Tradeoff |
| --- | --- | --- |
| Dependency scan | Scan pending tasks against completed prerequisites; fewer consistency rules than counters. | O(V + E) per active run on each scan; ordering is simple, not fair or optimal. |
| Dispatch coordination | One lock serialises result handling and task/instrument claims across runs. Physical execution remains concurrent. | Slow database operations or acknowledgements delay scheduling decisions. |
| Busy handling | Undo a refused claim and rescan after 250 ms, even without another result. | Repeated polling while busy; refusals do not consume failure retries. |
| Failure handling | Honour `retryable`: release equipment, wait three seconds, retry once. This delivers one bounded extension. | Non-retryable or exhausted failures stop the whole run, including independent unstarted branches. |
| State and history | Atomically record outcomes, the causal failure and skipped work; preserve late results without reviving failed runs. Attempt IDs reject stale results. | Unversioned results are accepted only on the first offer for supplied-test compatibility. |

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
