# Study and implementation notes

Time spent: under two hours on the assessment, plus approximately 30 minutes on
an optional visualisation. AI assisted the study, design, implementation and
verification.

## Study first

We read the starter code, traced a run from API to driver and ran the baseline
checks before implementing the scheduler. The stub changed run status but sent no
commands: the default workflow stayed at one ready task and six waiting tasks.

| Read | Finding |
| --- | --- |
| [Workflow and loader](config/workflows.yaml), [validation](services/executor/workflows.py) | A DAG with run-local dependencies and fixed device assignments. |
| [API](services/executor/api.py), [original scheduler](https://github.com/dance-cmdr/lab-workflow/blob/3e19f22/services/executor/scheduler.py) | Two scheduling triggers: start and result. Dispatch was missing. |
| [Store](services/executor/store.py), [schema](db/init.sql) | Postgres owns task/run states, counts and timestamps. |
| [Bus](services/executor/bus.py), [driver](services/worker/main.py) | Acceptance is separate from completion. Results arrive concurrently; busy devices refuse. |
| [Acceptance checks](scripts/acceptance.sh), [concurrency test](tests/test_scheduler_concurrency.py) | Check completion, dependencies, overlap, duplicates and simultaneous results. |

![Workflow before implementation: one ready task, six waiting](docs/design/starter-workflow-observation.png)

*Historical observer snapshot. “Stage” means dependency depth, not scheduled
time. The optional [observer](tools/queue-viewer/README.md) is included separately
from the executor.*

## Decomposition and decisions

**Start → scan dependencies → claim task and instrument → dispatch → record
result → release instrument → rescan or finish.**

After `fill_sample_plate`, incubation and both fill tasks become eligible.
Incubation can overlap filling; the fills share one liquid handler. `combine`
waits for incubation, buffer filling and reagent warming.

![Early scheduler decomposition in Excalidraw](docs/design/initial-scheduler-sketch.png)

*Early proposal: counters were replaced by scanning. Bounded retries, skipped
states and attempt history are now implemented.*

- **Scan rather than maintain counters:** fewer consistency rules; O(V + E) per
  run scanned. Each event scans all tracked active runs.
- **One lock across runs:** prevents simultaneous results claiming the same task
  or instrument. It covers database changes and acknowledgements, not physical
  execution. Slow acknowledgements delay scheduling decisions.
- **Claim before sending:** reserve the task and instrument. Only completion
  unlocks dependants. Duplicate or mismatched results are ignored.
- **Refusal is not failure:** undo the claim and rescan after 250 ms, even if no
  other result arrives. This resends declined work, not failed physical work.
- **One safe retry:** honour the driver's `retryable` flag, release equipment and
  wait three seconds. A second failure, or a non-retryable failure, ends the run.
- **Failure is explicit:** record the causal step, mark unstarted tasks `skipped`,
  and retain late results without reviving the run. Other runs keep progressing.
- **Preserve attempts:** each command offer has an ID echoed by the driver.
  Append-only events retain outcomes, errors and refusals; state/history changes
  share a transaction. Refusals do not consume the failure-retry budget. Stale
  results cannot complete a newer attempt. Unversioned results are accepted only
  for the first offer, for compatibility with the supplied concurrency test.
- **Unknown acknowledgement is not refusal:** fail the run, retain the diagnostic
  and instrument reservation, and never blindly resend. Release on an actual
  result. Driver-side deduplication would be needed for safe command replay.

## Evidence

- Both supplied workflows: **5/5 acceptance checks passed**.
- Pytest: **12 passed**, covering concurrency, refusals, shared equipment, bounded
  retries, stale results, skipped work, late results and driver attempt-ID echoing.
- Four concurrent live runs completed all 26 tasks after four injected transient
  failures and four successful retries, using a separate demonstration driver.
- Live failure check and lint passed; drivers restored to normal.
- Pytest now uses separate PostgreSQL storage. Live acceptance runs use the real
  executor and drivers. Driver acceptance history resets on restart.

## Limits and next step

One executor process; no automatic restart recovery, distributed coordination or
atomic database/message delivery. A crash between claiming and sending work
needs reconciliation; attempt IDs do not provide driver-side deduplication.

If a completion result is lost, the current scheduler waits indefinitely and
retains the instrument reservation. Production recovery would reconcile
instrument and experiment state before resuming; an unresolved outcome would
require abandoning the run and confirming a safe reset.

Start order and alphabetical task order are deterministic, not fair or optimal.
The seven-step workflow completed in approximately 12 seconds.

## Exercise feedback

Personal feedback to be added before submission.
