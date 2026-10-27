# Working notes — basic scheduler checkpoint

This is a development draft, not the final submission note. Total time spent on
setup, design, implementation and the optional visualisation still needs to be
reconciled before submission. The expected extension and final reflection remain
unfinished.

## Current implementation

The scheduler scans persisted steps after each start or result. It builds a set
of completed names and dispatches pending steps only when every dependency is in
that set. Commands are acknowledged separately from completion: an accepted
command never unlocks dependants. All completed steps finish the run.

One asyncio lock protects scheduling decisions across every run started through
this scheduler instance. It covers state changes, instrument reservations and
command acknowledgements. Driver operations themselves run outside the lock.
Simultaneous result handlers therefore see each other's committed changes rather
than both dispatching the same pending step. Duplicate or mismatched results are
ignored. Reservations identify the owning step, so an old result cannot release
another step's instrument.

An explicit refusal reverses the provisional dispatch claim, leaves the step
pending and arranges another scan after 250 ms. This wakeup matters when the busy
instrument belongs to external work whose result will never reach this executor.
These are retries of refused commands, not retries of failed physical work.

A reported failure is recorded on the step and ends its run as failed. No new
work starts for that run. Already dispatched steps can finish and their results
are recorded; a late success cannot revive the run. Other tracked runs continue
when equipment becomes available. Unstarted steps remain pending at this basic
checkpoint; identifying them explicitly as skipped belongs to the extension.

## Boundaries and likely weaknesses

A scan costs O(V + E) for a run's tasks and dependency links; each event scans all
tracked active runs. Database calls and acknowledgements are serial under the
lock. Run start order and step-name order provide simple deterministic priority,
not fairness or an optimal schedule. A slow acknowledgement temporarily delays
other scheduling decisions, though ongoing instruments keep working.

This assumes one executor process and uninterrupted ownership. There is no
startup recovery, distributed locking, transactional outbox or end-to-end
exactly-once guarantee across crashes. A crash between claiming and sending, or
between recording a failed step and failing its run, needs reconciliation.

A command timeout has an unknown outcome. The run fails, the diagnostic remains
on the dispatched step, and the instrument reservation stays held until a result
arrives. The command is never blindly repeated. A dropped result leaves a run
waiting and the instrument reserved indefinitely. Neither situation is solved by
assuming the instrument stopped. Production recovery would reconcile driver
state and command identity before deciding whether work can safely continue.

## Next stage

Add bounded safe retries and append-only attempt history, with state and history
written atomically where possible. Complete the required final note, actual time
accounting and feedback on the exercise. The optional visualisation remains
outside the assessment source and is not part of this implementation.

---

## Design-study appendix: understanding the starter code

These snapshots and discussion notes document the exploration before scheduler
implementation. They supplement the draft answers above; they are not evidence
that every feature drawn in a sketch has been implemented. AI assistance was used
to inspect the starter pack, discuss tradeoffs, build a separate local observer,
and implement and verify the chosen design.

### Trace the existing system before changing it

We read the brief and followed the execution path through the supplied modules:

| Component inspected | What we needed to establish |
| --- | --- |
| [Workflow definitions](config/workflows.yaml) and [loader](services/executor/workflows.py) | Tasks form a DAG, not a tree; dependencies are run-local and device assignments are fixed. |
| [API](services/executor/api.py) and [scheduler stub](https://github.com/dance-cmdr/lab-workflow/blob/3e19f22/services/executor/scheduler.py) | Starting a run changed its status but dispatched nothing. The scheduler's entry points were start and result handling. |
| [Store](services/executor/store.py) and [schema](db/init.sql) | Postgres owns current run/step state; dispatch counts and timestamps already exist. |
| [Bus](services/executor/bus.py) | An immediate acknowledgement is separate from a later completion result. Result handlers run concurrently. |
| [Driver](services/worker/main.py) | Each device accepts one operation, refuses while busy, and can report failures or drop results. Acceptance history is not completion history. |
| [Acceptance checks](scripts/acceptance.sh) and [concurrency test](tests/test_scheduler_concurrency.py) | Verify dependency order, actual overlap, unique execution, and simultaneous-result races. |

We brought up the supplied stack and ran the baseline checks before implementing
the scheduler. The stub did not dispatch work. A separate read-only observer made
that state visible: one ready task, six waiting tasks, and no commands sent.

![Pre-implementation observation of the seven-step workflow](docs/design/starter-workflow-observation.png)

*Historical snapshot from the local study tool, not the submitted application.
The columns labelled “stage” represent dependency depth, not scheduled start
times. The observer's code remains outside this repository.*

### Decompose the scheduling problem

Our first mental model was a task queue feeding three instrument lanes. We refined
that into five responsibilities:

1. Start a run and load its recorded task states.
2. Find pending tasks whose prerequisites have completed.
3. Claim a task and its free instrument, then send the command.
4. Record the reported outcome and release that instrument.
5. Rescan for newly eligible work, or finish the run.

In PCR Amplification, completing `fill_sample_plate` unlocks incubation and two
fill operations. Incubation can overlap filling, but the fills share the liquid
handler. `combine` must wait for incubation, buffer filling and reagent warming.
This separated dependency readiness from equipment availability.

### Record alternatives and narrow the design

![Initial scheduler decomposition drawn in Excalidraw](docs/design/initial-scheduler-sketch.png)

*Early proposal, not an as-built architecture diagram. “Ready queue” describes
eligible work conceptually; the current implementation derives it by scanning.*

The sketch initially proposed Kahn-style dependency counters. We chose scans of
stored task states instead: less bookkeeping for duplicate results and restarts,
at the cost of checking the graph after each event. We discussed the O(V + E)
scan cost explicitly rather than treating the seven-task example as the only
possible workload.

We also identified a race: two completions could both discover `combine` ready.
The implementation uses one shared scheduler lock and claims work before sending.
Instrument reservations span runs because equipment is shared, not owned by an
individual experiment.

The sketch's bounded safe retries, explicit skipping and append-only attempt
history remain extension ideas. The basic scheduler currently records failures,
stops dispatching that run and accepts late results. We distinguished an explicit
busy refusal, which is safe to reschedule, from an acknowledgement timeout, whose
physical outcome is unknown. Command identifiers alone cannot make retries safe
unless the driver also deduplicates them.
