# Study and implementation notes

Time spent: under two hours on the assessment, plus approximately 30 minutes on
the optional visualisation. AI assisted the study, design, implementation and
verification.

## Study first

I traced the API, store, scheduler, bus and drivers first. Acceptance is separate from completion; dependencies are
run-local, while instruments are shared. Baseline checks confirmed the stub sent
no commands: one task was ready and six were waiting.

![Baseline workflow: one ready task, six waiting](docs/design/starter-workflow-observation.png)

*Columns show dependency depth, not time. The optional
[observer](tools/queue-viewer/README.md) runs separately.*

## Decomposition and decisions

**Start → scan → claim task and instrument → dispatch → record result → release
instrument → rescan.**

![Early scheduler decomposition](docs/design/initial-scheduler-sketch.png)

*Early proposal: dependency counters were replaced by scanning.*

- Scanning avoids maintaining counters: O(V + E) per run scanned. Each event
  scans all active runs.
- One lock serialises simultaneous results and scheduling decisions across runs,
  preventing duplicate claims. It covers database updates and acknowledgements;
  slow acknowledgements delay scheduling, but physical execution overlaps.
- Only successful completion unlocks dependants. Busy refusals release the claim
  and trigger another scan after 250 ms, without consuming failure retries.
- Retryable failures release equipment and allow one retry after three seconds.
  Non-retryable or exhausted failures record the causal step, fail the run and
  mark unstarted tasks skipped. Late results remain recorded; other runs continue.
- Attempt IDs correlate results; transactional, append-only events retain history.
  Stale results are ignored. Unversioned results are accepted only on the first
  offer for compatibility with the supplied concurrency test.
- An acknowledgement timeout fails the run but retains the instrument reservation
  until a result arrives: acceptance is unknown, so resending could duplicate work.

## Scope and weaknesses

I chose reported-failure handling over missing-result recovery to deliver one
bounded extension. Distributed scheduling and automatic restart recovery were
omitted to focus on one executor process.

The main weakness is a crash between the database claim and command delivery.
Production handling needs reconciliation and driver-side deduplication before
safe replay; attempt IDs alone do not provide that. Scheduling is not fair or
optimal.

If a completion result is lost, the current scheduler waits indefinitely and
retains the instrument reservation. Production recovery would reconcile
instrument and experiment state before resuming; an unresolved outcome would
require abandoning the run and confirming a safe reset.

## Verification

Both workflows passed 5/5 acceptance checks; all 12 pytest tests, the persistent
failure check and lint passed. Four concurrent runs completed 26 tasks after four
injected transient failures and successful retries. Pytest uses a separate database.

## Exercise feedback

- I really enjoyed the exercise: the lab workflow was fascinating,
  and the problem statement was clear.
- As a visual thinker, I built a disposable dashboard inspired by GitHub Actions
  to understand the flow. That made the mental model click and was worth the time.
- Docker and a real PostgreSQL database made setup straightforward; I appreciated
  not having to guess how much infrastructure to build.
- My only suggested change is to include a basic visualisation in the starter pack.
