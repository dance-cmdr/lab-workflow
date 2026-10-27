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
