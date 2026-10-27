export function taskView(step, steps, run) {
  const byName = new Map(steps.map(s => [s.name, s]));
  const unmet = step.depends_on.filter(name => byName.get(name)?.status !== 'completed');
  let column;
  if (step.status === 'pending') column = unmet.length ? 'waiting' : 'ready';
  else if (['dispatched', 'running'].includes(step.status)) column = 'active';
  else if (step.status === 'completed') column = 'completed';
  else column = 'stopped';
  const terminal = ['failed', 'aborted', 'completed'].includes(run.status);
  if (step.status === 'pending' && terminal) column = 'stopped';
  return {...step, column, unmet, terminalPending: step.status === 'pending' && terminal};
}

export function runMetrics(steps, drivers) {
  const ids = new Set(steps.map(s => s.id));
  return {
    attempts: steps.reduce((n, s) => n + (Number(s.dispatch_count) || 0), 0),
    sentTasks: steps.filter(s => s.dispatch_count > 0).length,
    executions: drivers.reduce((n, d) => n + (d.executed || []).filter(id => ids.has(id)).length, 0),
    completed: steps.filter(s => s.status === 'completed').length,
  };
}

// Convert the executor's immutable offer/event history to observed occupancy.
export function withAttemptHistory(snapshot) {
  if (!Array.isArray(snapshot.attempts)) return snapshot;
  return {...snapshot, steps: snapshot.steps.map(step => {
    const offers = snapshot.attempts.filter(a => a.step_id === step.id);
    const attempts = offers.flatMap(a => {
      const event = kind => a.events.find(e => e.kind === kind);
      const accepted = event('accepted'), offered = event('offered');
      const outcome = event('failed') || event('completed');
      if (event('refused') || (!accepted && !outcome)) return [];
      return [{number:a.number, started_at:(accepted || offered).created_at,
        finished_at:outcome?.created_at, outcome:outcome?.kind || 'running',
        retryScheduled:!!event('retry_scheduled')}];
    }).sort((a,b) => a.number-b.number);
    return {...step, attempts, hasAttemptHistory:true,
      retryAt:step.retry_after ? Date.parse(step.retry_after) : null};
  })};
}
