import {taskView} from './model.mjs';

const terminal = status => ['completed','failed','aborted'].includes(status);

// Scope only presentation. Keep global forecast slots and device contention intact.
export function filterSharedTimeline(model,runId='all') {
  if(runId==='all')return model;
  const matches=s=>s.run.id===runId;
  return {...model,runs:model.runs.filter(r=>r.id===runId),tasks:model.tasks.filter(matches),forecast:model.forecast.filter(matches),unresolved:model.unresolved.filter(matches),active:model.active.filter(matches)};
}

// Forecast is a local, conditional illustration, never a scheduler assignment.
export function sharedTimeline(snapshots, drivers, now, duration=2000) {
  const ordered=[...snapshots].sort((a,b)=>Date.parse(a.run.created_at)-Date.parse(b.run.created_at)||a.run.id.localeCompare(b.run.id));
  const runs=ordered.map((s,i)=>({...s.run,label:`R${i+1}`,index:i}));
  const tasks=ordered.flatMap((s,i)=>s.steps.map(step=>({...taskView(step,s.steps,s.run),run:s.run,runIndex:i,runLabel:runs[i].label,key:s.run.id+'/'+step.id})));
  const deviceIds=[...new Set(['liquid-handler-1','incubator-1','plate-reader-1',...drivers.map(d=>d.device_id),...tasks.map(s=>s.device_id)])];
  const completions=new Map(),free=new Map(deviceIds.map(id=>[id,0]));
  const active=tasks.filter(s=>s.column==='active');
  for(const s of tasks)if(s.status==='completed')completions.set(s.key,0);
  for(const s of active){completions.set(s.key,duration);free.set(s.device_id,duration);}
  const unknownBusy=new Set(drivers.filter(d=>d.error||(d.busy&&!active.some(s=>s.device_id===d.device_id))).map(d=>d.device_id));
  for(const id of unknownBusy)free.set(id,Infinity);
  const pending=tasks.filter(s=>s.status==='pending'&&s.run.status==='running');
  const forecast=[];
  while(pending.length){
    const candidates=pending.flatMap(s=>{
      const deps=s.depends_on.map(name=>tasks.find(t=>t.run.id===s.run.id&&t.name===name));
      if(deps.some(dep=>!dep||!completions.has(dep.key)))return [];
      const start=Math.max(free.get(s.device_id)||0,Math.max(0,(s.retryAt||now)-now),...deps.map(dep=>completions.get(dep.key)));
      return Number.isFinite(start)?[{...s,start,end:start+duration}]:[];
    }).sort((a,b)=>a.start-b.start||a.runIndex-b.runIndex||a.name.localeCompare(b.name));
    if(!candidates.length)break;
    const chosen=candidates[0];forecast.push(chosen);completions.set(chosen.key,chosen.end);free.set(chosen.device_id,chosen.end);
    pending.splice(pending.findIndex(s=>s.key===chosen.key),1);
  }
  const forecastKeys=new Set(forecast.map(s=>s.key));
  const unresolved=tasks.filter(s=>s.status==='pending'&&!forecastKeys.has(s.key)).map(s=>({...s,reason:terminal(s.run.status)?`Run ${s.run.status}`:s.run.status!=='running'?'Run not started':unknownBusy.has(s.device_id)?'Instrument busy; current task unknown':'Unresolved dependency or unavailable instrument'}));
  const devices=deviceIds.map(id=>{
    const own=tasks.filter(s=>s.device_id===id), driver=drivers.find(d=>d.device_id===id);
    const ready=own.filter(s=>s.column==='ready'&&s.run.status==='running');
    const inFlight=own.filter(s=>s.column==='active');
    return {id,ready,active:inFlight,waiting:own.filter(s=>s.column==='waiting'&&!terminal(s.run.status)),competing:ready.length>1||(ready.length>0&&(inFlight.length>0||driver?.busy)),unknownBusy:unknownBusy.has(id),driver};
  });
  return {runs,tasks,forecast,unresolved,active,devices,duration,now,horizon:Math.max(10000,...forecast.map(s=>s.end),active.length?duration:0)};
}
