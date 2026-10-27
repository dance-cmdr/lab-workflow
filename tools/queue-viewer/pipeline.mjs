// Pure layout: dependency depth sets the column, instrument sets the lane.
export function layoutPipeline(steps) {
  const byName = new Map(steps.map(s => [s.name,s]));
  const depths = new Map(), visiting = new Set();
  function depth(s) {
    if (depths.has(s.name)) return depths.get(s.name);
    if (visiting.has(s.name)) return 0;
    visiting.add(s.name);
    const d = Math.max(0,...s.depends_on.filter(n => byName.has(n)).map(n => depth(byName.get(n))+1));
    visiting.delete(s.name); depths.set(s.name,d); return d;
  }
  steps.forEach(depth);
  const known = ['liquid-handler-1','incubator-1','plate-reader-1'];
  const devices = [...new Set(steps.map(s => s.device_id))].sort((a,b) => {
    const ia=known.indexOf(a),ib=known.indexOf(b);
    return (ia < 0 ? 99 : ia)-(ib < 0 ? 99 : ib) || a.localeCompare(b);
  });
  const nodes=[],lanes=[];
  let top=40;
  for (const device of devices) {
    const slots=new Map();
    const laneSteps=steps.filter(s => s.device_id===device).sort((a,b)=>depth(a)-depth(b)||a.name.localeCompare(b.name));
    let rows=1;
    for (const s of laneSteps) {
      const col=depth(s), row=slots.get(col)||0;
      slots.set(col,row+1); rows=Math.max(rows,row+1);
      nodes.push({...s,x:166+col*232,y:top+32+row*90,width:196,height:68});
    }
    const height=rows*90+36;
    lanes.push({device,top,height}); top+=height;
  }
  const width=Math.max(760,166+(Math.max(0,...depths.values())+1)*232);
  return {nodes,lanes,width,height:top+20};
}

// Demonstration only: one slot per instrument, successful completions unlock dependants.
export function advanceDemo(data, now) {
  for (const s of data.steps) {
    if (s.status==='running' && now-s.demoStarted>=2400) {
      s.status='completed'; s.finished_at=new Date(now).toISOString();
    }
  }
  const byName=new Map(data.steps.map(s=>[s.name,s]));
  const busy=new Set(data.steps.filter(s=>s.status==='running').map(s=>s.device_id));
  // Prefer steps which unlock more work; this is illustrative, not the real scheduler.
  const downstream=n=>data.steps.filter(s=>s.depends_on.includes(n)).length;
  const ready=data.steps.filter(s=>s.status==='pending' && s.depends_on.every(n=>byName.get(n)?.status==='completed')).sort((a,b)=>downstream(b.name)-downstream(a.name)||a.name.localeCompare(b.name));
  for (const s of ready) {
    if(busy.has(s.device_id)) continue;
    s.status='running';s.dispatch_count=1;s.dispatched_at=new Date(now).toISOString();s.demoStarted=now;
    busy.add(s.device_id);
    data.drivers.find(d=>d.device_id===s.device_id)?.executed.push(s.id);
  }
  for(const d of data.drivers){const s=data.steps.find(s=>s.device_id===d.device_id && s.status==='running');d.busy=Boolean(s);d.current_step=s?.name||'';}
  if(data.steps.every(s=>s.status==='completed')){data.run.status='completed';data.run.finished_at=new Date(now).toISOString();}
  return data;
}

// Simulation policy: one reported failure on each run's first incubation.
// A retry releases the equipment, waits three seconds, then gets one more attempt.
export function advanceSharedDemo(snapshots, drivers, now, duration=2000, options={}) {
  const ordered=[...snapshots].sort((a,b)=>Date.parse(a.run.created_at)-Date.parse(b.run.created_at)||a.run.id.localeCompare(b.run.id));
  const all=ordered.flatMap((data,index)=>data.steps.map(step=>({step,data,index})));
  const stamp=new Date(now).toISOString();
  for(const {step:s,data} of all){
    if(s.status!=='running'||now-s.demoStarted<duration)continue;
    const fail=options.errors&&s.demoFailureTarget&&!s.demoFailedOnce;
    s.attempts ||= [];
    s.attempts.push({number:s.dispatch_count,started_at:s.dispatched_at,finished_at:stamp,outcome:fail?'failed':'completed'});
    if(fail){
      s.demoFailedOnce=true;drivers.find(d=>d.device_id===s.device_id).failed++;
      s.error='Simulated transient incubation error';
      if(options.retries&&data.run.status==='running'){
        s.status='pending';s.retryAt=now+3000;
        s.retryMessage='Attempt failed · retry 1/1 after 3s · instrument released';
      }else{
        s.status='failed';s.finished_at=stamp;data.run.status='failed';data.run.finished_at=stamp;
        for(const pending of data.steps.filter(t=>t.status==='pending')){pending.status='skipped';pending.error='Run failed · no new work will start';pending.finished_at=stamp;}
      }
    }else{s.status='completed';s.error=null;s.retryMessage=null;s.finished_at=stamp;}
  }
  const busy=new Set(all.filter(({step:s})=>s.status==='running').map(({step:s})=>s.device_id));
  const ready=all.filter(({step:s,data})=>data.run.status==='running'&&s.status==='pending'&&(!s.retryAt||now>=s.retryAt)&&s.depends_on.every(name=>data.steps.find(dep=>dep.name===name)?.status==='completed')).sort((a,b)=>a.index-b.index||a.step.name.localeCompare(b.step.name));
  for(const {step:s} of ready){
    if(busy.has(s.device_id))continue;
    s.status='running';s.dispatch_count++;s.dispatched_at=stamp;s.demoStarted=now;s.retryAt=null;s.retryMessage=null;busy.add(s.device_id);
    drivers.find(d=>d.device_id===s.device_id)?.executed.push(s.id);
  }
  for(const data of ordered){if(data.run.status==='running'&&data.steps.every(s=>s.status==='completed')){data.run.status='completed';data.run.finished_at ||=stamp;}}
  for(const d of drivers){const current=all.find(({step:s})=>s.device_id===d.device_id&&s.status==='running');d.busy=Boolean(current);d.current_step=current?.step.name||'';}
  return snapshots;
}
