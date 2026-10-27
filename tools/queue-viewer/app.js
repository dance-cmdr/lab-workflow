import {taskView, runMetrics, withAttemptHistory} from './model.mjs';
import {layoutPipeline, advanceSharedDemo} from './pipeline.mjs?v=3';
import {sharedTimeline, filterSharedTimeline} from './timeline.mjs?v=2';

const $ = id => document.getElementById(id);
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const pretty = name => String(name || '').replace(/_/g, ' ');
const deviceName = id => ({'liquid-handler-1':'Liquid handler','incubator-1':'Incubator','plate-reader-1':'Plate reader'}[id] || id);
const ago = milliseconds => {
  const seconds = Math.max(0, Math.floor(milliseconds / 1000));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`;
};
let selected = 'all', timer, polling = false, busy = false, again = false;
let lastData, lastSuccess = 0, lastProgress = 0, signature = '', previous = new Map(), changes = [];
let demoMode=false, demoTimer, demoData, liveData, graphKey='', graphSelection='', activeKey='';
let allSnapshots=[], timelineSelection='', timelineTasks=new Map(), timelineInitial=true;
let demoSnapshots=[],demoDrivers=[],simPaused=false,simClock=0,simLastReal=0;
const columns = [
  ['waiting','Waiting','Dependencies unfinished'],
  ['ready','Ready','Dependencies done · awaiting dispatch'],
  ['active','In flight','Dispatched or running'],
  ['completed','Completed','Successful result recorded'],
  ['stopped','Stopped','Failed, skipped or other state'],
];

async function get(path) {
  const response = await fetch('/api' + path, {cache:'no-store', signal:AbortSignal.timeout(15000)});
  if (!response.ok) throw new Error(`API request failed (${response.status})`);
  return response.json();
}

function render({run, steps, drivers}) {
  const views = steps.map(s => taskView(s, steps, run));
  const allView=selected==='all';
  for(const id of ['pipeline-section','queue-section','activity'])$(id).hidden=allView;
  if(!allView)renderPipeline(views, run);
  const snapshots=demoMode?demoSnapshots:allSnapshots;
  const shared=sharedTimeline(snapshots,drivers,demoMode?simClock:Date.now());
  renderShared(shared);
  const globalSteps=shared.tasks;
  const visibleSteps=allView?globalSteps:globalSteps.filter(s=>s.run.id===selected);
  const metrics = runMetrics(visibleSteps, drivers),allMetrics=runMetrics(globalSteps,drivers);
  document.querySelector('.stats').setAttribute('aria-label',allView?'All run counts':'Selected run counts');
  const otherRuns=Math.max(0,shared.runs.length-1);
  $('filter-context').textContent=allView?`Bird’s-eye view · ${shared.runs.length} runs share the same instruments. Filtering changes the view, not execution.`:`Sharing instruments with ${otherRuns} other run${otherRuns===1?'':'s'}. Filtering does not pause or isolate them.`;
  $('count-scope').textContent=allView?`${demoMode?'Simulation · ':''}All ${shared.runs.length} runs`:`${demoMode?'Simulation · ':''}${run.workflow_name} · selected run`;
  $('simulation-banner').hidden=!demoMode;
  if(demoMode){
    const finished=demoSnapshots.every(s=>['completed','failed'].includes(s.run.status))&&!globalSteps.some(s=>s.status==='running');
    const failures=globalSteps.reduce((n,s)=>n+(s.attempts||[]).filter(a=>a.outcome==='failed').length,0),retrying=globalSteps.filter(s=>s.retryAt).length;
    $('sim-events').innerHTML=globalSteps.flatMap(s=>(s.attempts||[]).map(a=>({...a,s}))).sort((a,b)=>Date.parse(b.finished_at)-Date.parse(a.finished_at)).map(a=>`<li><b>${escape(a.s.runLabel)} · ${escape(a.s.run.workflow_name)}</b> · ${escape(pretty(a.s.name))} · attempt ${a.number} · <strong class="attempt-${a.outcome}">${a.outcome}</strong></li>`).join('')||'<li>No attempts finished yet.</li>';
    const active=shared.active.length,ready=shared.devices.reduce((n,d)=>n+d.ready.length,0);
    $('simulation-banner').textContent=`ALL-RUNS SIMULATION · ${simPaused?'Paused · ':''}${allMetrics.completed}/${globalSteps.length} completed overall · ${active} in flight · ${ready} ready · ${failures} failed attempts · ${retrying} awaiting retry. ${finished?'Simulation complete. ':''}Simulated data only; no API writes.`;
    $('play').disabled=finished;$('play').textContent=finished?'Complete':simPaused?'Play':'Pause';
  }
  $('execution-label').textContent=demoMode?'Simulated executions':'Executions since driver restart';
  $('execution-source').textContent=demoMode?'Accepted commands · includes retries':'Accepted commands · history resets per driver';
  $('instrument-source').textContent=demoMode?'Simulated instrument state · demo only':'Live availability · across all runs';
  $('footer-source').textContent=demoMode?'Demo playback · simulated data · no API writes':'Read-only · polls every 2 seconds · independent of executor';
  $('total').textContent = visibleSteps.length;
  $('sent').textContent = `${metrics.sentTasks} / ${visibleSteps.length}`;
  $('attempts').textContent = `${metrics.attempts} recorded dispatch${metrics.attempts === 1 ? '' : 'es'} · includes retries`;
  $('executed').textContent = metrics.executions;
  $('done').textContent = `${metrics.completed} / ${visibleSteps.length}`;
  $('run-status').hidden=allView;
  $('run-status').textContent = run.status;
  $('run-status').className = `badge ${run.status}`;
  $('run-meta').textContent = demoMode ? `Demo · ${run.workflow_name}` : `Created ${new Date(run.created_at).toLocaleString([], {day:'numeric', month:'short',hour:'2-digit',minute:'2-digit'})} · ${run.id}`;
  const now = demoMode?simClock:Date.now();
  const activityTimes = [run.started_at, ...steps.flatMap(s => [s.dispatched_at,s.finished_at])].filter(Boolean).map(t => Date.parse(t)).filter(Number.isFinite);
  const base = activityTimes.length ? Math.max(...activityTimes) : now;
  const nextSignature = JSON.stringify([run.status, ...steps.map(s => [s.id,s.status,s.dispatch_count,s.error])]);
  if (signature && nextSignature !== signature) lastProgress = now;
  if (!signature) lastProgress = Math.min(now,base);
  signature = nextSignature;
  const quiet = now - lastProgress;
  const stalled = run.status === 'running' && quiet >= Number($('threshold').value) * 1000;
  const ready = views.filter(v => v.column === 'ready').length;
  const active = views.filter(v => v.column === 'active').length;
  $('notice').className = 'notice' + (stalled ? ' warn' : '');
  const localCompleted=steps.filter(s=>s.status==='completed').length;
  if (demoMode) {$('notice').className='notice demo-notice';$('notice').textContent=`Selected run · ${localCompleted}/${steps.length} completed · ${ready} ready · ${active} in flight. This run shares equipment with the other simulated runs.`;}
  else if (stalled) $('notice').textContent = `Selected run: no progress for ${ago(quiet)}. ${ready} ready · ${active} in flight. ${steps.every(s=>!s.dispatch_count) ? 'No commands recorded as sent. Check the scheduler before the instruments.' : 'Inspect the tasks in flight and instrument status.'}`;
  else if (run.status === 'completed') $('notice').textContent = `Run completed. ${localCompleted} of ${steps.length} tasks have a successful result.`;
  else if (['failed','aborted'].includes(run.status)) $('notice').textContent = `Run ${run.status}. Unstarted tasks are shown under Stopped; their recorded database state is preserved.`;
  else if (run.status === 'pending') $('notice').textContent = 'Run has not been started. Ready tasks have no unfinished dependencies; the scheduler has not been asked to dispatch them.';
  else $('notice').textContent = `${ready} ready · ${active} in flight · ${localCompleted} completed. Watching for the next change.`;

  $('instruments').innerHTML = drivers.map(d => {
    const own=shared.devices.find(x=>x.id===d.device_id);
    const related = own?.active||[];
    const pending = own?.ready.length||0;
    const visibleReady=own?.ready.filter(s=>allView||s.run.id===selected).length||0;
    const visibleActive=related.filter(s=>allView||s.run.id===selected).length;
    const slot = d.error ? 'Instrument state unavailable' : d.busy ? `Working: ${pretty(d.current_step) || 'unknown task'}` : 'Idle · available';
    return `<article class="instrument"><header><span>${escape(deviceName(d.device_id))}</span><span class="badge">${d.error ? 'Unknown' : d.busy ? 'Busy' : 'Idle'}</span></header><div class="slot ${d.error ? 'error' : d.busy ? 'busy' : ''}">${escape(slot)}</div><p>${allView?`${pending} ready across all runs · ${related.length} in flight`:`${visibleReady} ready here · ${visibleActive} in flight here · ${pending} ready overall`}${own?.competing?' · competing for equipment':''}</p><p>${escape(d.error || `${d.rejected || 0} refusals · ${d.failed || 0} failures · ${d.dropped || 0} dropped results (driver lifetime)`)}</p></article>`;
  }).join('') || '<p class="sub">No instrument state returned.</p>';
  renderTaskBoard('board',views,run,now);
  renderTaskBoard('all-board',globalSteps,null,now);
  $('all-queue-count').textContent=`${globalSteps.length} tasks · ${shared.runs.length} experiments`;
  $('all-queue-context').textContent=`${demoMode?'Simulation':'Live'} · Includes every experiment, regardless of the run filter above. Tags match the shared timeline’s run labels.`;
  for (const s of steps) {
    const old = previous.get(s.id);
    if (old && (old.status !== s.status || old.dispatch_count !== s.dispatch_count)) changes.unshift({time:now,text:`${pretty(s.name)}: ${old.status} → ${s.status}${old.dispatch_count !== s.dispatch_count ? ` · dispatches ${s.dispatch_count}` : ''}`});
    previous.set(s.id,{status:s.status,dispatch_count:s.dispatch_count});
  }
  changes = changes.slice(0,30);
  $('event-count').textContent = changes.length;
  $('events').innerHTML = changes.map(e => `<li>${new Date(e.time).toLocaleTimeString()} · ${escape(e.text)}</li>`).join('') || '<li>No changes observed yet.</li>';
}

function renderTaskBoard(id,steps,run,now){
  const root=$(id);
  const focusedTask=root.contains(document.activeElement)?document.activeElement.closest('.task')?.dataset.id:null;
  const oldRects=new Map([...root.querySelectorAll('.task')].map(el=>[el.dataset.id,el.getBoundingClientRect()]));
  const expanded=new Set([...root.querySelectorAll('.task[open]')].map(el=>el.dataset.id));
  root.innerHTML=columns.map(([key,label,desc])=>{
    const tasks=steps.filter(s=>s.column===key);
    return `<section class="column" data-state="${key}" aria-label="${label}"><h3 class="column-title"><span class="name"><i class="dot" aria-hidden="true"></i>${label}</span><span class="count">${tasks.length}</span></h3><p class="column-desc">${desc}</p>${tasks.length?tasks.map(s=>taskCard(s,s.run||run,expanded,now)).join(''):'<div class="empty">No tasks</div>'}</section>`;
  }).join('');
  if(!matchMedia('(prefers-reduced-motion: reduce)').matches){
    for(const el of root.querySelectorAll('.task')){
      const old=oldRects.get(el.dataset.id),rect=el.getBoundingClientRect();
      if(old&&rect.width&&old.width&&(Math.abs(old.x-rect.x)>1||Math.abs(old.y-rect.y)>1))el.animate([{transform:`translate(${old.x-rect.x}px,${old.y-rect.y}px)`},{transform:'translate(0,0)'}],{duration:550,easing:'cubic-bezier(.2,.8,.2,1)'});
    }
  }
  if(focusedTask)[...root.querySelectorAll('.task')].find(el=>el.dataset.id===focusedTask)?.querySelector('summary')?.focus({preventScroll:true});
}

function renderShared(model){
  const scoped=filterSharedTimeline(model,selected);
  const {runs,tasks,forecast,devices,now,horizon}=scoped;
  const ready=tasks.filter(s=>s.column==='ready'&&s.run.status==='running').length,waiting=tasks.filter(s=>s.column==='waiting'&&s.run.status==='running').length;
  const competing=devices.filter(d=>d.competing);
  const stopped=tasks.filter(s=>s.column==='stopped').length;
  const attemptView=demoMode||tasks.some(s=>s.hasAttemptHistory);
  $('shared-title').textContent=selected==='all'?'Shared instrument timeline':'Run timeline · shared equipment';
  $('shared-summary').textContent=`${runs.length} run${runs.length===1?'':'s'} · ${tasks.length} tasks · ${ready} ready · ${scoped.active.length} in flight · ${waiting} waiting on dependencies${stopped?' · '+stopped+' stopped':''}`;
  $('shared-mode').textContent=`${demoMode?'SIMULATION':'LIVE'} · ${selected==='all'?'ALL RUNS':'FILTERED VIEW'}`;
  document.querySelector('.timeline-key').innerHTML=attemptView?'<span><i class="sample solid"></i>Individual attempts</span><span style="color:var(--stopped)">× Failed · equipment released</span><span>↻ Retry attempt</span><span><i class="sample dashed"></i>Released / waiting gap</span><span>Dashed future: conditional forecast</span>':'<span><i class="sample solid"></i>Recorded dispatch → result</span><span><i class="sample dashed"></i>Conditional forecast</span><span>Amber: ready / competing</span><span>Grey stripes: awaiting dependencies</span>';
  $('run-legend').innerHTML=runs.map(r=>`<span class="run-chip" style="--run-color:var(--run-${r.index%4})"><b>${r.label}</b> ${escape(r.workflow_name)} <small>${escape(r.status)}</small></span>`).join('');
  const scale=.032,past=demoMode?Math.max(10000,now-Math.min(...runs.map(r=>Date.parse(r.started_at)||now))+1000):(attemptView?60000:10000),left=150,axis=48,row=attemptView?48:36;
  const nowX=left+past*scale,width=Math.max(950,nowX+horizon*scale+65);
  let top=axis;
  const rows=[];
  const bars=[];
  timelineTasks=new Map();
  const oldFocus=document.activeElement?.dataset.timelineTask;
  const oldRects=new Map([...document.querySelectorAll('[data-timeline-task]')].map(el=>[el.dataset.timelineTask,el.getBoundingClientRect()]));
  function bar(s,start,end,y,type,explanation,barLabel=null){
    if(end < -past)return;
    const x=left+(Math.max(-past,start)+past)*scale;
    const w=Math.max(10,(end-Math.max(-past,start))*scale);
    const key=s.key+'/'+type;
    timelineTasks.set(key,{s,type,start,end,explanation});
    const label=`${s.runLabel} · ${pretty(s.name)} · ${s.column} · ${explanation}`;
    bars.push(`<button type="button" class="timeline-task ${type}" data-state="${s.column}" data-timeline-task="${escape(key)}" aria-label="${escape(label)}" aria-pressed="${timelineSelection===key}" style="left:${x}px;top:${y}px;width:${w}px;--run-color:var(--run-${s.runIndex%4})"><span>${escape(barLabel||pretty(s.name))}</span></button>`);
  }
  for(const device of devices){
    const own=tasks.filter(s=>s.device_id===device.id);
    const visibleReady=device.ready.filter(s=>selected==='all'||s.run.id===selected).length;
    const readyRuns=new Set(device.ready.map(s=>s.run.id)).size;
    rows.push(`<div class="timeline-device" style="top:${top}px;width:${width}px"><div class="timeline-device-name">${escape(deviceName(device.id))}</div><span class="contention ${device.competing?'is-competing':''}">${selected==='all'?`${device.ready.length} ready · ${device.active.length} in flight`:`${visibleReady} ready here · ${device.ready.length} ready overall · ${device.active.length} in flight overall`}${device.competing?(readyRuns>1?` · ${readyRuns} runs competing`:' · waiting for equipment'):''}${device.unknownBusy?' · unknown availability':''}</span></div>`);
    top+=34;
    for(const run of runs){
      const y=top+3;
      const ownRun=own.filter(s=>s.run.id===run.id);
      const ownReady=device.ready.filter(s=>s.run.id===run.id).length;
      rows.push(`<div class="timeline-row" style="top:${top}px;width:${width}px;height:${row}px"><span class="timeline-row-label" style="--run-color:var(--run-${run.index%4})"><b>${run.label}</b><span>${ownReady?ownReady+' ready':ownRun.filter(s=>s.column==='waiting').length+' waiting'}</span></span></div>`);
      for(const s of ownRun){
        if(demoMode||s.hasAttemptHistory){
          const history=(s.attempts||[]).filter(a=>a.finished_at);
          for(const attempt of history){
            const failed=attempt.outcome==='failed',number=attempt.number;
            const start=Date.parse(attempt.started_at)-now,end=Date.parse(attempt.finished_at)-now;
            bar({...s,column:failed?'stopped':'completed'},start,end,y,`attempt-${number}`,`Attempt ${number} ${attempt.outcome}. Instrument released at +${((Date.parse(attempt.finished_at)-Date.parse(s.run.started_at))/1000).toFixed(1)}s from run start`,`${failed?'×':number>1?'↻':'✓'} #${number} ${pretty(s.name)}`);
            const next=(s.attempts||[]).find(a=>a.number>number);
            const retryStart=next?.started_at||(s.dispatch_count>number?s.dispatched_at:null);
            if(failed&&(retryStart||s.retryAt||(attempt.retryScheduled&&s.run.status==='running'))){
              const releaseEnd=retryStart?Date.parse(retryStart):now;
              bar({...s,column:'ready'},end,Math.max(end,releaseEnd-now),y+29,`retry-wait-${number}`,'Retry waiting: instrument released and available to other experiments; this interval does not reserve equipment','↻ released');
            }
          }
          if(s.column==='active'){
            const current=(s.attempts||[]).find(a=>!a.finished_at);
            const number=current?.number||s.dispatch_count;
            bar(s,Date.parse(current?.started_at||s.dispatched_at)-now,0,y,`running-${number}`,`Attempt ${number} running${number>1?' (retry)':''}`,`${number>1?'↻':'▶'} #${number} ${pretty(s.name)}`);
          }
        }else if(s.dispatched_at){
          const start=Date.parse(s.dispatched_at)-now;
          const end=s.finished_at?Date.parse(s.finished_at)-now:0;
          if(Number.isFinite(start)&&Number.isFinite(end))bar(s,start,end,y,'recorded',s.finished_at?'Recorded first dispatch to final result':'Dispatched; result not yet recorded');
        }else if(s.column==='active')bar(s,-200,0,y,'uncertain','In flight, but dispatch time is unavailable');
        if(s.column==='active')bar(s,0,model.duration,y,'forecast','Assumed 2 seconds remaining; outcome unknown');
      }
      for(const s of forecast.filter(s=>s.run.id===run.id&&s.device_id===device.id))bar(s,s.start,s.end,y,'forecast',`Conditional slot +${(s.start/1000).toFixed(0)}–${(s.end/1000).toFixed(0)}s from now${s.column==='ready'&&device.competing?'; competing for this instrument':''}`);
      top+=row;
    }
    top+=14;
  }
  let ticks='';
  for(let t=-Math.floor(past/5000)*5000;t<=horizon;t+=5000){const x=left+(t+past)*scale;ticks+=`<div class="timeline-grid" style="left:${x}px;height:${top}px"><span>${t===0?'NOW':(t>0?'+':'')+t/1000+'s'}</span></div>`;}
  $('shared-timeline').style.width=width+'px';$('shared-timeline').style.height=top+'px';
  $('shared-timeline').innerHTML=`<div class="timeline-axis-label">RECORDED <span>→ FORECAST</span></div>${ticks}${rows.join('')}<div class="timeline-now" style="left:${nowX}px;height:${top}px"></div>${bars.join('')}`;
  if(oldFocus)[...$('shared-timeline').querySelectorAll('[data-timeline-task]')].find(el=>el.dataset.timelineTask===oldFocus)?.focus({preventScroll:true});
  if(!matchMedia('(prefers-reduced-motion: reduce)').matches){
    for(const el of $('shared-timeline').querySelectorAll('[data-timeline-task]')){
      const old=oldRects.get(el.dataset.timelineTask),rect=el.getBoundingClientRect();
      if(old&&Math.abs(old.x-rect.x)>2)el.animate([{transform:`translateX(${old.x-rect.x}px)`},{transform:'translateX(0)'}],{duration:450,easing:'ease-out'});
    }
  }
  const hiddenHistory=tasks.filter(s=>s.finished_at&&Date.parse(s.finished_at)<now-past).length;
  $('timeline-footnote').textContent=demoMode?'Simulation history: every attempt since playback began. Red × = failed and released; ↻ = retry; dashed “released” gaps do not occupy equipment. Future slots remain conditional.':`Past: last ${past/1000} seconds${hiddenHistory?` (${hiddenHistory} earlier finished tasks outside this window)`:''}. Future: recalculated from each snapshot. No reservations. ${attemptView?'Individual accepted attempts: red × = failed and released; ↻ = retry. Dashed released gaps do not occupy equipment.':'Legacy records show first dispatch to final result.'}`;
  const unplanned=[...scoped.unresolved,...tasks.filter(s=>s.column==='stopped'&&s.status!=='pending').map(s=>({...s,reason:s.error||s.status}))];
  $('unplanned').hidden=!unplanned.length;
  $('unplanned').innerHTML=unplanned.length?`<strong>${unplanned.length} tasks have no forecast slot</strong><p>${unplanned.map(s=>`${escape(s.runLabel)} · ${escape(pretty(s.name))}: ${escape(s.reason)}`).join('<br>')}</p>`:'';
  if(timelineTasks.has(timelineSelection))showTimelineDetail(timelineTasks.get(timelineSelection));
  else $('timeline-detail').textContent=competing.length?`${ready} task${ready===1?'':'s'} ready in this view. ${competing.map(d=>deviceName(d.id)).join(', ')} ${competing.length===1?'has':'have'} competing work across all runs. Select a bar to inspect its conditional slot.`:'Select a task to inspect its run, state and resource contention.';
  if(timelineInitial){$('shared-timeline').parentElement.scrollLeft=Math.max(0,nowX-(attemptView?1300:260));timelineInitial=false;}
}

function showTimelineDetail({s,type,explanation}){
  $('timeline-detail').textContent=`${s.runLabel} · ${s.run.workflow_name} · ${pretty(s.name)} · ${deviceName(s.device_id)} · ${s.column}. ${explanation}.${s.error?' Error: '+s.error:''}`;
}
$('shared-timeline').addEventListener('click',event=>{
  const button=event.target.closest('[data-timeline-task]');if(!button)return;
  timelineSelection=button.dataset.timelineTask;
  const data=timelineTasks.get(timelineSelection);if(data)showTimelineDetail(data);
  for(const el of $('shared-timeline').querySelectorAll('[data-timeline-task]'))el.setAttribute('aria-pressed',String(el===button));
});

function renderPipeline(views,run) {
  const layout=layoutPipeline(views);
  const key=JSON.stringify([demoMode,run.id,views.map(s=>[s.id,s.name,s.device_id,s.depends_on])]);
  if(graphKey!==key){
    graphKey=key;graphSelection='';activeKey='';
    const byName=new Map(layout.nodes.map(s=>[s.name,s]));
    const edges=layout.nodes.flatMap(target=>target.depends_on.filter(n=>byName.has(n)).map(n=>({source:byName.get(n),target})));
    $('pipeline').style.width=layout.width+'px';$('pipeline').style.height=layout.height+'px';
    $('pipeline').innerHTML=[...new Set(layout.nodes.map(s=>s.x))].sort((a,b)=>a-b).map((x,i)=>`<span class="graph-stage" style="left:${x}px">${i===0?'NO PREREQUISITES':`DEPENDENCY LEVEL ${i}`}</span>`).join('')+layout.lanes.map(l=>`<div class="graph-lane" style="top:${l.top}px;height:${l.height}px"><span>${escape(deviceName(l.device))}<small>1 task at a time</small></span></div>`).join('')+
      `<svg class="graph-edges" width="${layout.width}" height="${layout.height}" aria-hidden="true">${edges.map(({source:s,target:t},i)=>{
        const x=s.x+s.width,y=s.y+s.height/2,tx=t.x,ty=t.y+t.height/2,m=(x+tx)/2;
        const d=`M ${x} ${y} C ${m} ${y}, ${m} ${ty}, ${tx} ${ty}`;
        return `<g data-edge-from="${escape(s.id)}" data-edge-to="${escape(t.id)}"><path class="edge-line" d="${d}"/><circle class="port" cx="${x}" cy="${y}" r="4"/><circle class="port" cx="${tx}" cy="${ty}" r="4"/><circle class="flow-token" r="4"><animateMotion dur="1.8s" repeatCount="indefinite" path="${d}"/></circle></g>`;
      }).join('')}</svg>`+
      layout.nodes.map(s=>`<button type="button" class="graph-task" data-task="${escape(s.id)}" style="left:${s.x}px;top:${s.y}px;width:${s.width}px;height:${s.height}px"><span class="state-icon" aria-hidden="true"></span><span class="graph-task-label"><strong>${escape(pretty(s.name))}</strong><small class="graph-task-state"></small></span><span class="graph-task-count"></span></button>`).join('');
    $('pipeline-detail').textContent='Select a task to see what it is waiting for.';
  }
  const byId=new Map(views.map(s=>[s.id,s]));
  for(const el of $('pipeline').querySelectorAll('.graph-task')){
    const s=byId.get(el.dataset.task),old=el.dataset.state;
    if(old!==s.column){el.dataset.state=s.column;if(old){el.classList.remove('state-changed');void el.offsetWidth;el.classList.add('state-changed');}}
    el.querySelector('.state-icon').textContent={waiting:'',ready:'',active:'',completed:'✓',stopped:'×'}[s.column];
    el.querySelector('.graph-task-state').textContent={waiting:`Waiting · ${s.unmet.length} dependenc${s.unmet.length===1?'y':'ies'}`,ready:run.status==='pending'?'Ready · run not started':'Ready · awaiting dispatch',active:'In flight · awaiting result',completed:'Completed',stopped:s.status}[s.column];
    el.querySelector('.graph-task-count').textContent=s.dispatch_count ? `${s.dispatch_count} sent` : '';
    el.setAttribute('aria-label',`${pretty(s.name)}: ${s.column}, ${s.dispatch_count} dispatches`);
    el.setAttribute('aria-pressed',String(graphSelection===s.id));
  }
  for(const edge of $('pipeline').querySelectorAll('[data-edge-from]')){
    const source=byId.get(edge.dataset.edgeFrom),target=byId.get(edge.dataset.edgeTo);
    edge.classList.toggle('satisfied',source.column==='completed');
    edge.classList.toggle('flowing',source.column==='completed' && target.column==='active');
    edge.classList.toggle('blocked',source.column==='stopped');
  }
  if(graphSelection) showGraphDetail(byId.get(graphSelection));
  const active=layout.nodes.filter(s=>s.column==='active');
  const nextActive=active.map(s=>s.id).join('|');
  if(nextActive!==activeKey && active.length && $('follow').checked){
    const scroll=$('pipeline').parentElement;
    const x=Math.min(...active.map(s=>s.x));
    scroll.scrollTo({left:Math.max(0,x-scroll.clientWidth/2+98),behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});
  }
  activeKey=nextActive;
}

function showGraphDetail(s){
  if(!s)return;
  $('pipeline-detail').textContent=`${pretty(s.name)} · ${deviceName(s.device_id)} · ${s.dispatch_count} sent · ${s.error || (s.unmet.length ? 'Needs: '+s.unmet.map(pretty).join(', ') : s.column==='completed' ? 'Finished successfully' : s.column==='active' ? 'Awaiting completion result' : 'All prerequisites satisfied')}`;
}

$('pipeline').addEventListener('click',event=>{
  const button=event.target.closest('[data-task]');if(!button)return;
  graphSelection=button.dataset.task;
  const data=demoMode?demoData:lastData;
  const s=data?.steps.find(s=>s.id===graphSelection);
  if(s)showGraphDetail(taskView(s,data.steps,data.run));
  for(const el of $('pipeline').querySelectorAll('.graph-task'))el.setAttribute('aria-pressed',String(el.dataset.task===graphSelection));
});

function taskCard(s, run, expanded, now) {
  let reason = '';
  if (s.terminalPending) reason = `Never started · recorded state: ${s.status}`;
  else if (s.column === 'waiting') reason = 'Needs: ' + s.unmet.map(pretty).join(', ');
  else if (s.column === 'ready') reason = run.status === 'pending' ? 'Run not started' : 'Dependencies satisfied · not dispatched';
  else if (s.column === 'active') reason = s.status === 'dispatched' ? 'Command dispatched · awaiting result' : 'Instrument execution in progress';
  else if (s.column === 'stopped') reason = s.error || `Recorded state: ${s.status}`;
  else reason = 'Finished successfully';
  if(s.retryAt)reason=`Retry in ${Math.max(0,Math.ceil((s.retryAt-now)/1000))}s · instrument released · other experiments can use it`;
  const reference = s.dispatched_at || run.started_at || run.created_at;
  const duration = s.finished_at && s.dispatched_at ? `took ${ago(Date.parse(s.finished_at)-Date.parse(s.dispatched_at))}` : s.finished_at ? 'finished' : `${s.dispatched_at ? 'in flight' : 'waiting'} ${ago(now-Date.parse(reference))}`;
  const tag=s.runLabel?`<span class="experiment-tag" style="--run-color:var(--run-${s.runIndex%4})"><b>${escape(s.runLabel)}</b><span>${escape(run.workflow_name)}</span></span>`:'';
  return `<details class="task" data-id="${escape(s.id)}" ${expanded.has(s.id) ? 'open' : ''}><summary>${tag}<span class="task-name">${escape(pretty(s.name))}</span><span class="task-device">${escape(deviceName(s.device_id))}</span><span class="task-meta"><span>${s.dispatch_count} sent</span><span>${duration}</span></span><p class="reason ${s.error ? 'problem' : ''}">${escape(reason)}</p></summary><div class="extra">${s.attempts?.length?`Attempt history: ${s.attempts.map(a=>`#${a.number} ${a.outcome}`).join(' → ')}<br>`:''}Database state: ${escape(s.status)}<br>Dependencies: ${escape(s.depends_on.map(pretty).join(', ') || 'None')}<br>Dispatched: ${s.dispatched_at ? escape(new Date(s.dispatched_at).toLocaleString()) : 'Never'}<br>Finished: ${s.finished_at ? escape(new Date(s.finished_at).toLocaleString()) : 'Not yet'}${s.error ? `<br>Error: ${escape(s.error)}` : ''}<br><code>${escape(s.id)}</code></div></details>`;
}

async function poll() {
  clearTimeout(timer);
  if(demoMode)return;
  if (busy) { again = true; return; }
  busy = true;
  try {
    const [runs,drivers] = await Promise.all([get('/runs'),get('/drivers')]);
    const snapshots=await Promise.all(runs.map(r=>get('/runs/'+encodeURIComponent(r.id))));
    if(demoMode)return;
    allSnapshots=snapshots.map(withAttemptHistory);
    liveData={drivers};
    if (snapshots.length) {
      renderCurrent();
    } else {
      populateRuns([]);
      $('notice').textContent = 'No runs yet. Create a run through the assessment API to see its tasks here.';
      $('board').innerHTML = '';
    }
    lastSuccess = Date.now();
    $('error').hidden = true;
    $('connection').textContent = polling ? '● Live · every 2s' : 'Ⅱ Paused';
    $('connection').className = polling ? 'live' : '';
    $('play').textContent=polling?'Pause':'Play';
    $('updated').textContent = `Updated ${new Date(lastSuccess).toLocaleTimeString()}`;
  } catch (error) {
    if(demoMode)return;
    $('error').hidden = false;
    $('error').textContent = `Could not refresh live state. Check that the assessment services are running on port 5001. ${lastSuccess ? 'Showing the last successful snapshot.' : 'No live data loaded.'}`;
    $('connection').textContent = '● Disconnected';
    $('connection').className = '';
    $('updated').textContent = lastSuccess ? `Last success ${new Date(lastSuccess).toLocaleTimeString()}` : 'Waiting for the API';
  } finally {
    busy = false;
    if (again) {again = false; timer = setTimeout(poll,0);}
    else if (polling && !demoMode) timer = setTimeout(poll,2000);
  }
}

function populateRuns(snapshots){
  const runs=snapshots.map(s=>s.run);
  if(selected!=='all'&&!runs.some(r=>r.id===selected))selected='all';
  const key=JSON.stringify([demoMode,runs.map(r=>[r.id,r.status])]);
  if($('run').dataset.options!==key){
    $('run').innerHTML=`<option value="all">All runs (${runs.length})</option>`+runs.map(r=>`<option value="${escape(r.id)}">${escape(r.workflow_name)} · ${new Date(r.created_at).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})} · ${escape(r.status)} · ${escape(r.id.slice(-6))}</option>`).join('');
    $('run').dataset.options=key;
  }
  $('run').value=selected;
}
function renderCurrent(){
  const snapshots=demoMode?demoSnapshots:allSnapshots;
  populateRuns(snapshots);
  const snapshot=(selected==='all'?null:snapshots.find(s=>s.run.id===selected))||snapshots[0];
  if(!snapshot)return;
  const data={...snapshot,drivers:demoMode?demoDrivers:liveData?.drivers||[]};
  if(demoMode)demoData=data;else lastData=data;
  render(data);
}
$('run').addEventListener('change',()=>{
  selected=$('run').value;signature='';previous.clear();changes=[];timelineInitial=true;timelineSelection='';
  renderCurrent();
});
$('threshold').addEventListener('change',renderCurrent);
$('mode').addEventListener('change',()=>{
  if($('mode').value==='simulation')startSimulation(true);else stopSimulation();
});
$('play').addEventListener('click',()=>{
  if(demoMode){
    simPaused=!simPaused;document.body.classList.toggle('simulation-paused',simPaused);clearTimeout(demoTimer);
    if(simPaused)renderCurrent();else{simLastReal=Date.now();tickSimulation();}
  }else{
    polling=!polling;clearTimeout(timer);$('play').textContent=polling?'Pause':'Play';
    if(polling)poll();else{$('connection').textContent='Ⅱ Live polling paused';$('connection').className='';}
  }
});
$('restart').addEventListener('click',()=>startSimulation(simPaused));
for(const id of ['sim-errors','sim-retries'])$(id).addEventListener('change',()=>{if(demoMode)startSimulation(true);});
function tickSimulation(){
  if(!demoMode||simPaused)return;
  const real=Date.now();simClock+=Math.max(0,real-simLastReal);simLastReal=real;
  advanceSharedDemo(demoSnapshots,demoDrivers,simClock,2000,{errors:$('sim-errors').checked,retries:$('sim-retries').checked});
  renderCurrent();
  if(demoSnapshots.some(s=>s.run.status==='running'||s.steps.some(t=>t.status==='running')))demoTimer=setTimeout(tickSimulation,300);
}
function startSimulation(paused=true){
  if(!allSnapshots.length){$('mode').value='live';return;}
  clearTimeout(timer);clearTimeout(demoTimer);
  demoMode=true;simPaused=paused;simClock=Date.now();simLastReal=simClock;
  signature='';previous.clear();changes=[];timelineInitial=true;
  demoSnapshots=structuredClone(allSnapshots).map(({run,steps})=>({run:{...run,status:'running',started_at:new Date(simClock).toISOString(),finished_at:null},steps:steps.map(s=>({...s,status:'pending',dispatch_count:0,dispatched_at:null,finished_at:null,error:null}))}));
  for(const data of demoSnapshots){const target=data.steps.find(s=>s.device_id==='incubator-1');if(target)target.demoFailureTarget=true;}
  $('simulation-options').hidden=false;$('simulation-history').hidden=false;
  demoDrivers=[...new Set(demoSnapshots.flatMap(s=>s.steps.map(t=>t.device_id)))].map(device_id=>({device_id,busy:false,current_step:'',executed:[],rejected:0,failed:0,dropped:0}));
  $('mode').value='simulation';$('restart').disabled=false;$('play').disabled=false;
  $('demo-caption').hidden=false;$('demo-caption').textContent='SIMULATION · This run is part of the shared simulation. Other runs keep using equipment when filtered out. No API writes.';
  $('graph-mode').textContent='SIMULATED DATA';
  $('error').hidden=true;$('connection').textContent='◉ All-runs simulation';$('connection').className='';$('updated').textContent='Simulated data · no API writes';
  $('threshold').disabled=true;
  document.body.classList.toggle('simulation-paused',simPaused);
  if(simPaused)renderCurrent();else tickSimulation();
}
function stopSimulation(){
  $('simulation-options').hidden=true;$('simulation-history').hidden=true;
  demoMode=false;clearTimeout(demoTimer);simPaused=false;signature='';previous.clear();changes=[];timelineInitial=true;
  document.body.classList.remove('simulation-paused');
  $('mode').value='live';$('restart').disabled=true;$('play').disabled=false;
  $('simulation-banner').hidden=true;$('demo-caption').hidden=true;$('graph-mode').textContent='LIVE DATA';$('threshold').disabled=false;
  polling=true;$('play').textContent='Pause';$('run').dataset.options='';
  renderCurrent();poll();
}
polling=true;
poll();
