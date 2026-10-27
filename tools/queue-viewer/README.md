# Workflow observer (optional)

**How it works:** polls the executor’s read-only APIs every two seconds and renders task states, dependencies and attempt history across shared instruments.

**Why I created it:** I think visually, so I built a GitHub Actions-inspired view to understand the workflow and see where tasks wait or retry.

## Start

From the repository root, start the assessment stack, then the observer:

```sh
docker compose up -d --build
python3 tools/queue-viewer/server.py
```

Open http://localhost:5010. Stop with Ctrl+C. Requires Python 3 and a modern browser;
no extra packages or build step. The server binds to loopback and proxies only
allowlisted GET endpoints to the executor on port 5001. It cannot create runs,
start experiments or change instrument settings. Use the assessment API or scripts
for those actions.

## Read the display

- **All runs** shows shared equipment; choose a run for its dependency graph and
  task details. The collapsible all-experiment queue remains unfiltered.
- **Grey:** waiting for dependencies. **Amber:** ready or awaiting retry.
  **Blue:** dispatched/in flight. **Green:** completed. **Red:** failed or skipped.
- Dependency columns show graph depth, not scheduled start times.
- The live timeline shows individual accepted attempts from API history over the
  last 60 seconds. Red attempts failed; green retries succeeded. Dashed released
  gaps include retry backoff and equipment waiting, and do not reserve equipment.
  Older records without attempt history may lack individual spans.
- Future slots are a conditional illustration assuming two-second successful
  operations. They are not the scheduler's plan or reservations.
- Task dispatch totals include retries. Driver execution counts mean accepted
  commands since each driver restarted, not successful completions.
- Instrument availability is global, even when a run is selected. Readiness is
  inferred from state and dependencies; it is not a promised queue order.

**Live** polls every two seconds. Pause freezes the view, not execution. Polling
may miss fast intermediate states; recorded attempt history retains outcomes.
A no-progress warning does not establish that an instrument failed or stopped.

**Simulation** runs entirely in the browser, across all loaded workflows. Its
error toggle fails the first incubation per run; its retry toggle allows one
retry after three seconds. Play/Pause and Restart affect only simulated state.
Simulation makes no API writes and is clearly labelled separately from live data.

## Visual assets

Inspired by Automata LINQ Cloud and GitHub Actions workflow views. The Automata
logo and locally served Roboto / JetBrains Mono fonts were obtained from
https://www.automata.tech/linq/cloud on 27 October 2026. This is a personal
assessment observer, not an official Automata product. Assets load locally;
reduced-motion preferences disable animations.
