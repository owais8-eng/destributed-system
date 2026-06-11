# ETLOrchestrator — Project Overview

This document documents the Grain Silo Saga demo: a Node.js backend orchestrator implementing a Saga-pattern workflow and a minimal, "dumb" HTML/JS client that visualizes real-time state and telemetry via WebSockets.

Purpose
-------
Demonstrate a Saga-style orchestration (4-step DAG) with failure injection and compensating (rollback) actions. The project is intended as an educational demo of orchestration, telemetry, and resilient workflows.

Architecture summary
--------------------
- HTTP server (Node.js) serves the client HTML and exposes a control endpoint to start runs.
- WebSocket server (ws) broadcasts live telemetry, logs, and node state events to connected clients.
- Orchestrator implements a 4-step sequential DAG with compensating actions executed in reverse on failure.

Important files
---------------
- server.js — Backend HTTP + WebSocket server and the Saga orchestrator (main runtime).
- index.html — Frontend dumb client (dashboard). Connects to ws://localhost:3001 and renders messages only.
- index.ts / index.js — TypeScript/JS sources (if present).
- package.json / package-lock.json — Dependencies and metadata.

Runtime details
---------------
Ports
- HTTP: http://localhost:3000  (serves index.html)
- WebSocket: ws://localhost:3001  (real-time telemetry & events)

HTTP endpoints
- GET  /              -> serves index.html
- POST /start-workflow -> triggers a new saga run. Optional JSON body: { "failAt": "Transform_Validation" | "Load_Database" }

WebSocket message types
- telemetry: { throughput, anomalies, totalLatencyMs }
- node_state: { runId, nodeId, status, ... } where status in { active, success, failed, rolled_back }
- log: { text, ts }

Orchestrator workflow (Saga)
----------------------------
Four steps executed sequentially with simulated latencies and compensations:
1. Extract_Sensors — 400 ms (compensate ~150 ms)
2. Transform_Validation — 1800 ms (compensate ~300 ms) [bottleneck]
3. Load_Database — 300 ms (compensate ~200 ms)
4. Aggregate_Analytics — 500 ms (compensate ~200 ms)

Failure & compensation
- The system injects random failures (30% chance) at Transform_Validation or Load_Database unless an explicit `failAt` is provided in the POST body or chosen from the UI.
- On failure: forward progress stops, anomalies counter increments, and compensating actions run in reverse order for completed steps. Node state events are emitted for active, success, failed, and rolled_back states.

Telemetry & live updates
- Telemetry (simulated throughput between 75-85 Req/s) is broadcast every 1.5s regardless of workflow runs.
- Anomaly count is persisted in-memory while the server runs and included in telemetry messages.
- Latest completed workflow total latency is exposed in telemetry.

Frontend responsibilities
------------------------
- The client is intentionally "dumb": it performs NO business logic.
- It connects to ws://localhost:3001, listens for JSON payloads (telemetry, node_state, log), and updates the DOM only.
- UI includes: telemetry cards, a 4-node visual graph, start workflow control, and a terminal/log pane.

How to run locally
------------------
1. Install dependencies:
   - npm install
2. Start server:
   - node server.js
3. Open the dashboard:
   - http://localhost:3000
4. Trigger runs using the "Start Workflow" button or curl:
   - curl -X POST -d '{"failAt":"Transform_Validation"}' -H 'Content-Type: application/json' http://localhost:3000/start-workflow

Recommended package.json scripts
-------------------------------
Add these for convenience:
- "start": "node server.js"
- "dev": "npx ts-node index.ts"
- "build": "npx tsc"

Notes & next steps
------------------
- Logs and anomaly state are in-memory; persist to a database or file for long-term analysis.
- Add authentication/authorization to the control endpoint if used beyond demos.
- Add unit tests around orchestrator compensation logic and telemetry broadcast.
- Consider extracting orchestrator into a module and adding integration tests that simulate partial failures.

Contact
-------
This file was updated to document the Saga demo. The code was generated and edited by an AI assistant using Copilot CLI runtime in VS Code — review server.js and index.html for implementation details and tweak port/config as needed.
