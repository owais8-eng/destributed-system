
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { createClient } = require('redis');
const WebSocket = require('ws');

const PORT = parseInt(process.env.PORT || process.env.HTTP_PORT || '3000', 10);
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const CHANNEL = 'saga_events';
const INDEX_PATH = path.join(__dirname, 'index.html');

const pid = process.pid;
let runCounter = 0;

let latestTotalLatencyMs = 0;

const pubClient = createClient({ url: REDIS_URL });
const subClient = createClient({ url: REDIS_URL });

async function initRedis() {
    await pubClient.connect();
    await subClient.connect();
    console.log(`[REDIS] Connected to ${REDIS_URL} (PID ${pid})`);
}


async function publishEvent(type, payload) {
    try {
        const envelope = {
            type,
            payload,
            meta: { originPid: pid, originPort: PORT, host: os.hostname(), ts: Date.now() }
        };
        await pubClient.publish(CHANNEL, JSON.stringify(envelope));
        console.log(`[PUB] ${type} (originPid=${pid}, originPort=${PORT})`, payload);
    } catch (err) {
        console.error('[PUB] publish failed', err);
    }
}


async function startServer() {
    const server = http.createServer((req, res) => {
        if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
            fs.readFile(INDEX_PATH, 'utf8', (err, data) => {
                if (err) {
                    res.writeHead(500, { 'Content-Type': 'text/plain' });
                    res.end('Server error');
                    return;
                }
                res.writeHead(200, { 'Content-Type': 'text/html' });
                res.end(data);
            });
            return;
        }

        if (req.method === 'POST' && req.url === '/start-workflow') {
            let body = '';
            req.on('data', chunk => (body += chunk));
            req.on('end', () => {
                let parsed = {};
                try {
                    parsed = body ? JSON.parse(body) : {};
                } catch (e) { parsed = {}; }

                res.writeHead(202, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ status: 'accepted', originPid: pid, originPort: PORT }));

                
                const runId = ++runCounter;
                startSagaRun(runId, parsed.failAt).catch(err => {
                    publishEvent('log', { text: `Run ${runId} crashed unexpectedly: ${err && err.stack ? err.stack : err}` });
                });
            });
            return;
        }

        res.writeHead(404);
        res.end('Not found');
    });

    const wss = new WebSocket.Server({ server });

 
    await subClient.subscribe(CHANNEL, (rawMessage) => {
        try {
            const envelope = JSON.parse(rawMessage);
            console.log(`[SUB:${PORT}] Received ${envelope.type} from originPid=${envelope.meta.originPid} originPort=${envelope.meta.originPort}`);
            const message = JSON.stringify(envelope);
          
            for (const client of wss.clients) {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(message);
                }
            }
        } catch (err) {
            console.error('[SUB] failed to process message', err);
        }
    });

    wss.on('connection', (ws, req) => {
        console.log(`[WS:${PORT}] client connected (pid=${pid}) from ${req.socket.remoteAddress}`);
        ws.send(JSON.stringify({ type: 'log', payload: { text: `Connected to server pid=${pid}, port=${PORT}` }, meta: { ts: Date.now(), originPid: pid, originPort: PORT } }));
        ws.on('close', () => console.log(`[WS:${PORT}] client disconnected`));
        ws.on('error', (err) => console.error(`[WS:${PORT}] socket error`, err));
    });

    server.listen(PORT, () => {
        console.log(`HTTP+WS server listening on http://localhost:${PORT} (PID ${pid})`);
        publishEvent('log', { text: `Server started (PID ${pid}, PORT ${PORT})` });
    });

   
    setInterval(async () => {
        try {
            const raw = await pubClient.get('global_anomalies_count'); 
            const currentAnomalies = parseInt(raw, 10) || 0;
            const throughput = Math.floor(Math.random() * (85 - 75 + 1)) + 75; 
            const payload = {
                throughput,
                anomalies: currentAnomalies,
                totalLatencyMs: latestTotalLatencyMs
            };
            await publishEvent('telemetry', payload);
        } catch (redisErr) {
            console.error('Redis GET failed during telemetry:', redisErr);
            const payloadFallback = {
                throughput: null,
                anomalies: 0,
                totalLatencyMs: latestTotalLatencyMs
            };
            await publishEvent('telemetry', payloadFallback);
        }
    }, 1500);
}

async function startSagaRun(runId, explicitFailAt) {
    const runLabel = `run-${String(runId).padStart(3, '0')}`;
    await publishEvent('log', { text: `Starting workflow ${runLabel}`, runId });

    const startTs = Date.now();
    const completedSteps = [];

    function shouldFailAt(stepId) {
        if (explicitFailAt) return explicitFailAt === stepId;
        if (stepId === 'Transform_Validation' || stepId === 'Load_Database') {
            return Math.random() < 0.30; 
        }
        return false;
    }

    function emitNode(nodeId, status, extra = {}) {
        
        publishEvent('node_state', { runId, nodeId, status, ...extra });
    }

    const steps = [
        { id: 'Extract_Sensors', friendly: 'Extract', execMs: 400, compMs: 150 },
        { id: 'Transform_Validation', friendly: 'Transform', execMs: 1800, compMs: 300 },
        { id: 'Load_Database', friendly: 'Load', execMs: 300, compMs: 200 },
        { id: 'Aggregate_Analytics', friendly: 'Aggregate', execMs: 500, compMs: 200 }
    ];

    try {
        for (const step of steps) {
            await publishEvent('log', { text: `Run ${runLabel}: ${step.friendly} => starting`, step: step.id });
            emitNode(step.id, 'active', { message: `${step.friendly} started` });

            if (shouldFailAt(step.id)) {
                await sleep(Math.max(30, Math.floor(Math.random() * 150)));
                emitNode(step.id, 'failed', { message: `${step.friendly} failed` });
                await publishEvent('log', { text: `Run ${runLabel}: ${step.friendly} FAILED (injected)`, step: step.id });

                try {
                    await pubClient.incr('global_anomalies_count'); // atomic INCR prevents race conditions
                } catch (redisErr) {
                    console.error('Redis INCR failed (on failure path):', redisErr);
                }

                throw new Error(`${step.id} failed`);
            }

            const execStart = Date.now();
            await sleep(step.execMs);
            const execMs = Date.now() - execStart;
            completedSteps.push(step);
            emitNode(step.id, 'success', { message: `${step.friendly} succeeded`, durationMs: execMs });
            await publishEvent('log', { text: `Run ${runLabel}: ${step.friendly} => success (${execMs}ms)`, step: step.id, durationMs: execMs });
        }

        const totalMs = Date.now() - startTs;
        latestTotalLatencyMs = totalMs;
        await publishEvent('log', { text: `Run ${runLabel}: Completed successfully in ${totalMs} ms` });

        
        try {
            const raw = await pubClient.get('global_anomalies_count');
            const currentAnomalies = parseInt(raw, 10) || 0;
            await publishEvent('telemetry', { throughput: null, anomalies: currentAnomalies, totalLatencyMs: latestTotalLatencyMs });
        } catch (redisErr) {
            console.error('Redis GET failed when publishing final telemetry:', redisErr);
            await publishEvent('telemetry', { throughput: null, anomalies: 0, totalLatencyMs: latestTotalLatencyMs });
        }

    } catch (err) {
        await publishEvent('log', { text: `Run ${runLabel}: Failure detected - starting rollback`, error: err.message });
        try {
            const raw = await pubClient.get('global_anomalies_count');
            const currentAnomalies = parseInt(raw, 10) || 0;
            await publishEvent('telemetry', { throughput: null, anomalies: currentAnomalies, totalLatencyMs: latestTotalLatencyMs });
        } catch (redisErr) {
            console.error('Redis GET failed during failure telemetry:', redisErr);
            await publishEvent('telemetry', { throughput: null, anomalies: 0, totalLatencyMs: latestTotalLatencyMs });
        }

        for (let i = completedSteps.length - 1; i >= 0; i--) {
            const completed = completedSteps[i];
            emitNode(completed.id, 'active', { message: `Compensating ${completed.friendly}` });
            await publishEvent('log', { text: `Run ${runLabel}: Compensating ${completed.friendly}` });

            try {
                await sleep(completed.compMs);
                emitNode(completed.id, 'rolled_back', { message: `${completed.friendly} rolled back` });
                await publishEvent('log', { text: `Run ${runLabel}: Compensation complete for ${completed.friendly}` });
            } catch (compErr) {
                emitNode(completed.id, 'failed', { message: `${completed.friendly} compensation FAILED` });
                await publishEvent('log', { text: `Run ${runLabel}: Compensation FAILED for ${completed.friendly}: ${compErr && compErr.stack ? compErr.stack : compErr}` });
            }
        }

        await publishEvent('log', { text: `Run ${runLabel}: Rollback complete. Workflow aborted.` });
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

(async () => {
    try {
        await initRedis();

        await startServer();

        console.log(`Instance ready -> PID: ${pid}, PORT: ${PORT}, Redis: ${REDIS_URL}`);
    } catch (err) {
        console.error('Startup failed', err);
        process.exit(1);
    }
})();

process.on('SIGINT', async () => {
    console.log('SIGINT received, shutting down...');
    try { await publishEvent('log', { text: `Shutting down server pid=${pid}` }); } catch (e) { }
    try { await subClient.unsubscribe(CHANNEL); } catch (e) { }
    try { await subClient.quit(); } catch (e) { }
    try { await pubClient.quit(); } catch (e) { }
    process.exit(0);
});