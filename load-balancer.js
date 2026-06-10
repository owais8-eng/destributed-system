const http = require('http');
const httpProxy = require('http-proxy');



const ALGORITHM = (process.env.LB_ALGORITHM || 'round-robin').toLowerCase();
const PROXY_PORT = parseInt(process.env.PROXY_PORT || '8080', 10);


const servers = [
  { id: 'A', host: 'localhost', port: 3001, weight: 3, alive: true, active: 0 },
  { id: 'B', host: 'localhost', port: 3002, weight: 1, alive: true, active: 0 },
  { id: 'C', host: 'localhost', port: 3003, weight: 1, alive: true, active: 0 },
];

const TOTAL_WEIGHT = servers.reduce((sum, s) => sum + s.weight, 0);


const proxy = httpProxy.createProxyServer({});

proxy.on('error', (err, req, res) => {
  if (res && !res.headersSent) {
    res.writeHead(502, { 'Content-Type': 'text/plain' });
    res.end('Bad Gateway');
  }
});


let rrIndex = 0;
function roundRobin() {
  const alive = servers.filter(s => s.alive);
  if (alive.length === 0) return null;
  const server = alive[rrIndex % alive.length];
  rrIndex = (rrIndex + 1) % alive.length;
  return server;
}


function weightedRoundRobin() {
  const alive = servers.filter(s => s.alive && s.weight > 0);
  if (alive.length === 0) return null;

  let best = null;
  for (const s of alive) {
    s.effectiveWeight = (s.effectiveWeight || 0) + s.weight;
    if (!best || s.effectiveWeight > best.effectiveWeight) {
      best = s;
    }
  }

  best.effectiveWeight -= TOTAL_WEIGHT;
  return best;
}


function leastConnections() {
  const alive = servers.filter(s => s.alive);
  if (alive.length === 0) return null;
  return alive.reduce((a, b) => (a.active <= b.active ? a : b));
}



function selectServer() {
  switch (ALGORITHM) {
    case 'weighted-round-robin':
    case 'weighted':
    case 'wrr':
      return weightedRoundRobin();
    case 'least-connections':
    case 'leastconn':
    case 'lc':
      return leastConnections();
    case 'round-robin':
    case 'rr':
    default:
      return roundRobin();
  }
}


const server = http.createServer((req, res) => {
  const target = selectServer();
  if (!target) {
    res.writeHead(503, { 'Content-Type': 'text/plain' });
    res.end('Service Unavailable – no alive backends');
    return;
  }

  const targetUrl = `http://${target.host}:${target.port}`;
  console.log(`[${ALGORITHM}]  →  ${target.id}  (${targetUrl})  active=${target.active}`);

  target.active += 1;

  proxy.web(req, res, { target: targetUrl }, (proxyErr) => {
    target.active = Math.max(0, target.active - 1);

    if (proxyErr.code === 'ECONNREFUSED') {
      console.warn(`[!] ${target.id} is DOWN – marking dead, retrying...`);
      target.alive = false;

      if (!global._healthCheckScheduled) {
        global._healthCheckScheduled = true;
        setInterval(() => {
          for (const s of servers) {
            if (!s.alive) {
              const conn = require('net').createConnection(s.port, s.host, () => {
                s.alive = true;
                console.log(`[+] ${s.id} is back ONLINE`);
                conn.end();
              });
              conn.on('error', () => { /* still dead */ });
              conn.setTimeout(1000, () => { conn.destroy(); });
            }
          }
        }, 5000);
      }

      const retryTarget = selectServer();
      if (retryTarget) {
        retryTarget.active += 1;
        console.log(`[RETRY]  →  ${retryTarget.id}`);
        proxy.web(req, res, { target: `http://${retryTarget.host}:${retryTarget.port}` }, () => {
          retryTarget.active = Math.max(0, retryTarget.active - 1);
          if (!res.headersSent) {
            res.writeHead(502);
            res.end('Bad Gateway');
          }
        });
      } else if (!res.headersSent) {
        res.writeHead(503);
        res.end('Service Unavailable');
      }
    }
  });

  res.on('finish', () => {
    target.active = Math.max(0, target.active - 1);
  });
});

server.listen(PROXY_PORT, () => {
  console.log(`Load Balancer listening on :${PROXY_PORT}`);
  console.log(`Algorithm : ${ALGORITHM}`);
  console.log(`Backends  : ${servers.map(s => `${s.id}(:${s.port},w=${s.weight})`).join(', ')}`);
});


server.on('upgrade', (req, socket, head) => {
  const target = selectServer();
  if (!target) {
    socket.destroy();
    return;
  }

  console.log(`[WS-UPGRADE] → ${target.id} (${target.host}:${target.port})`);

  // توجيه اتصال WebSocket للخادم الهدف
  proxy.ws(req, socket, head, { target: `http://${target.host}:${target.port}` }, (err) => {
    console.error(`[WS-ERROR] ${target.id}:`, err.message);
    socket.destroy();
  });
});
