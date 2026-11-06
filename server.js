// Servidor de salas de "La Colonia Olvidada".
// Une a los jugadores: el anfitrión crea una sala con código y los demás se unen.
// El servidor solo reenvía mensajes; el juego del anfitrión manda en la partida.
const http = require("http");
const { WebSocketServer } = require("./ws_mini");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;
const PROTO = 1;
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ";

const rooms = new Map(); // code -> { code, host, peers: Map<id, ws>, isPublic, created }

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end(`La Colonia Olvidada - servidor OK - salas: ${rooms.size}\n`);
});

const wss = new WebSocketServer({ server, maxPayload: 512 * 1024 });

function newCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function cleanName(n) {
  n = String(n || "Hormiga").replace(/[\u0000-\u001f]/g, "").trim().slice(0, 16);
  return n || "Hormiga";
}

function leave(ws) {
  const room = ws.room ? rooms.get(ws.room) : null;
  ws.room = null;
  if (!room) return;
  room.peers.delete(ws.id);
  if (room.host === ws) {
    // el anfitrión se fue: la sala se cierra
    for (const p of room.peers.values()) {
      send(p, { op: "closed" });
      p.room = null;
    }
    rooms.delete(room.code);
  } else {
    for (const p of room.peers.values()) send(p, { op: "peer_left", id: ws.id });
  }
}

wss.on("connection", (ws) => {
  ws.alive = true;
  ws.room = null;
  ws.id = 0;
  ws.on("pong", () => (ws.alive = true));

  ws.on("message", (data, isBinary) => {
    if (isBinary) {
      // [destino, ...datos] -> se reenvía como [remitente, ...datos]
      const room = ws.room ? rooms.get(ws.room) : null;
      if (!room || data.length < 2) return;
      const target = data[0];
      const out = Buffer.from(data);
      out[0] = ws.id;
      if (target === 0) {
        for (const [id, p] of room.peers) if (id !== ws.id && p.readyState === 1) p.send(out, { binary: true });
      } else {
        const p = room.peers.get(target);
        if (p && p.readyState === 1) p.send(out, { binary: true });
      }
      return;
    }
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      return;
    }
    if (msg.v !== undefined && msg.v !== PROTO) {
      send(ws, { op: "error", msg: "Versión distinta del juego. Actualiza el juego." });
      return;
    }
    switch (msg.op) {
      case "host": {
        leave(ws);
        const code = newCode();
        if (!code) return send(ws, { op: "error", msg: "Servidor lleno" });
        ws.id = 1;
        ws.name = cleanName(msg.name);
        ws.room = code;
        rooms.set(code, { code, host: ws, peers: new Map([[1, ws]]), isPublic: msg.public !== false, created: Date.now() });
        send(ws, { op: "hosted", code, id: 1 });
        break;
      }
      case "join": {
        leave(ws);
        const code = String(msg.code || "").toUpperCase().trim();
        const room = rooms.get(code);
        if (!room) return send(ws, { op: "error", msg: "No existe una sala con ese código." });
        if (room.peers.size >= MAX_PLAYERS) return send(ws, { op: "error", msg: "La sala está llena (4 jugadores)." });
        let id = 2;
        while (room.peers.has(id)) id++;
        ws.id = id;
        ws.name = cleanName(msg.name);
        ws.room = code;
        const peers = [...room.peers.values()].map((p) => ({ id: p.id, name: p.name }));
        room.peers.set(id, ws);
        send(ws, { op: "joined", code, id, peers });
        for (const p of room.peers.values()) if (p !== ws) send(p, { op: "peer_joined", id, name: ws.name });
        break;
      }
      case "list": {
        const list = [];
        for (const r of rooms.values()) {
          if (r.isPublic && r.peers.size < MAX_PLAYERS) list.push({ code: r.code, host: r.host.name, n: r.peers.size });
        }
        list.sort((a, b) => b.n - a.n);
        send(ws, { op: "list", rooms: list.slice(0, 30) });
        break;
      }
      case "leave":
        leave(ws);
        break;
    }
  });

  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
});

// latido: corta conexiones muertas y mantiene vivas las de los proxys
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) {
      leave(ws);
      ws.terminate();
      continue;
    }
    ws.alive = false;
    ws.ping();
  }
}, 20000);

server.listen(PORT, () => console.log("Servidor de La Colonia Olvidada escuchando en el puerto " + PORT));
