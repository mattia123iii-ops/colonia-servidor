// WebSocket mínimo (RFC 6455) sin dependencias, con la misma interfaz básica que el paquete "ws":
// new WebSocketServer({ server, maxPayload }), wss.on("connection"), wss.clients,
// ws.send(data, {binary}), ws.ping(), ws.terminate(), ws.readyState, eventos message/close/error/pong.
const crypto = require("crypto");
const { EventEmitter } = require("events");

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

class MiniSocket extends EventEmitter {
  constructor(socket, maxPayload) {
    super();
    this.socket = socket;
    this.maxPayload = maxPayload;
    this.readyState = 1;
    this.buf = Buffer.alloc(0);
    this.fragments = [];
    this.fragOpcode = 0;
    socket.setNoDelay(true);
    socket.on("data", (d) => this._onData(d));
    socket.on("close", () => this._closed());
    socket.on("end", () => this._closed());
    socket.on("error", (e) => {
      this.emit("error", e);
      this._closed();
    });
  }

  _closed() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit("close");
  }

  _onData(d) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    while (this.readyState === 1) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0;
      const opcode = b[0] & 0x0f;
      const masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f;
      let off = 2;
      if (len === 126) {
        if (b.length < 4) return;
        len = b.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (b.length < 10) return;
        len = Number(b.readBigUInt64BE(2));
        off = 10;
      }
      if (len > this.maxPayload) return this.terminate();
      const need = off + (masked ? 4 : 0) + len;
      if (b.length < need) return;
      let payload = Buffer.from(b.subarray(off + (masked ? 4 : 0), need));
      if (masked) {
        const mask = b.subarray(off, off + 4);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      this.buf = b.subarray(need);
      this._frame(fin, opcode, payload);
    }
  }

  _frame(fin, opcode, payload) {
    if (opcode === 0x8) {
      // cierre
      this._write(0x8, payload.subarray(0, 2));
      this.socket.end();
      this._closed();
      return;
    }
    if (opcode === 0x9) return this._write(0xa, payload); // ping -> pong
    if (opcode === 0xa) return this.emit("pong");
    if (opcode === 0x0) {
      this.fragments.push(payload);
      if (!fin) return;
      const all = Buffer.concat(this.fragments);
      this.fragments = [];
      return this.emit("message", all, this.fragOpcode === 0x2);
    }
    if (opcode === 0x1 || opcode === 0x2) {
      if (!fin) {
        this.fragOpcode = opcode;
        this.fragments = [payload];
        return;
      }
      this.emit("message", payload, opcode === 0x2);
    }
  }

  _write(opcode, payload) {
    if (this.readyState !== 1 || this.socket.destroyed) return;
    const len = payload.length;
    let head;
    if (len < 126) {
      head = Buffer.from([0x80 | opcode, len]);
    } else if (len < 65536) {
      head = Buffer.alloc(4);
      head[0] = 0x80 | opcode;
      head[1] = 126;
      head.writeUInt16BE(len, 2);
    } else {
      head = Buffer.alloc(10);
      head[0] = 0x80 | opcode;
      head[1] = 127;
      head.writeBigUInt64BE(BigInt(len), 2);
    }
    this.socket.write(Buffer.concat([head, payload]));
  }

  send(data, opts) {
    const binary = (opts && opts.binary) || Buffer.isBuffer(data);
    const payload = Buffer.isBuffer(data) ? data : Buffer.from(String(data), "utf8");
    this._write(binary ? 0x2 : 0x1, payload);
  }

  ping() {
    this._write(0x9, Buffer.alloc(0));
  }

  terminate() {
    this.socket.destroy();
    this._closed();
  }
}

class WebSocketServer extends EventEmitter {
  constructor({ server, maxPayload = 1024 * 1024 }) {
    super();
    this.clients = new Set();
    server.on("upgrade", (req, socket) => {
      const key = req.headers["sec-websocket-key"];
      if (!key || String(req.headers["upgrade"] || "").toLowerCase() !== "websocket") {
        socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
        return;
      }
      const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
      socket.write(
        "HTTP/1.1 101 Switching Protocols\r\n" +
          "Upgrade: websocket\r\n" +
          "Connection: Upgrade\r\n" +
          "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
      );
      const ws = new MiniSocket(socket, maxPayload);
      this.clients.add(ws);
      ws.on("close", () => this.clients.delete(ws));
      this.emit("connection", ws, req);
    });
  }
}

module.exports = { WebSocketServer };
