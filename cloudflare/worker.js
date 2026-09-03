// Cloudflare Worker + Durable Object 一体化五子棋联机中转
// 与 server.js 使用相同协议，可无卡免费部署到 Cloudflare。
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    // WebSocket 升级请求统一交给全局房间对象处理
    if (request.headers.get('Upgrade') === 'websocket' || url.pathname === '/ws') {
      const id = env.GAME.idFromName('global');
      const stub = env.GAME.get(id);
      return stub.fetch(request);
    }
    // 其余请求交给静态资源（index.html）
    return env.ASSETS.fetch(request);
  }
};

export class Game {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rooms = new Map();   // code -> room
    this.conns = new Map();   // ws -> { roomCode, color, playerIndex }
    this.idCounter = 0;
  }

  makeId() {
    return (++this.idCounter) + '_' + Date.now().toString(36);
  }

  send(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }

  genCode() {
    let code;
    do {
      code = String(Math.floor(1000 + Math.random() * 9000));
    } while (this.rooms.has(code));
    return code;
  }

  bothConnected(room) {
    return room.players[0] && room.players[0].connected &&
           room.players[1] && room.players[1].connected;
  }

  notifyStart(room) {
    room.moves = [];
    room.players.forEach((p) => {
      if (p) this.send(p.ws, { type: 'start', color: p.color, turn: 1 });
    });
  }

  createRoom(ws, requested) {
    const code = (requested && /^\d{4}$/.test(requested) && !this.rooms.has(requested))
      ? requested
      : this.genCode();

    const slot = { ws, color: 1, connected: true, id: this.makeId() };
    const room = { code, players: [slot, null], lastActivity: Date.now(), moves: [] };
    this.rooms.set(code, room);
    this.conns.set(ws, { roomCode: code, color: 1, playerIndex: 0 });
    this.send(ws, { type: 'created', room: code, color: 1 });
  }

  joinRoom(ws, code) {
    const room = this.rooms.get(code);
    if (!room) return this.send(ws, { type: 'error', message: '房间不存在，请核对房间码' });

    let idx = -1;
    for (let i = 0; i < 2; i++) {
      const p = room.players[i];
      if (!p || !p.connected) { idx = i; break; }
    }
    if (idx === -1) return this.send(ws, { type: 'error', message: '房间已满' });

    const slot = { ws, color: idx + 1, connected: true, id: this.makeId() };
    room.players[idx] = slot;
    room.lastActivity = Date.now();
    this.conns.set(ws, { roomCode: code, color: idx + 1, playerIndex: idx });
    this.send(ws, { type: 'joined', room: code, color: idx + 1 });

    if (this.bothConnected(room)) this.notifyStart(room);
  }

  rejoinRoom(ws, code, color) {
    const room = this.rooms.get(code);
    if (!room) return this.send(ws, { type: 'error', message: '房间已失效，请重新创建' });

    const idx = (color === 1) ? 0 : 1;
    const old = room.players[idx];
    if (old && old.connected && old.ws !== ws) {
      return this.send(ws, { type: 'error', message: '座位已被占用，无法重连' });
    }

    const slot = { ws, color, connected: true, id: (old ? old.id : this.makeId()) };
    room.players[idx] = slot;
    room.lastActivity = Date.now();
    this.conns.set(ws, { roomCode: code, color, playerIndex: idx });

    this.send(ws, { type: 'rejoined', room: code, color });
    this.send(ws, { type: 'sync', moves: room.moves.slice() });

    if (this.bothConnected(room)) {
      room.players.forEach((p) => this.send(p.ws, { type: 'resume', color: p.color, turn: 1 }));
    }
  }

  relay(room, from, msg) {
    room.lastActivity = Date.now();
    if (msg.type === 'move') {
      room.moves.push({ x: msg.x, y: msg.y, color: msg.color });
    } else if (msg.type === 'reset') {
      room.moves = [];
    }
    const other = room.players.find((p) => p && p.ws !== from && p.connected);
    if (other) this.send(other.ws, msg);
  }

  handleMessage(ws, data) {
    let msg;
    try { msg = JSON.parse(data); } catch (e) { return; }

    const meta = this.conns.get(ws) || {};
    const room = this.rooms.get(meta.roomCode);
    if (room) room.lastActivity = Date.now();

    switch (msg.type) {
      case 'create':
        this.createRoom(ws, msg.room);
        break;
      case 'join':
        this.joinRoom(ws, msg.room);
        break;
      case 'rejoin':
        this.rejoinRoom(ws, msg.room, msg.color);
        break;
      case 'move':
      case 'reset':
      case 'undo':
      case 'chat':
        if (room) this.relay(room, ws, msg);
        break;
      case 'ping':
        this.send(ws, { type: 'pong' });
        break;
    }
  }

  onClose(ws) {
    const meta = this.conns.get(ws);
    if (!meta) return;
    const room = this.rooms.get(meta.roomCode);
    if (!room) { this.conns.delete(ws); return; }

    const idx = room.players.findIndex((p) => p && p.ws === ws);
    if (idx >= 0 && room.players[idx]) {
      room.players[idx].connected = false;
      const other = room.players[1 - idx];
      if (other && other.connected) {
        this.send(other.ws, { type: 'opponent_disconnected' });
      }
    }

    this.conns.delete(ws);

    setTimeout(() => {
      const r = this.rooms.get(room.code);
      if (r && !r.players.some((p) => p && p.connected)) {
        this.rooms.delete(room.code);
      }
    }, 30000);
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    const ws = server;
    const self = this;

    ws.addEventListener('message', (evt) => self.handleMessage(ws, evt.data));
    ws.addEventListener('close', () => self.onClose(ws));
    ws.addEventListener('error', () => self.onClose(ws));

    return new Response(null, { status: 101, webSocket: client });
  }
}