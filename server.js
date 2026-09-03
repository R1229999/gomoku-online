// ============================================================
//  Gomoku 五子棋 联机服务器（前端 + WebSocket 后端 一体化）
//  ------------------------------------------------------------
//  单一服务即可同时提供页面和联机中转，微信好友直接打开本服务地址即可玩。
//
//  部署到 Render 免费平台:
//    1. 把 server.js / index.html / package.json 放到一个 GitHub 仓库
//    2. Render: New -> Web Service -> 连接该仓库
//    3. Runtime: Node
//    4. Build Command:  npm install
//    5. Start Command:  node server.js
//    6. 部署完成后把 https://xxx.onrender.com 发给微信好友
//
//  本地测试:  npm install && node server.js
// ============================================================

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('index.html not found');
    }
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Gomoku WS server is running');
  }
});

const wss = new WebSocket.Server({ server });

// code -> room
// room: { code, players:[slot|null, slot|null], lastActivity, moves }
// slot: { ws, color(1=黑/2=白), connected, id }
const rooms = new Map();

let idCounter = 0;
function makeId() {
  return (++idCounter) + '_' + Date.now().toString(36);
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}

function genCode() {
  let code;
  do {
    code = String(Math.floor(1000 + Math.random() * 9000));
  } while (rooms.has(code));
  return code;
}

function bothConnected(room) {
  return room.players[0] && room.players[0].connected &&
         room.players[1] && room.players[1].connected;
}

function notifyStart(room) {
  room.moves = [];
  room.players.forEach((p) => {
    if (p) send(p.ws, { type: 'start', color: p.color, turn: 1 });
  });
}

function createRoom(ws, requested) {
  const code = (requested && /^\d{4}$/.test(requested) && !rooms.has(requested))
    ? requested
    : genCode();

  const slot = { ws, color: 1, connected: true, id: makeId() };
  const room = { code, players: [slot, null], lastActivity: Date.now(), moves: [] };
  rooms.set(code, room);

  ws.roomCode = code;
  ws.color = 1;
  ws.playerIndex = 0;

  send(ws, { type: 'created', room: code, color: 1 });
}

function joinRoom(ws, code) {
  const room = rooms.get(code);
  if (!room) return send(ws, { type: 'error', message: '房间不存在，请核对房间码' });

  let idx = -1;
  for (let i = 0; i < 2; i++) {
    const p = room.players[i];
    if (!p || !p.connected || (p.ws && p.ws.readyState !== WebSocket.OPEN)) {
      idx = i;
      break;
    }
  }
  if (idx === -1) return send(ws, { type: 'error', message: '房间已满' });

  const slot = { ws, color: idx + 1, connected: true, id: makeId() };
  room.players[idx] = slot;
  room.lastActivity = Date.now();

  ws.roomCode = code;
  ws.color = idx + 1;
  ws.playerIndex = idx;

  send(ws, { type: 'joined', room: code, color: idx + 1 });

  if (bothConnected(room)) notifyStart(room);
}

function rejoinRoom(ws, code, color) {
  const room = rooms.get(code);
  if (!room) return send(ws, { type: 'error', message: '房间已失效，请重新创建' });

  const idx = (color === 1) ? 0 : 1;
  const old = room.players[idx];

  if (old && old.connected && old.ws !== ws && old.ws.readyState === WebSocket.OPEN) {
    return send(ws, { type: 'error', message: '座位已被占用，无法重连' });
  }

  const slot = { ws, color, connected: true, id: (old ? old.id : makeId()) };
  room.players[idx] = slot;
  room.lastActivity = Date.now();

  ws.roomCode = code;
  ws.color = color;
  ws.playerIndex = idx;

  send(ws, { type: 'rejoined', room: code, color });
  send(ws, { type: 'sync', moves: room.moves.slice() });

  if (bothConnected(room)) {
    room.players.forEach((p) => send(p.ws, { type: 'resume', color: p.color, turn: 1 }));
  }
}

function relay(room, from, msg) {
  room.lastActivity = Date.now();
  if (msg.type === 'move') {
    room.moves.push({ x: msg.x, y: msg.y, color: msg.color });
  } else if (msg.type === 'reset') {
    room.moves = [];
  }
  const other = room.players.find((p) => p && p.ws !== from && p.connected);
  if (other) send(other.ws, msg);
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }

    const room = rooms.get(ws.roomCode);
    if (room) room.lastActivity = Date.now();

    switch (msg.type) {
      case 'create':
        createRoom(ws, msg.room);
        break;
      case 'join':
        joinRoom(ws, msg.room);
        break;
      case 'rejoin':
        rejoinRoom(ws, msg.room, msg.color);
        break;
      case 'move':
      case 'reset':
      case 'undo':
      case 'chat':
        if (room) relay(room, ws, msg);
        break;
      case 'ping':
        send(ws, { type: 'pong' });
        break;
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    const idx = room.players.findIndex((p) => p && p.ws === ws);
    if (idx >= 0 && room.players[idx]) {
      room.players[idx].connected = false;
      const other = room.players[1 - idx];
      if (other && other.connected && other.ws.readyState === WebSocket.OPEN) {
        send(other.ws, { type: 'opponent_disconnected' });
      }
    }
    // 30 秒宽限期, 等待重连; 超时且双方均断开则删除房间
    setTimeout(() => {
      const r = rooms.get(room.code);
      if (r && !r.players.some((p) => p && p.connected)) {
        rooms.delete(room.code);
      }
    }, 30000);
  });
});

// 心跳检测, 清理死连接
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

// 空闲房间自动清理
const cleaner = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const anyConnected = room.players.some((p) => p && p.connected);
    if (!anyConnected && now - room.lastActivity > 30000) {
      rooms.delete(code);
    } else if (now - room.lastActivity > 3600000) { // 1 小时
      room.players.forEach((p) => { if (p) try { p.ws.close(); } catch (e) {} });
      rooms.delete(code);
    }
  }
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeat);
  clearInterval(cleaner);
});

server.listen(PORT, () => {
  console.log('Gomoku server listening on port ' + PORT);
  console.log('Open http://localhost:' + PORT);
});