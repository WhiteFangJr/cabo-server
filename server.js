// ═══════════════════════════════════════════════════════════════
// CABO – Game of Thrones Edition  |  Railway WebSocket Server
// ═══════════════════════════════════════════════════════════════
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'http';

const PORT = process.env.PORT || 3000;

// rooms: { [roomCode]: { state, pendingJoins, conns } }
const rooms = {};

function getRoom(code) {
  if (!rooms[code]) rooms[code] = { state: null, pendingJoins: [], conns: new Set() };
  return rooms[code];
}

function broadcast(room, msg) {
  const str = JSON.stringify(msg);
  for (const conn of room.conns) {
    if (conn.readyState === WebSocket.OPEN) conn.send(str);
  }
}

function applyJoin(room, playerId, playerName, conn) {
  const already = room.state.players.some(p => p.id === playerId);
  if (already) {
    if (conn) conn.send(JSON.stringify({ type: 'state', state: room.state }));
    return;
  }
  if (room.state.phase !== 'lobby') {
    if (conn) conn.send(JSON.stringify({ type: 'error', message: 'Game already started' }));
    return;
  }
  if (room.state.players.length >= 6) {
    if (conn) conn.send(JSON.stringify({ type: 'error', message: 'Room is full (max 6)' }));
    return;
  }
  room.state.players.push({ id: playerId, name: playerName });
  broadcast(room, { type: 'state', state: room.state });
}

// HTTP server (Railway needs a port to bind)
const httpServer = createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const url = new URL(req.url, 'http://localhost');

  // Diagnostic: list all active rooms
  if (url.pathname === '/rooms') {
    const summary = Object.entries(rooms).map(([code, room]) => ({
      room: code,
      phase: room.state?.phase ?? 'empty',
      players: room.state?.players?.map(p => p.name) ?? [],
      connections: room.conns.size,
      pendingJoins: room.pendingJoins.length,
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ totalRooms: summary.length, rooms: summary }, null, 2));
    return;
  }

  // Delete a specific room (also closes all its WebSocket connections)
  const deleteMatch = url.pathname.match(/^\/rooms\/delete\/([^/]+)$/);
  if (deleteMatch) {
    const code = deleteMatch[1].toLowerCase();
    const room = rooms[code];
    if (!room) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Room not found', room: code }));
      return;
    }
    // Close all active WebSocket connections in this room
    for (const conn of room.conns) {
      try { conn.close(1000, 'Room deleted by admin'); } catch (e) {}
    }
    delete rooms[code];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ deleted: true, room: code }));
    return;
  }

  const match = url.pathname.match(/^\/parties\/main\/([^/]+)$/);
  if (match) {
    const room = rooms[match[1]];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      room: match[1],
      phase: room?.state?.phase ?? 'empty',
      players: room?.state?.players?.length ?? 0,
    }));
  } else {
    res.writeHead(200);
    res.end('CABO server running');
  }
});

// WebSocket server
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (conn, req) => {
  const match = req.url?.match(/^\/parties\/main\/([^/?]+)/);
  if (!match) { conn.close(); return; }

  const roomCode = match[1].toLowerCase();
  const room = getRoom(roomCode);
  room.conns.add(conn);

  // Send current state immediately on connect
  if (room.state) {
    conn.send(JSON.stringify({ type: 'state', state: room.state }));
  }

  conn.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    switch (msg.type) {
      case 'hello':
        break;

      case 'join': {
        const { playerId, playerName } = msg;
        if (!room.state) {
          room.pendingJoins.push({ playerId, playerName });
          conn.send(JSON.stringify({ type: 'join_queued' }));
          break;
        }
        applyJoin(room, playerId, playerName, conn);
        break;
      }

      case 'setState': {
        room.state = msg.state;
        if (room.pendingJoins.length > 0) {
          for (const { playerId, playerName } of room.pendingJoins) {
            applyJoin(room, playerId, playerName, null);
          }
          room.pendingJoins = [];
        }
        broadcast(room, { type: 'state', state: room.state });
        break;
      }
    }
  });

  conn.on('close', () => {
    room.conns.delete(conn);
    if (room.conns.size === 0) {
      setTimeout(() => {
        if (rooms[roomCode]?.conns?.size === 0) delete rooms[roomCode];
      }, 3600_000);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`CABO server listening on port ${PORT}`);
});
