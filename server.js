// server.js - Classroom Gallery WebSocket Backend
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

const rooms = {};
const participants = {};

app.get('/api/sessions/:roomId', (req, res) => {
  const { roomId } = req.params;
  if (rooms[roomId]) {
    res.json({
      roomId,
      createdAt: rooms[roomId].createdAt,
      participants: Object.values(rooms[roomId].participants || {}),
      messageCount: rooms[roomId].messages?.length || 0,
    });
  } else {
    res.status(404).json({ error: 'Session not found' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

wss.on('connection', (ws) => {
  console.log('New WebSocket connection');
  let participantId = null;
  let roomId = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'join') {
        participantId = data.participantId;
        roomId = data.roomId;

        if (!rooms[roomId]) {
          rooms[roomId] = {
            createdAt: new Date().toISOString(),
            messages: [],
            participants: {},
          };
        }

        participants[participantId] = {
          participantId,
          peerId: data.peerId,
          name: data.name,
          roomId,
          joinedAt: new Date().toISOString(),
          handRaised: false,
          cameraOn: true,
          micOn: true,
        };

        rooms[roomId].participants[participantId] = participants[participantId];
        console.log(`${data.name} joined room ${roomId}`);

        broadcastToRoom(roomId, {
          type: 'participant-joined',
          participant: participants[participantId],
          totalParticipants: Object.keys(rooms[roomId].participants).length,
        });

        ws.send(JSON.stringify({
          type: 'session-state',
          participants: Object.values(rooms[roomId].participants),
          messages: rooms[roomId].messages,
        }));
      }

      if (data.type === 'state-update') {
        if (participants[participantId]) {
          participants[participantId] = {
            ...participants[participantId],
            ...data.state,
          };
          broadcastToRoom(roomId, {
            type: 'participant-state-changed',
            participant: participants[participantId],
          });
        }
      }

      if (data.type === 'chat-message') {
        const msg = {
          id: Date.now(),
          participantId,
          name: data.name,
          text: data.text,
          timestamp: new Date().toISOString(),
        };
        rooms[roomId].messages.push(msg);
        broadcastToRoom(roomId, {
          type: 'chat-message',
          message: msg,
        });
      }

      if (data.type === 'screen-share-start') {
        broadcastToRoom(roomId, {
          type: 'screen-share-started',
          participantId,
          peerId: data.peerId,
        });
      }

      if (data.type === 'screen-share-stop') {
        broadcastToRoom(roomId, {
          type: 'screen-share-stopped',
          participantId,
        });
      }

      if (data.type === 'leave') {
        if (participants[participantId]) {
          delete participants[participantId];
          if (rooms[roomId]) {
            delete rooms[roomId].participants[participantId];
            broadcastToRoom(roomId, {
              type: 'participant-left',
              participantId,
              totalParticipants: Object.keys(rooms[roomId].participants).length,
            });
          }
        }
      }
    } catch (err) {
      console.error('Message error:', err);
    }
  });

  ws.on('close', () => {
    if (participantId && roomId) {
      if (participants[participantId]) {
        delete participants[participantId];
      }
      if (rooms[roomId]) {
        delete rooms[roomId].participants[participantId];
        broadcastToRoom(roomId, {
          type: 'participant-left',
          participantId,
          totalParticipants: Object.keys(rooms[roomId].participants).length,
        });
      }
    }
    console.log('WebSocket connection closed');
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

function broadcastToRoom(room, data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`✓ Classroom Gallery backend started`);
  console.log(`✓ Listening on port ${PORT}`);
  console.log(`✓ Health check: GET http://0.0.0.0:${PORT}/api/health`);
  console.log(`✓ WebSocket endpoint: ws://0.0.0.0:${PORT}`);
}).on('error', (err) => {
  console.error('✗ Server error:', err);
  process.exit(1);
});
