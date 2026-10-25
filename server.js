const express = require('express'), http = require('http'), crypto = require('crypto');
const fs = require('fs'), path = require('path'), { Server } = require('socket.io');
const app = express(), server = http.createServer(app), io = new Server(server);
app.use(express.static(path.join(__dirname, 'public')));

// ---- tiny JSON "database" ----
const FILE = path.join(__dirname, 'data.json');
let db = { users: {}, messages: [], sessions: {} };
try { db = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch {}
let timer; const save = () => { clearTimeout(timer); timer = setTimeout(() => fs.writeFileSync(FILE, JSON.stringify(db)), 200); };
const hash = (p, salt) => crypto.scryptSync(p, salt, 32).toString('hex');
const online = {}; // username -> Set(socket.id)
const isOnline = u => !!(online[u] && online[u].size);

io.on('connection', socket => {
  let me = null, count = 0;
  setInterval(() => count = 0, 10000).unref();
  const limited = () => ++count > 40;

  function login(u) {
    me = u; socket.join('u:' + u);
    (online[u] ||= new Set()).add(socket.id);
    // anything sent to me while offline is now "delivered"
    const ids = {};
    db.messages.forEach(m => { if (m.to === u && m.status === 'sent') { m.status = 'delivered'; (ids[m.from] ||= []).push(m.id); } });
    Object.entries(ids).forEach(([from, list]) => io.to('u:' + from).emit('status', { ids: list, status: 'delivered' }));
    save(); io.emit('presence', { user: u, online: true });
  }

  socket.on('auth', ({ mode, username, password, token }, ack) => {
    if (token && db.sessions[token]) { login(db.sessions[token]); return ack({ ok: true, username: me, token }); }
    username = String(username || '').trim().toLowerCase();
    if (!/^[a-z0-9_]{2,20}$/.test(username) || String(password || '').length < 4)
      return ack({ ok: false, error: 'Username: 2-20 letters/numbers. Password: 4+ chars.' });
    if (mode === 'signup') {
      if (db.users[username]) return ack({ ok: false, error: 'Username taken' });
      const salt = crypto.randomBytes(16).toString('hex');
      db.users[username] = { salt, hash: hash(password, salt), lastSeen: Date.now() };
    } else {
      const u = db.users[username];
      if (!u || u.hash !== hash(password, u.salt)) return ack({ ok: false, error: 'Wrong username or password' });
    }
    const t = crypto.randomBytes(24).toString('hex'); db.sessions[t] = username; save();
    login(username); ack({ ok: true, username, token: t });
  });

  socket.on('init', ack => {
    if (!me) return;
    const users = Object.keys(db.users).filter(u => u !== me).map(u => {
      const conv = db.messages.filter(m => (m.from === me && m.to === u) || (m.from === u && m.to === me));
      return { user: u, online: isOnline(u), lastSeen: db.users[u].lastSeen, last: conv[conv.length - 1] || null,
               unread: conv.filter(m => m.to === me && m.status !== 'read').length };
    });
    ack(users);
  });

  socket.on('history', ({ with: peer, before }, ack) => {
    if (!me) return;
    let conv = db.messages.filter(m => (m.from === me && m.to === peer) || (m.from === peer && m.to === me));
    if (before) conv = conv.filter(m => m.ts < before);
    ack(conv.slice(-50));
  });

  socket.on('send', ({ id, to, text }, ack) => {
    if (!me || limited() || !db.users[to]) return ack && ack({ ok: false });
    text = String(text || '').slice(0, 2000).trim(); if (!text) return;
    let m = db.messages.find(x => x.id === id && x.from === me);   // dedupe retries
    if (!m) {
      m = { id: String(id).slice(0, 40), from: me, to, text, ts: Date.now(), status: 'sent' };
      if (isOnline(to)) m.status = 'delivered';
      db.messages.push(m); save();
      io.to('u:' + to).emit('message', m);
    }
    ack && ack({ ok: true, msg: m });
  });

  socket.on('read', ({ with: peer }) => {
    if (!me) return;
    const ids = [];
    db.messages.forEach(m => { if (m.from === peer && m.to === me && m.status !== 'read') { m.status = 'read'; ids.push(m.id); } });
    if (ids.length) { save(); io.to('u:' + peer).emit('status', { ids, status: 'read', at: Date.now() }); }
  });

  socket.on('typing', ({ to, on }) => { if (me && !limited()) io.to('u:' + to).emit('typing', { from: me, on: !!on }); });

  socket.on('disconnect', () => {
    if (!me) return;
    online[me].delete(socket.id);
    if (!isOnline(me)) { db.users[me].lastSeen = Date.now(); save(); io.emit('presence', { user: me, online: false, lastSeen: db.users[me].lastSeen }); }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chat running at http://localhost:${PORT}`));
