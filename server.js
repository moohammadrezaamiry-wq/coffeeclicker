const express = require('express');
const http = require('http').createServer(app);
const { Server } = require('socket.io');
const Redis = require('ioredis');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// ========== اتصال به Redis ==========
const redis = new Redis(REDIS_URL);

redis.on('connect', () => console.log('✅ متصل به Redis'));
redis.on('error', (err) => console.error('❌ خطا در Redis:', err));

// ========== سرویس فایل‌های استاتیک ==========
app.use(express.static('public'));

// ========== Socket.io ==========
io.on('connection', (socket) => {
  console.log('🔵 کاربر متصل:', socket.id);

  // ---- ۱. ثبت/ورود با Device ID ----
  socket.on('register', async ({ deviceId, username }) => {
    username = username.trim();
    if (!username || username.length < 2) {
      socket.emit('registerError', 'نام کاربری باید حداقل ۲ کاراکتر باشد.');
      return;
    }

    // بررسی تکراری بودن نام
    const existingDevice = await redis.hgetall('devices');
    for (const [devId, user] of Object.entries(existingDevice)) {
      if (user === username && devId !== deviceId) {
        socket.emit('registerError', 'این نام قبلاً توسط دستگاه دیگری ثبت شده است.');
        return;
      }
    }

    // ذخیره Device ID → Username
    await redis.hset('devices', deviceId, username);

    // اگر کاربر در لیدربورد نیست، اضافه کن
    const userScore = await redis.zscore('leaderboard', username);
    if (userScore === null) {
      await redis.zadd('leaderboard', 0, username);
    }

    socket.username = username;
    socket.deviceId = deviceId;

    // ارسال اطلاعات کاربر
    const clicks = await redis.zscore('leaderboard', username) || 0;
    socket.emit('registerSuccess', { username, deviceId, clicks: parseInt(clicks) });

    // پخش لیدربورد به همه
    sendLeaderboard();
  });

  // ---- ۲. شناسایی دستگاه ----
  socket.on('identify', async ({ deviceId }) => {
    const username = await redis.hget('devices', deviceId);
    if (username) {
      socket.username = username;
      socket.deviceId = deviceId;
      const clicks = await redis.zscore('leaderboard', username) || 0;
      socket.emit('identity', { username, deviceId, clicks: parseInt(clicks) });
    } else {
      socket.emit('identity', null);
    }
  });

  // ---- ۳. اضافه کردن کلیک ----
  socket.on('addClicks', async ({ count }) => {
    if (!socket.username) return;
    const newScore = await redis.zincrby('leaderboard', count, socket.username);
    // ارسال لیدربورد به‌روز به همه
    sendLeaderboard();
    // ارسال امتیاز جدید به خود کاربر
    socket.emit('scoreUpdate', { clicks: parseInt(newScore) });
  });

  // ---- ۴. دریافت لیدربورد ----
  socket.on('getLeaderboard', () => {
    sendLeaderboard(socket);
  });

  // ---- ۵. دریافت اطلاعات کاربر ----
  socket.on('getUserData', async () => {
    if (!socket.username) return;
    const clicks = await redis.zscore('leaderboard', socket.username) || 0;
    const rank = await redis.zrevrank('leaderboard', socket.username);
    const total = await redis.zcard('leaderboard');
    socket.emit('userData', {
      username: socket.username,
      clicks: parseInt(clicks),
      rank: rank !== null ? rank + 1 : '-',
      total: total
    });
  });

  // ---- ۶. قطع اتصال ----
  socket.on('disconnect', () => {
    console.log('🔴 کاربر قطع شد:', socket.id);
  });
});

// ========== تابع ارسال لیدربورد ==========
async function sendLeaderboard(targetSocket = null) {
  const top = await redis.zrevrange('leaderboard', 0, 9, 'WITHSCORES');
  const leaderboard = [];
  for (let i = 0; i < top.length; i += 2) {
    leaderboard.push({
      username: top[i],
      clicks: parseInt(top[i + 1])
    });
  }

  const data = { leaderboard };
  if (targetSocket) {
    targetSocket.emit('leaderboardData', data);
  } else {
    io.emit('leaderboardData', data);
  }
}

// ========== اجرای سرور ==========
server.listen(PORT, () => {
  console.log(`🚀 سرور روی پورت ${PORT} اجرا شد`);
  console.log(`🔗 آدرس: http://localhost:${PORT}`);
});
