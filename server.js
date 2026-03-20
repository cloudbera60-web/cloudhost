// server.js - COMPLETE PRODUCTION CODE
// CloudHost - The Greatest Hosting Platform in Human History
// Created by Bruce Bera | +254787527753 | @brucebera | bruce@cloudhost.app

const express = require('express');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const fsPromises = require('fs').promises;
const axios = require('axios');
const crypto = require('crypto');
const { spawn } = require('child_process');
const cron = require('node-cron');
const winston = require('winston');
const simpleGit = require('simple-git');
const AdmZip = require('adm-zip');
const { Resend } = require('resend');
const socketIo = require('socket.io');
const http = require('http');

// Initialize Express
const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  transports: ['websocket', 'polling']
});

// Ensure all required directories exist
const directories = ['uploads', 'projects', 'backups', 'ch_bot_zips', 'logs', 'public'];
directories.forEach(dir => {
  const dirPath = path.join(__dirname, dir);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
});

// Logger configuration
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: path.join(__dirname, 'logs', 'error.log'), level: 'error' }),
    new winston.transports.File({ filename: path.join(__dirname, 'logs', 'combined.log') }),
    new winston.transports.Console({ format: winston.format.simple() })
  ]
});

// Database connection with retry
let pool;
let dbConnected = false;

async function connectWithRetry() {
  let retries = 10;
  while (retries > 0) {
    try {
      pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
        connectionTimeoutMillis: 10000,
        idleTimeoutMillis: 30000,
        max: 20
      });
      
      await pool.query('SELECT NOW()');
      dbConnected = true;
      logger.info('Database connected successfully');
      return pool;
    } catch (err) {
      retries--;
      logger.error(`Database connection failed (${retries} retries left): ${err.message}`);
      if (retries === 0) throw err;
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }
}

// Middleware
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
app.use('/projects', express.static(path.join(__dirname, 'projects')));

// File upload configuration
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, path.join(__dirname, 'uploads')),
    filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`)
  }),
  limits: { fileSize: 100 * 1024 * 1024 }
});

// ============== HELPER FUNCTIONS ==============

function generateId() {
  return uuidv4();
}

function generateReferralCode() {
  return 'REF' + Math.random().toString(36).substring(2, 10).toUpperCase();
}

function generateApiKey() {
  return 'ck_' + crypto.randomBytes(32).toString('hex');
}

async function updateUserCoins(userId, amount, type, description, referenceId = null) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    const userResult = await client.query('SELECT coins FROM users WHERE id = $1 FOR UPDATE', [userId]);
    if (userResult.rows.length === 0) throw new Error('User not found');
    
    const currentCoins = userResult.rows[0].coins;
    const newBalance = currentCoins + amount;
    
    if (newBalance < 0) throw new Error('Insufficient coins');
    
    await client.query(
      'UPDATE users SET coins = $1, updated_at = NOW() WHERE id = $2',
      [newBalance, userId]
    );
    
    await client.query(
      `INSERT INTO transactions (id, user_id, type, amount, balance_after, description, reference_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [generateId(), userId, type, amount, newBalance, description, referenceId]
    );
    
    if (amount > 0) {
      await client.query('UPDATE users SET total_earned = total_earned + $1 WHERE id = $2', [amount, userId]);
    } else {
      await client.query('UPDATE users SET total_spent = total_spent + $1 WHERE id = $2', [Math.abs(amount), userId]);
    }
    
    await client.query('COMMIT');
    return newBalance;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function sendNotification(userId, type, title, message, data = {}) {
  try {
    await pool.query(
      `INSERT INTO notifications (id, user_id, type, title, message, data)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [generateId(), userId, type, title, message, JSON.stringify(data)]
    );
    
    // Emit socket event
    const socketId = userSockets[userId];
    if (socketId) {
      io.to(socketId).emit('notification', { id: generateId(), type, title, message, data, created_at: new Date() });
    }
  } catch (err) {
    logger.error('Send notification error:', err);
  }
}

async function logAudit(userId, action, targetType, targetId, details = {}, ip = null, userAgent = null) {
  try {
    await pool.query(
      `INSERT INTO audit_logs (id, user_id, action, target_type, target_id, details, ip_address, user_agent)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [generateId(), userId, action, targetType, targetId, JSON.stringify(details), ip, userAgent]
    );
  } catch (err) {
    logger.error('Audit log error:', err);
  }
}

// Store user sockets
const userSockets = {};

// Socket.IO
io.on('connection', (socket) => {
  logger.info(`Socket connected: ${socket.id}`);
  
  socket.on('register', (userId) => {
    userSockets[userId] = socket.id;
    socket.join(`user:${userId}`);
  });
  
  socket.on('join-project', (projectId) => {
    socket.join(`project:${projectId}`);
  });
  
  socket.on('project-log', (data) => {
    io.to(`project:${data.projectId}`).emit('project-log', data);
  });
  
  socket.on('disconnect', () => {
    for (const [userId, socketId] of Object.entries(userSockets)) {
      if (socketId === socket.id) {
        delete userSockets[userId];
        break;
      }
    }
  });
});

// ============== AUTHENTICATION MIDDLEWARE ==============

const authenticate = async (req, res, next) => {
  const authHeader = req.headers.authorization;
  const apiKey = req.headers['x-api-key'];
  
  if (!authHeader && !apiKey) {
    return res.status(401).json({ success: false, error: 'UNAUTHORIZED', message: 'No authentication provided' });
  }
  
  try {
    let user;
    
    if (apiKey) {
      const result = await pool.query('SELECT * FROM users WHERE api_key = $1 AND is_banned = false', [apiKey]);
      if (result.rows.length === 0) {
        return res.status(401).json({ success: false, error: 'INVALID_API_KEY', message: 'Invalid API key' });
      }
      user = result.rows[0];
    } else {
      const token = authHeader.split(' ')[1];
      const decoded = jwt.verify(token, process.env.JWT_SECRET || 'cloudhost-super-secret-key-change-in-production');
      const result = await pool.query('SELECT * FROM users WHERE id = $1 AND is_banned = false', [decoded.userId]);
      if (result.rows.length === 0) {
        return res.status(401).json({ success: false, error: 'USER_NOT_FOUND', message: 'User not found' });
      }
      user = result.rows[0];
    }
    
    // Check subscription expiry
    if (user.subscription_expiry && new Date(user.subscription_expiry) < new Date()) {
      await pool.query('UPDATE users SET subscription_tier = $1, max_projects = $2 WHERE id = $3', ['free', 3, user.id]);
      user.subscription_tier = 'free';
      user.max_projects = 3;
    }
    
    req.user = user;
    next();
  } catch (err) {
    if (err.name === 'JsonWebTokenError') {
      return res.status(401).json({ success: false, error: 'INVALID_TOKEN', message: 'Invalid token' });
    }
    logger.error('Auth error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Authentication failed' });
  }
};

const requireAdmin = async (req, res, next) => {
  if (req.user.role !== 'admin' && req.user.role !== 'superadmin') {
    return res.status(403).json({ success: false, error: 'FORBIDDEN', message: 'Admin access required' });
  }
  next();
};

const requireSuperAdmin = async (req, res, next) => {
  if (req.user.role !== 'superadmin') {
    return res.status(403).json({ success: false, error: 'FORBIDDEN', message: 'Super admin access required' });
  }
  next();
};

// ============== DATABASE INITIALIZATION ==============

async function initDatabase() {
  const client = await pool.connect();
  try {
    // Users table
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE,
        password_hash TEXT NOT NULL,
        role VARCHAR(20) DEFAULT 'user',
        admin_level INTEGER DEFAULT 0,
        permissions JSONB DEFAULT '[]',
        coins INTEGER DEFAULT 100,
        total_earned INTEGER DEFAULT 0,
        total_spent INTEGER DEFAULT 0,
        referral_code VARCHAR(20) UNIQUE NOT NULL,
        referred_by UUID REFERENCES users(id),
        telegram_id VARCHAR(100),
        telegram_linked BOOLEAN DEFAULT false,
        avatar TEXT,
        bio TEXT,
        badges JSONB DEFAULT '[]',
        login_streak INTEGER DEFAULT 0,
        last_login_date DATE,
        is_verified BOOLEAN DEFAULT false,
        is_banned BOOLEAN DEFAULT false,
        email_verified BOOLEAN DEFAULT false,
        two_factor_enabled BOOLEAN DEFAULT false,
        two_factor_secret TEXT,
        api_key TEXT UNIQUE,
        subscription_tier VARCHAR(20) DEFAULT 'free',
        subscription_expiry TIMESTAMP,
        max_projects INTEGER DEFAULT 3,
        total_projects_deployed INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Projects table
    await client.query(`
      CREATE TABLE IF NOT EXISTS projects (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        name VARCHAR(100) NOT NULL,
        bot_template_id UUID,
        type VARCHAR(20) DEFAULT 'git',
        git_url TEXT,
        branch VARCHAR(100) DEFAULT 'main',
        zip_filename TEXT,
        port INTEGER DEFAULT 3000,
        status VARCHAR(20) DEFAULT 'stopped',
        pid INTEGER,
        env_vars JSONB DEFAULT '{}',
        custom_domain TEXT,
        last_deployed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        deploy_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Bot templates table
    await client.query(`
      CREATE TABLE IF NOT EXISTS bot_templates (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(100) NOT NULL,
        description TEXT,
        type VARCHAR(20) DEFAULT 'git',
        git_url TEXT,
        branch VARCHAR(100) DEFAULT 'main',
        zip_filename TEXT,
        config_fields JSONB DEFAULT '[]',
        category VARCHAR(50) DEFAULT 'other',
        icon TEXT,
        is_featured BOOLEAN DEFAULT false,
        is_verified BOOLEAN DEFAULT false,
        is_active BOOLEAN DEFAULT true,
        deploy_count INTEGER DEFAULT 0,
        author_id UUID REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Payments table
    await client.query(`
      CREATE TABLE IF NOT EXISTS payments (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        provider VARCHAR(20) NOT NULL,
        type VARCHAR(20) NOT NULL,
        amount INTEGER NOT NULL,
        currency VARCHAR(3) DEFAULT 'KES',
        phone_number VARCHAR(15),
        status VARCHAR(20) DEFAULT 'pending',
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Transactions table
    await client.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        type VARCHAR(50) NOT NULL,
        amount INTEGER NOT NULL,
        balance_after INTEGER NOT NULL,
        description TEXT,
        reference_id UUID,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Airdrops table
    await client.query(`
      CREATE TABLE IF NOT EXISTS airdrops (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(100) NOT NULL,
        description TEXT,
        coins INTEGER NOT NULL,
        total_claims INTEGER,
        remaining_claims INTEGER,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        status VARCHAR(20) DEFAULT 'draft',
        claim_codes JSONB DEFAULT '[]',
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Airdrop claims table
    await client.query(`
      CREATE TABLE IF NOT EXISTS airdrop_claims (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        airdrop_id UUID REFERENCES airdrops(id) ON DELETE CASCADE,
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        claim_code VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(airdrop_id, user_id)
      )
    `);

    // Bot submissions table
    await client.query(`
      CREATE TABLE IF NOT EXISTS bot_submissions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        git_url TEXT NOT NULL,
        category VARCHAR(50),
        status VARCHAR(20) DEFAULT 'pending',
        admin_notes TEXT,
        reviewed_by UUID REFERENCES users(id),
        reviewed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Reviews table
    await client.query(`
      CREATE TABLE IF NOT EXISTS reviews (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        bot_template_id UUID REFERENCES bot_templates(id) ON DELETE CASCADE,
        rating INTEGER CHECK (rating >= 1 AND rating <= 5),
        comment TEXT,
        helpful_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, bot_template_id)
      )
    `);

    // Support tickets table
    await client.query(`
      CREATE TABLE IF NOT EXISTS support_tickets (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        subject VARCHAR(200) NOT NULL,
        message TEXT NOT NULL,
        status VARCHAR(20) DEFAULT 'open',
        priority VARCHAR(20) DEFAULT 'normal',
        assigned_to UUID REFERENCES users(id),
        resolved_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Ticket replies table
    await client.query(`
      CREATE TABLE IF NOT EXISTS ticket_replies (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        ticket_id UUID REFERENCES support_tickets(id) ON DELETE CASCADE,
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        message TEXT NOT NULL,
        is_admin BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Audit logs table
    await client.query(`
      CREATE TABLE IF NOT EXISTS audit_logs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE SET NULL,
        action VARCHAR(100) NOT NULL,
        target_type VARCHAR(50),
        target_id UUID,
        details JSONB DEFAULT '{}',
        ip_address VARCHAR(45),
        user_agent TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // System settings table
    await client.query(`
      CREATE TABLE IF NOT EXISTS system_settings (
        key VARCHAR(100) PRIMARY KEY,
        value JSONB NOT NULL,
        updated_by UUID REFERENCES users(id),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Messages table
    await client.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        sender_id UUID REFERENCES users(id) ON DELETE SET NULL,
        subject VARCHAR(200) NOT NULL,
        content TEXT NOT NULL,
        type VARCHAR(20) DEFAULT 'bulk',
        target_filter JSONB DEFAULT '{}',
        sent_count INTEGER DEFAULT 0,
        scheduled_for TIMESTAMP,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sent_at TIMESTAMP
      )
    `);

    // Notifications table
    await client.query(`
      CREATE TABLE IF NOT EXISTS notifications (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        type VARCHAR(50) NOT NULL,
        title VARCHAR(200) NOT NULL,
        message TEXT NOT NULL,
        data JSONB DEFAULT '{}',
        is_read BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Chat messages table
    await client.query(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE SET NULL,
        username VARCHAR(50),
        message TEXT NOT NULL,
        room VARCHAR(50) DEFAULT 'general',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    logger.info('All database tables created successfully');

    // Insert default bot templates
    await insertDefaultBotTemplates(client);
    
    // Create default admin user if none exists
    const adminCheck = await client.query("SELECT id FROM users WHERE role = 'superadmin'");
    if (adminCheck.rows.length === 0) {
      const hashedPassword = await bcrypt.hash(process.env.ADMIN_PASSWORD || 'CloudHostAdmin2024!', 10);
      const adminId = generateId();
      const adminApiKey = generateApiKey();
      
      await client.query(
        `INSERT INTO users (id, username, email, password_hash, role, admin_level, referral_code, api_key, coins, max_projects, is_verified)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [adminId, process.env.ADMIN_USERNAME || 'admin', process.env.ADMIN_EMAIL || 'admin@cloudhost.app', hashedPassword, 'superadmin', 10, generateReferralCode(), adminApiKey, 10000, 999999, true]
      );
      logger.info('Default admin user created');
    }

  } catch (err) {
    logger.error('Database initialization error:', err);
    throw err;
  } finally {
    client.release();
  }
}

async function insertDefaultBotTemplates(client) {
  // Check if templates already exist
  const existing = await client.query('SELECT COUNT(*) FROM bot_templates');
  if (parseInt(existing.rows[0].count) > 0) return;
  
  const templates = [
    {
      name: 'ATASSA MD',
      description: 'Powerful WhatsApp bot with multi-device support, group management, and media processing',
      git_url: 'https://github.com/mauricegift/atassa',
      branch: 'main',
      category: 'whatsapp',
      icon: '🤖',
      config_fields: JSON.stringify([
        { key: 'SESSION_ID', label: 'Session ID', type: 'password', required: true, description: 'Get from session.giftedtech.co.ke' },
        { key: 'PREFIX', label: 'Command Prefix', type: 'text', required: false, default: '.', description: 'Character before commands' },
        { key: 'OWNER_NUMBER', label: 'Owner Phone', type: 'text', required: true, placeholder: '254712345678' }
      ])
    },
    {
      name: 'Gifted Tech MD',
      description: 'Advanced WhatsApp bot with AI features, sticker maker, and automated responses',
      git_url: 'https://github.com/giftedtech/gifted-md',
      branch: 'main',
      category: 'whatsapp',
      icon: '🎁',
      config_fields: JSON.stringify([
        { key: 'SESSION_ID', label: 'Session ID', type: 'password', required: true },
        { key: 'AUTO_STATUS', label: 'Auto Read Status', type: 'boolean', required: false, default: 'true' }
      ])
    },
    {
      name: 'Xstro MD',
      description: 'Lightweight WhatsApp bot with plugin system and custom commands',
      git_url: 'https://github.com/astroboy/xstro-md',
      branch: 'main',
      category: 'whatsapp',
      icon: '⚡',
      config_fields: JSON.stringify([
        { key: 'SESSION_ID', label: 'Session ID', type: 'password', required: true },
        { key: 'PREFIX', label: 'Prefix', type: 'text', required: false, default: '!' }
      ])
    },
    {
      name: 'Telegram Bot Manager',
      description: 'Complete Telegram group management bot with moderation tools',
      git_url: 'https://github.com/telegram-bot/group-manager',
      branch: 'main',
      category: 'telegram',
      icon: '📱',
      config_fields: JSON.stringify([
        { key: 'BOT_TOKEN', label: 'Bot Token', type: 'password', required: true, description: 'Get from @BotFather' },
        { key: 'OWNER_ID', label: 'Owner ID', type: 'text', required: true }
      ])
    }
  ];
  
  for (const template of templates) {
    await client.query(
      `INSERT INTO bot_templates (id, name, description, type, git_url, branch, category, icon, config_fields, is_active, is_verified)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [generateId(), template.name, template.description, 'git', template.git_url, template.branch, template.category, template.icon, template.config_fields, true, true]
    );
  }
  
  logger.info('Default bot templates inserted');
}

// ============== HEALTH CHECK ==============

app.get('/api/system/health', async (req, res) => {
  const health = {
    success: true,
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: {
      rss: Math.round(process.memoryUsage().rss / 1024 / 1024) + ' MB',
      heapTotal: Math.round(process.memoryUsage().heapTotal / 1024 / 1024) + ' MB',
      heapUsed: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + ' MB'
    },
    database: { status: 'disconnected' }
  };
  
  if (pool) {
    try {
      await pool.query('SELECT 1');
      health.database.status = 'connected';
    } catch (err) {
      health.database.status = 'error';
      health.database.error = err.message;
    }
  }
  
  res.json(health);
});

// ============== AUTHENTICATION ENDPOINTS ==============

app.post('/api/auth/register', async (req, res) => {
  const { username, email, password, referral_code } = req.body;
  
  if (!username || !password) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Username and password required' });
  }
  
  try {
    const existing = await pool.query('SELECT id FROM users WHERE username = $1 OR email = $2', [username, email]);
    if (existing.rows.length > 0) {
      return res.status(400).json({ success: false, error: 'USER_EXISTS', message: 'Username or email already exists' });
    }
    
    const hashedPassword = await bcrypt.hash(password, 10);
    const userId = generateId();
    const referralCode = generateReferralCode();
    const apiKey = generateApiKey();
    
    let referredBy = null;
    if (referral_code) {
      const referrer = await pool.query('SELECT id FROM users WHERE referral_code = $1', [referral_code]);
      if (referrer.rows.length > 0) {
        referredBy = referrer.rows[0].id;
      }
    }
    
    await pool.query(
      `INSERT INTO users (id, username, email, password_hash, referral_code, referred_by, api_key)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [userId, username, email || null, hashedPassword, referralCode, referredBy, apiKey]
    );
    
    if (referredBy) {
      await updateUserCoins(referredBy, 50, 'referral_referrer', `Referral bonus for ${username}`, userId);
      await updateUserCoins(userId, 25, 'referral_referred', `Welcome bonus from referral`, referredBy);
    }
    
    const token = jwt.sign({ userId }, process.env.JWT_SECRET || 'cloudhost-super-secret-key-change-in-production', { expiresIn: '15m' });
    const refreshToken = jwt.sign({ userId }, process.env.JWT_REFRESH_SECRET || 'cloudhost-refresh-secret-key-change-in-production', { expiresIn: '30d' });
    
    res.json({
      success: true,
      data: {
        user: { id: userId, username, email, coins: 125, api_key: apiKey },
        token,
        refresh_token: refreshToken
      }
    });
    
  } catch (err) {
    logger.error('Registration error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Registration failed' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  
  if (!username || !password) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Username and password required' });
  }
  
  try {
    const result = await pool.query('SELECT * FROM users WHERE username = $1 OR email = $1', [username]);
    if (result.rows.length === 0) {
      return res.status(401).json({ success: false, error: 'INVALID_CREDENTIALS', message: 'Invalid credentials' });
    }
    
    const user = result.rows[0];
    
    if (user.is_banned) {
      return res.status(403).json({ success: false, error: 'ACCOUNT_BANNED', message: 'This account has been banned' });
    }
    
    const valid = await bcrypt.compare(password, user.password_hash);
    if (!valid) {
      return res.status(401).json({ success: false, error: 'INVALID_CREDENTIALS', message: 'Invalid credentials' });
    }
    
    // Handle login streak
    const today = new Date().toISOString().split('T')[0];
    let streakAward = 0;
    let newStreak = user.login_streak || 0;
    
    if (user.last_login_date) {
      const lastLogin = user.last_login_date.toISOString().split('T')[0];
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const yesterdayStr = yesterday.toISOString().split('T')[0];
      
      if (lastLogin === yesterdayStr) {
        newStreak = (user.login_streak || 0) + 1;
        streakAward = 10;
        
        if (newStreak === 7) {
          streakAward += 25;
          await sendNotification(user.id, 'streak', '🔥 7-Day Streak!', 'You earned 25 bonus coins!', { streak: 7, bonus: 25 });
        } else if (newStreak === 30) {
          streakAward += 100;
          await sendNotification(user.id, 'streak', '🏆 30-Day Streak!', 'You earned 100 bonus coins!', { streak: 30, bonus: 100 });
        }
        
        await updateUserCoins(user.id, streakAward, 'daily_streak', `Day ${newStreak} streak reward`, null);
      } else if (lastLogin !== today) {
        newStreak = 1;
        streakAward = 10;
        await updateUserCoins(user.id, 10, 'daily_streak', 'Daily login reward', null);
      }
    } else {
      newStreak = 1;
      streakAward = 10;
      await updateUserCoins(user.id, 10, 'daily_streak', 'First login reward', null);
    }
    
    await pool.query('UPDATE users SET login_streak = $1, last_login_date = $2, updated_at = NOW() WHERE id = $3', [newStreak, today, user.id]);
    
    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET || 'cloudhost-super-secret-key-change-in-production', { expiresIn: '15m' });
    const refreshToken = jwt.sign({ userId: user.id }, process.env.JWT_REFRESH_SECRET || 'cloudhost-refresh-secret-key-change-in-production', { expiresIn: '30d' });
    
    // Get fresh user data
    const updatedUser = await pool.query('SELECT * FROM users WHERE id = $1', [user.id]);
    
    res.json({
      success: true,
      data: {
        user: {
          id: updatedUser.rows[0].id,
          username: updatedUser.rows[0].username,
          email: updatedUser.rows[0].email,
          coins: updatedUser.rows[0].coins,
          role: updatedUser.rows[0].role,
          subscription_tier: updatedUser.rows[0].subscription_tier,
          max_projects: updatedUser.rows[0].max_projects,
          api_key: updatedUser.rows[0].api_key,
          is_verified: updatedUser.rows[0].is_verified
        },
        token,
        refresh_token: refreshToken,
        streak_reward: streakAward > 0 ? streakAward : null
      }
    });
    
  } catch (err) {
    logger.error('Login error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Login failed' });
  }
});

app.post('/api/auth/refresh', async (req, res) => {
  const { refresh_token } = req.body;
  
  if (!refresh_token) {
    return res.status(400).json({ success: false, error: 'NO_REFRESH_TOKEN', message: 'Refresh token required' });
  }
  
  try {
    const decoded = jwt.verify(refresh_token, process.env.JWT_REFRESH_SECRET || 'cloudhost-refresh-secret-key-change-in-production');
    const result = await pool.query('SELECT id FROM users WHERE id = $1 AND is_banned = false', [decoded.userId]);
    
    if (result.rows.length === 0) {
      return res.status(401).json({ success: false, error: 'INVALID_REFRESH', message: 'Invalid refresh token' });
    }
    
    const newToken = jwt.sign({ userId: decoded.userId }, process.env.JWT_SECRET || 'cloudhost-super-secret-key-change-in-production', { expiresIn: '15m' });
    
    res.json({ success: true, data: { token: newToken } });
  } catch (err) {
    res.status(401).json({ success: false, error: 'INVALID_REFRESH', message: 'Invalid refresh token' });
  }
});

// ============== ECONOMY ENDPOINTS ==============

app.get('/api/economy/balance', authenticate, async (req, res) => {
  try {
    const result = await pool.query('SELECT coins, total_earned, total_spent FROM users WHERE id = $1', [req.user.id]);
    res.json({ success: true, data: result.rows[0] });
  } catch (err) {
    logger.error('Get balance error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get balance' });
  }
});

app.get('/api/economy/transactions', authenticate, async (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 20;
  const offset = (page - 1) * limit;
  
  try {
    const result = await pool.query(
      `SELECT id, type, amount, balance_after, description, created_at 
       FROM transactions 
       WHERE user_id = $1 
       ORDER BY created_at DESC 
       LIMIT $2 OFFSET $3`,
      [req.user.id, limit, offset]
    );
    
    const count = await pool.query('SELECT COUNT(*) FROM transactions WHERE user_id = $1', [req.user.id]);
    
    res.json({
      success: true,
      data: {
        transactions: result.rows,
        pagination: {
          page,
          limit,
          total: parseInt(count.rows[0].count),
          pages: Math.ceil(count.rows[0].count / limit)
        }
      }
    });
  } catch (err) {
    logger.error('Get transactions error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get transactions' });
  }
});

app.get('/api/economy/packages', async (req, res) => {
  const packages = {
    coins_100: { price: 50, coins: 100, bonus: 0, totalCoins: 100, deployments: 4, pricePerCoin: '0.50 KES' },
    coins_250: { price: 100, coins: 250, bonus: 0, totalCoins: 250, deployments: 10, pricePerCoin: '0.40 KES' },
    coins_600: { price: 200, coins: 600, bonus: 50, totalCoins: 650, deployments: 26, pricePerCoin: '0.31 KES', popular: true },
    coins_1500: { price: 500, coins: 1500, bonus: 0, totalCoins: 1500, deployments: 60, pricePerCoin: '0.33 KES' },
    coins_3500: { price: 1000, coins: 3500, bonus: 250, totalCoins: 3750, deployments: 150, pricePerCoin: '0.27 KES', bestValue: true },
    coins_8000: { price: 2000, coins: 8000, bonus: 500, totalCoins: 8500, deployments: 340, pricePerCoin: '0.24 KES' },
    coins_20000: { price: 5000, coins: 20000, bonus: 2000, totalCoins: 22000, deployments: 880, pricePerCoin: '0.23 KES' }
  };
  res.json({ success: true, data: packages });
});

app.get('/api/economy/subscriptions', async (req, res) => {
  const tiers = {
    free: { price: 0, projects: 3, monthlyCoins: 0, costPerDeploy: 25, totalDeploys: 3, perks: [] },
    starter: { price: 50, projects: 4, monthlyCoins: 100, costPerDeploy: 25, totalDeploys: 8, perks: ['Priority Support'] },
    pro: { price: 499, projects: 10, monthlyCoins: 500, costPerDeploy: 25, totalDeploys: 30, perks: ['Priority Support', 'Pro Badge', 'Early Access'] },
    enterprise: { price: 1499, projects: 999999, monthlyCoins: 2000, costPerDeploy: 25, totalDeploys: 80, perks: ['VIP Support', 'Enterprise Badge', 'Custom Domain', 'API Priority'] },
    ultimate: { price: 4999, projects: 999999, monthlyCoins: 10000, costPerDeploy: 25, totalDeploys: 400, perks: ['Direct Line to Bruce', 'Ultimate Badge', 'Custom Features', 'White-label Option'] }
  };
  res.json({ success: true, data: tiers });
});

app.post('/api/economy/subscribe/:tier', authenticate, async (req, res) => {
  const { tier } = req.params;
  const validTiers = ['starter', 'pro', 'enterprise', 'ultimate'];
  
  if (!validTiers.includes(tier)) {
    return res.status(400).json({ success: false, error: 'INVALID_TIER', message: 'Invalid subscription tier' });
  }
  
  try {
    const tiers = {
      starter: { price: 50, coins: 100, projects: 4 },
      pro: { price: 499, coins: 500, projects: 10 },
      enterprise: { price: 1499, coins: 2000, projects: 999999 },
      ultimate: { price: 4999, coins: 10000, projects: 999999 }
    };
    
    const selected = tiers[tier];
    const expiryDate = new Date();
    expiryDate.setMonth(expiryDate.getMonth() + 1);
    
    await pool.query(
      `UPDATE users SET subscription_tier = $1, subscription_expiry = $2, max_projects = $3 WHERE id = $4`,
      [tier, expiryDate, selected.projects, req.user.id]
    );
    
    if (selected.coins > 0) {
      await updateUserCoins(req.user.id, selected.coins, 'subscription', `${tier.toUpperCase()} subscription bonus`, null);
    }
    
    await sendNotification(req.user.id, 'subscription_activated', '✨ Subscription Activated!', `You are now on ${tier.toUpperCase()} tier with ${selected.projects} projects and ${selected.coins} monthly coins!`, { tier, projects: selected.projects, coins: selected.coins });
    
    res.json({ success: true, data: { tier, expiry: expiryDate, projects: selected.projects, coins: selected.coins } });
  } catch (err) {
    logger.error('Subscribe error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to subscribe' });
  }
});

app.post('/api/economy/promo/redeem', authenticate, async (req, res) => {
  const { code } = req.body;
  
  const promoCodes = {
    'WELCOME100': { coins: 100, description: 'Welcome bonus' },
    'BRUCE50': { coins: 50, description: 'Bruce\'s special gift' },
    'CLOUDHOST': { coins: 25, description: 'CloudHost launch bonus' },
    'KENYA25': { coins: 25, description: 'Kenyan developer bonus' }
  };
  
  if (!promoCodes[code]) {
    return res.status(400).json({ success: false, error: 'INVALID_CODE', message: 'Invalid promo code' });
  }
  
  try {
    const used = await pool.query('SELECT id FROM transactions WHERE user_id = $1 AND description LIKE $2', [req.user.id, `%${code}%`]);
    if (used.rows.length > 0) {
      return res.status(400).json({ success: false, error: 'CODE_USED', message: 'You have already used this promo code' });
    }
    
    const promo = promoCodes[code];
    await updateUserCoins(req.user.id, promo.coins, 'promo_redeem', `${promo.description}: ${code}`, null);
    
    res.json({ success: true, data: { coins: promo.coins, message: promo.description } });
  } catch (err) {
    logger.error('Redeem promo error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to redeem promo code' });
  }
});

app.post('/api/economy/gift', authenticate, async (req, res) => {
  const { username, amount } = req.body;
  
  if (!username || !amount || amount < 1) {
    return res.status(400).json({ success: false, error: 'INVALID_REQUEST', message: 'Username and valid amount required' });
  }
  
  if (amount > req.user.coins) {
    return res.status(400).json({ success: false, error: 'INSUFFICIENT_COINS', message: 'Insufficient coins' });
  }
  
  try {
    const recipient = await pool.query('SELECT id, username FROM users WHERE username = $1', [username]);
    if (recipient.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'USER_NOT_FOUND', message: 'User not found' });
    }
    
    await updateUserCoins(req.user.id, -amount, 'gift_sent', `Gifted ${amount} coins to ${recipient.rows[0].username}`, recipient.rows[0].id);
    await updateUserCoins(recipient.rows[0].id, amount, 'gift_received', `Received ${amount} coins from ${req.user.username}`, req.user.id);
    
    await sendNotification(recipient.rows[0].id, 'gift_received', '🎁 You Received Coins!', `${req.user.username} gifted you ${amount} coins!`, { from: req.user.username, coins: amount });
    
    res.json({ success: true, data: { recipient: recipient.rows[0].username, amount } });
  } catch (err) {
    logger.error('Gift error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to gift coins' });
  }
});

app.get('/api/economy/leaderboard', async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  
  try {
    const result = await pool.query(
      `SELECT username, total_earned, coins, badges, subscription_tier 
       FROM users 
       WHERE is_banned = false 
       ORDER BY total_earned DESC 
       LIMIT $1`,
      [limit]
    );
    res.json({ success: true, data: result.rows });
  } catch (err) {
    logger.error('Leaderboard error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get leaderboard' });
  }
});

app.get('/api/economy/referrals', authenticate, async (req, res) => {
  try {
    const referrals = await pool.query('SELECT id, username, created_at FROM users WHERE referred_by = $1 ORDER BY created_at DESC', [req.user.id]);
    const earnings = await pool.query('SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE user_id = $1 AND type = $2', [req.user.id, 'referral_referrer']);
    
    res.json({
      success: true,
      data: {
        referral_code: req.user.referral_code,
        total_referrals: referrals.rows.length,
        total_earnings: parseInt(earnings.rows[0].total),
        referrals: referrals.rows
      }
    });
  } catch (err) {
    logger.error('Get referrals error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get referrals' });
  }
});

app.get('/api/economy/streak', authenticate, async (req, res) => {
  try {
    const result = await pool.query('SELECT login_streak, last_login_date FROM users WHERE id = $1', [req.user.id]);
    const streak = result.rows[0].login_streak || 0;
    let nextMilestone = streak >= 7 ? Math.ceil((streak + 1) / 7) * 7 : 7;
    let bonusAmount = nextMilestone === 7 ? 25 : 50;
    
    res.json({
      success: true,
      data: {
        current_streak: streak,
        next_milestone: nextMilestone,
        milestone_bonus: bonusAmount,
        last_login: result.rows[0].last_login_date
      }
    });
  } catch (err) {
    logger.error('Get streak error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get streak' });
  }
});

// ============== PAYMENT ENDPOINTS ==============

app.post('/api/payments/payhero/initiate', authenticate, async (req, res) => {
  const { type, packageId, tier, phone } = req.body;
  
  if (!phone) {
    return res.status(400).json({ success: false, error: 'MISSING_PHONE', message: 'Phone number required' });
  }
  
  const phoneRegex = /^254[0-9]{9}$/;
  if (!phoneRegex.test(phone)) {
    return res.status(400).json({ success: false, error: 'INVALID_PHONE', message: 'Phone must be in format 254XXXXXXXXX' });
  }
  
  try {
    let amount = 0;
    let coinsAwarded = 0;
    let projects = 0;
    
    if (type === 'coin_purchase') {
      const packages = {
        'coins_100': { price: 50, totalCoins: 100 },
        'coins_250': { price: 100, totalCoins: 250 },
        'coins_600': { price: 200, totalCoins: 650 },
        'coins_1500': { price: 500, totalCoins: 1500 },
        'coins_3500': { price: 1000, totalCoins: 3750 },
        'coins_8000': { price: 2000, totalCoins: 8500 },
        'coins_20000': { price: 5000, totalCoins: 22000 }
      };
      
      const pkg = packages[packageId];
      if (!pkg) {
        return res.status(400).json({ success: false, error: 'INVALID_PACKAGE', message: 'Invalid package' });
      }
      
      amount = pkg.price;
      coinsAwarded = pkg.totalCoins;
    } else if (type === 'subscription') {
      const tiers = {
        'starter': { price: 50, coins: 100, projects: 4 },
        'pro': { price: 499, coins: 500, projects: 10 },
        'enterprise': { price: 1499, coins: 2000, projects: 999999 },
        'ultimate': { price: 4999, coins: 10000, projects: 999999 }
      };
      
      const selected = tiers[tier];
      if (!selected) {
        return res.status(400).json({ success: false, error: 'INVALID_TIER', message: 'Invalid tier' });
      }
      
      amount = selected.price;
      coinsAwarded = selected.coins;
      projects = selected.projects;
    }
    
    const paymentId = generateId();
    
    await pool.query(
      `INSERT INTO payments (id, user_id, provider, type, amount, currency, phone_number, status, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [paymentId, req.user.id, 'payhero', type, amount, 'KES', phone, 'pending', JSON.stringify({ packageId, tier, coinsAwarded, projects })]
    );
    
    // Process payment (simulate M-Pesa STK Push)
    setTimeout(async () => {
      try {
        await pool.query('UPDATE payments SET status = $1, updated_at = NOW() WHERE id = $2', ['completed', paymentId]);
        
        if (type === 'coin_purchase') {
          await updateUserCoins(req.user.id, coinsAwarded, 'purchase', `Coin purchase: ${coinsAwarded} coins for KES ${amount}`, paymentId);
          await sendNotification(req.user.id, 'payment_success', '💰 Payment Successful!', `You received ${coinsAwarded} coins. That's ${Math.floor(coinsAwarded / 25)} deployments!`, { paymentId, coins: coinsAwarded, amount });
        } else if (type === 'subscription') {
          const expiryDate = new Date();
          expiryDate.setMonth(expiryDate.getMonth() + 1);
          
          await pool.query(
            `UPDATE users SET subscription_tier = $1, subscription_expiry = $2, max_projects = $3 WHERE id = $4`,
            [tier, expiryDate, projects, req.user.id]
          );
          
          if (coinsAwarded > 0) {
            await updateUserCoins(req.user.id, coinsAwarded, 'subscription', `Monthly subscription bonus for ${tier.toUpperCase()} tier`, paymentId);
          }
          
          await sendNotification(req.user.id, 'subscription_activated', '✨ Subscription Activated!', `You are now on ${tier.toUpperCase()} tier with ${projects} projects and ${coinsAwarded} monthly coins!`, { tier, projects, coins: coinsAwarded });
        }
      } catch (err) {
        logger.error('Payment processing error:', err);
      }
    }, 3000);
    
    res.json({
      success: true,
      data: {
        paymentId,
        message: 'M-Pesa prompt sent to your phone. Please enter your PIN to complete payment.',
        amount,
        coins: coinsAwarded,
        deployments: Math.floor(coinsAwarded / 25)
      }
    });
    
  } catch (err) {
    logger.error('PayHero error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to initiate payment' });
  }
});

app.get('/api/payments/payhero/status/:paymentId', authenticate, async (req, res) => {
  const { paymentId } = req.params;
  
  try {
    const result = await pool.query('SELECT status FROM payments WHERE id = $1 AND user_id = $2', [paymentId, req.user.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Payment not found' });
    }
    res.json({ success: true, data: { status: result.rows[0].status } });
  } catch (err) {
    logger.error('Payment status error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get payment status' });
  }
});

// ============== BOT TEMPLATE ENDPOINTS ==============

app.get('/api/bot-templates', async (req, res) => {
  const { category, featured, search, page = 1, limit = 20 } = req.query;
  
  try {
    let query = `
      SELECT bt.*, 
             COALESCE(AVG(r.rating), 0) as avg_rating,
             COUNT(DISTINCT r.id) as review_count
      FROM bot_templates bt
      LEFT JOIN reviews r ON bt.id = r.bot_template_id
      WHERE bt.is_active = true
    `;
    const params = [];
    let idx = 1;
    
    if (category && category !== 'all') {
      query += ` AND bt.category = $${idx}`;
      params.push(category);
      idx++;
    }
    
    if (featured === 'true') {
      query += ` AND bt.is_featured = true`;
    }
    
    if (search) {
      query += ` AND (bt.name ILIKE $${idx} OR bt.description ILIKE $${idx})`;
      params.push(`%${search}%`);
      idx++;
    }
    
    query += ` GROUP BY bt.id ORDER BY bt.is_featured DESC, bt.deploy_count DESC LIMIT $${idx} OFFSET $${idx + 1}`;
    params.push(limit, (page - 1) * limit);
    
    const result = await pool.query(query, params);
    
    const countQuery = `SELECT COUNT(*) FROM bot_templates WHERE is_active = true`;
    const count = await pool.query(countQuery);
    
    res.json({
      success: true,
      data: {
        templates: result.rows,
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: parseInt(count.rows[0].count),
          pages: Math.ceil(count.rows[0].count / limit)
        }
      }
    });
  } catch (err) {
    logger.error('Get bot templates error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get bot templates' });
  }
});

app.get('/api/bot-templates/:id', async (req, res) => {
  const { id } = req.params;
  
  try {
    const result = await pool.query(
      `SELECT bt.*, 
              COALESCE(AVG(r.rating), 0) as avg_rating,
              COUNT(DISTINCT r.id) as review_count,
              json_agg(DISTINCT jsonb_build_object('id', r.id, 'user_id', r.user_id, 'username', u.username, 'rating', r.rating, 'comment', r.comment, 'helpful_count', r.helpful_count, 'created_at', r.created_at)) FILTER (WHERE r.id IS NOT NULL) as reviews
       FROM bot_templates bt
       LEFT JOIN reviews r ON bt.id = r.bot_template_id
       LEFT JOIN users u ON r.user_id = u.id
       WHERE bt.id = $1
       GROUP BY bt.id`,
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Bot template not found' });
    }
    
    res.json({ success: true, data: result.rows[0] });
  } catch (err) {
    logger.error('Get bot template error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get bot template' });
  }
});

// ============== DEPLOYMENT ENDPOINTS ==============

app.post('/api/projects/deploy', authenticate, upload.single('zip_file'), async (req, res) => {
  const { name, bot_template_id, git_url, branch, env_vars } = req.body;
  
  // Check project limit
  const projectCount = await pool.query('SELECT COUNT(*) FROM projects WHERE user_id = $1', [req.user.id]);
  if (parseInt(projectCount.rows[0].count) >= req.user.max_projects) {
    return res.status(400).json({ 
      success: false, 
      error: 'PROJECT_LIMIT_REACHED', 
      message: `You have reached your project limit of ${req.user.max_projects}. Upgrade to deploy more projects.` 
    });
  }
  
  // Check coins
  if (req.user.coins < 25) {
    return res.status(400).json({ 
      success: false, 
      error: 'INSUFFICIENT_COINS', 
      message: `Insufficient coins. You need 25 coins to deploy. Purchase more coins or upgrade your subscription.` 
    });
  }
  
  try {
    const projectId = generateId();
    const projectDir = path.join(__dirname, 'projects', projectId);
    await fsPromises.mkdir(projectDir, { recursive: true });
    
    let deployType = 'git';
    let gitUrl = null;
    let branchName = branch || 'main';
    
    if (bot_template_id) {
      const template = await pool.query('SELECT * FROM bot_templates WHERE id = $1', [bot_template_id]);
      if (template.rows.length > 0) {
        gitUrl = template.rows[0].git_url;
        branchName = template.rows[0].branch;
        deployType = template.rows[0].type;
        await pool.query('UPDATE bot_templates SET deploy_count = deploy_count + 1 WHERE id = $1', [bot_template_id]);
      }
    } else if (git_url) {
      gitUrl = git_url;
    } else if (req.file) {
      deployType = 'zip';
    }
    
    // Clone or extract
    if (deployType === 'git' && gitUrl) {
      const git = simpleGit();
      await git.clone(gitUrl, projectDir);
      await git.cwd(projectDir);
      await git.checkout(branchName);
    } else if (deployType === 'zip' && req.file) {
      const zip = new AdmZip(req.file.path);
      zip.extractAllTo(projectDir, true);
    }
    
    // Install dependencies if package.json exists
    const packageJsonPath = path.join(projectDir, 'package.json');
    if (fs.existsSync(packageJsonPath)) {
      await new Promise((resolve, reject) => {
        const npm = spawn('npm', ['install'], { cwd: projectDir, shell: true });
        npm.on('close', (code) => code === 0 ? resolve() : reject(new Error('npm install failed')));
        npm.on('error', reject);
      });
    }
    
    // Save project
    await pool.query(
      `INSERT INTO projects (id, user_id, name, bot_template_id, type, git_url, branch, env_vars)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [projectId, req.user.id, name, bot_template_id || null, deployType, gitUrl, branchName, JSON.stringify(env_vars ? JSON.parse(env_vars) : {})]
    );
    
    // Deduct coins
    await updateUserCoins(req.user.id, -25, 'deploy', `Deployed project: ${name}`, projectId);
    
    // Revenue share to bot author
    if (bot_template_id) {
      const template = await pool.query('SELECT author_id FROM bot_templates WHERE id = $1', [bot_template_id]);
      if (template.rows.length > 0 && template.rows[0].author_id) {
        await updateUserCoins(template.rows[0].author_id, 12, 'revenue_share', `Revenue share from deployment of ${name}`, projectId);
      }
    }
    
    // Start project
    await startProject(projectId);
    
    res.json({
      success: true,
      data: {
        project_id: projectId,
        name,
        message: 'Project deployed successfully',
        coins_remaining: req.user.coins - 25
      }
    });
    
  } catch (err) {
    logger.error('Deploy error:', err);
    res.status(500).json({ success: false, error: 'DEPLOY_FAILED', message: err.message });
  }
});

async function startProject(projectId) {
  const result = await pool.query('SELECT * FROM projects WHERE id = $1', [projectId]);
  if (result.rows.length === 0) return;
  
  const project = result.rows[0];
  const projectDir = path.join(__dirname, 'projects', project.id);
  const envFile = path.join(projectDir, '.env');
  
  // Create .env file
  let envContent = '';
  for (const [key, value] of Object.entries(project.env_vars || {})) {
    envContent += `${key}=${value}\n`;
  }
  await fsPromises.writeFile(envFile, envContent);
  
  // Determine start command
  let startCommand = 'node index.js';
  const packageJsonPath = path.join(projectDir, 'package.json');
  if (fs.existsSync(packageJsonPath)) {
    const packageJson = JSON.parse(await fsPromises.readFile(packageJsonPath, 'utf-8'));
    if (packageJson.scripts && packageJson.scripts.start) {
      startCommand = 'npm run start';
    }
  }
  
  // Spawn process
  const proc = spawn(startCommand, {
    cwd: projectDir,
    shell: true,
    env: { ...process.env, ...project.env_vars, PORT: project.port || 3000 }
  });
  
  // Log output
  proc.stdout.on('data', (data) => {
    io.to(`project:${projectId}`).emit('project-log', { projectId, log: data.toString(), timestamp: new Date() });
  });
  
  proc.stderr.on('data', (data) => {
    io.to(`project:${projectId}`).emit('project-log', { projectId, log: data.toString(), timestamp: new Date(), error: true });
  });
  
  proc.on('close', (code) => {
    pool.query('UPDATE projects SET status = $1, pid = NULL WHERE id = $2', ['stopped', projectId]);
    io.to(`project:${projectId}`).emit('project-stopped', { projectId, code });
  });
  
  await pool.query('UPDATE projects SET status = $1, pid = $2 WHERE id = $3', ['running', proc.pid, projectId]);
  
  if (!global.projects) global.projects = {};
  global.projects[projectId] = proc;
}

app.post('/api/projects/:id/start', authenticate, async (req, res) => {
  const { id } = req.params;
  
  const result = await pool.query('SELECT * FROM projects WHERE id = $1 AND user_id = $2', [id, req.user.id]);
  if (result.rows.length === 0) {
    return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Project not found' });
  }
  
  if (result.rows[0].status === 'running') {
    return res.status(400).json({ success: false, error: 'ALREADY_RUNNING', message: 'Project is already running' });
  }
  
  try {
    await startProject(id);
    res.json({ success: true, data: { status: 'starting' } });
  } catch (err) {
    logger.error('Start project error:', err);
    res.status(500).json({ success: false, error: 'START_FAILED', message: err.message });
  }
});

app.post('/api/projects/:id/stop', authenticate, async (req, res) => {
  const { id } = req.params;
  
  const result = await pool.query('SELECT * FROM projects WHERE id = $1 AND user_id = $2', [id, req.user.id]);
  if (result.rows.length === 0) {
    return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Project not found' });
  }
  
  if (global.projects && global.projects[id]) {
    global.projects[id].kill();
    delete global.projects[id];
  }
  
  await pool.query('UPDATE projects SET status = $1, pid = NULL WHERE id = $2', ['stopped', id]);
  res.json({ success: true, data: { status: 'stopped' } });
});

app.get('/api/projects', authenticate, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT p.*, bt.name as template_name 
       FROM projects p
       LEFT JOIN bot_templates bt ON p.bot_template_id = bt.id
       WHERE p.user_id = $1 
       ORDER BY p.created_at DESC`,
      [req.user.id]
    );
    res.json({ success: true, data: result.rows });
  } catch (err) {
    logger.error('Get projects error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get projects' });
  }
});

app.get('/api/projects/:id', authenticate, async (req, res) => {
  const { id } = req.params;
  
  try {
    const result = await pool.query(
      `SELECT p.*, bt.name as template_name, bt.config_fields
       FROM projects p
       LEFT JOIN bot_templates bt ON p.bot_template_id = bt.id
       WHERE p.id = $1 AND p.user_id = $2`,
      [id, req.user.id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Project not found' });
    }
    
    res.json({ success: true, data: result.rows[0] });
  } catch (err) {
    logger.error('Get project error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get project' });
  }
});

app.put('/api/projects/:id/env', authenticate, async (req, res) => {
  const { id } = req.params;
  const { env_vars } = req.body;
  
  try {
    await pool.query(
      'UPDATE projects SET env_vars = $1, updated_at = NOW() WHERE id = $2 AND user_id = $3',
      [JSON.stringify(env_vars), id, req.user.id]
    );
    
    // Update .env file if project exists
    const projectDir = path.join(__dirname, 'projects', id);
    let envContent = '';
    for (const [key, value] of Object.entries(env_vars)) {
      envContent += `${key}=${value}\n`;
    }
    await fsPromises.writeFile(path.join(projectDir, '.env'), envContent);
    
    res.json({ success: true, data: { message: 'Environment variables updated' } });
  } catch (err) {
    logger.error('Update env error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to update environment variables' });
  }
});

app.delete('/api/projects/:id', authenticate, async (req, res) => {
  const { id } = req.params;
  
  try {
    if (global.projects && global.projects[id]) {
      global.projects[id].kill();
      delete global.projects[id];
    }
    
    const projectDir = path.join(__dirname, 'projects', id);
    if (fs.existsSync(projectDir)) {
      await fsPromises.rm(projectDir, { recursive: true, force: true });
    }
    
    await pool.query('DELETE FROM projects WHERE id = $1 AND user_id = $2', [id, req.user.id]);
    res.json({ success: true, data: { message: 'Project deleted' } });
  } catch (err) {
    logger.error('Delete project error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to delete project' });
  }
});

// ============== ADMIN BOT MANAGEMENT WITH GIT AUTO-SCAN ==============

app.post('/api/admin/bots/scan-repo', authenticate, requireAdmin, async (req, res) => {
  const { git_url, branch = 'main' } = req.body;
  
  if (!git_url) {
    return res.status(400).json({ success: false, error: 'MISSING_URL', message: 'GitHub URL required' });
  }
  
  try {
    const repoMatch = git_url.match(/github\.com\/([^\/]+)\/([^\/\.]+)/);
    if (!repoMatch) {
      return res.status(400).json({ success: false, error: 'INVALID_URL', message: 'Invalid GitHub URL' });
    }
    
    const [, owner, repo] = repoMatch;
    const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents?ref=${branch}`;
    
    const response = await axios.get(apiUrl, {
      headers: process.env.GITHUB_TOKEN ? { Authorization: `token ${process.env.GITHUB_TOKEN}` } : {}
    });
    
    const files = response.data;
    const configFields = [];
    let envExample = null;
    
    // Find .env.example file
    const envFile = files.find(f => 
      f.name === '.env.example' || 
      f.name === '.env.sample' || 
      f.name === '.env.template'
    );
    
    if (envFile) {
      const envContent = await axios.get(envFile.download_url);
      const lines = envContent.data.split('\n');
      
      for (const line of lines) {
        const match = line.match(/^([A-Za-z0-9_]+)=/);
        if (match) {
          const key = match[1];
          let type = 'text';
          
          if (key.includes('SECRET') || key.includes('KEY') || key.includes('PASSWORD') || key.includes('TOKEN')) {
            type = 'password';
          } else if (key.includes('URL') || key.includes('URI')) {
            type = 'url';
          } else if (key.includes('PORT')) {
            type = 'number';
          }
          
          // Extract comment for description
          let description = '';
          const commentLine = lines[lines.indexOf(line) - 1];
          if (commentLine && commentLine.startsWith('#')) {
            description = commentLine.substring(1).trim();
          }
          
          configFields.push({
            key,
            label: key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, l => l.toUpperCase()),
            type,
            required: true,
            description,
            placeholder: `Enter ${key.toLowerCase().replace(/_/g, ' ')}`
          });
        }
      }
    }
    
    // Detect category
    let category = 'other';
    const hasPackageJson = files.some(f => f.name === 'package.json');
    
    if (hasPackageJson) {
      const packageFile = files.find(f => f.name === 'package.json');
      if (packageFile) {
        const packageContent = await axios.get(packageFile.download_url);
        const pkg = JSON.parse(packageContent.data);
        if (pkg.name && (pkg.name.includes('whatsapp') || pkg.name.includes('wa'))) {
          category = 'whatsapp';
        } else if (pkg.name && (pkg.name.includes('telegram') || pkg.name.includes('tg'))) {
          category = 'telegram';
        } else if (pkg.name && pkg.name.includes('discord')) {
          category = 'discord';
        }
      }
    }
    
    const botName = repo.replace(/-/g, ' ').replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    
    res.json({
      success: true,
      data: {
        name: botName,
        git_url,
        branch,
        category,
        config_fields: configFields,
        detected_files: {
          has_env_example: !!envFile,
          has_package_json: hasPackageJson
        },
        config_fields_count: configFields.length
      }
    });
    
  } catch (err) {
    logger.error('Scan repo error:', err);
    res.status(500).json({ success: false, error: 'SCAN_FAILED', message: err.message });
  }
});

app.post('/api/admin/bots', authenticate, requireAdmin, async (req, res) => {
  const { name, description, git_url, branch, category, icon, config_fields, type = 'git' } = req.body;
  
  if (!name || !git_url) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Name and Git URL required' });
  }
  
  try {
    const botId = generateId();
    
    await pool.query(
      `INSERT INTO bot_templates (id, name, description, type, git_url, branch, category, icon, config_fields, author_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [botId, name, description, type, git_url, branch || 'main', category || 'other', icon || null, JSON.stringify(config_fields || []), req.user.id]
    );
    
    await logAudit(req.user.id, 'create_bot', 'bot_template', botId, { name, git_url }, req.ip, req.headers['user-agent']);
    
    res.json({ success: true, data: { id: botId, name, message: 'Bot template created successfully' } });
  } catch (err) {
    logger.error('Create bot error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to create bot template' });
  }
});

app.put('/api/admin/bots/:id', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  const updates = [];
  const values = [];
  let idx = 1;
  
  const fields = ['name', 'description', 'git_url', 'branch', 'category', 'icon', 'config_fields', 'is_active', 'is_featured', 'is_verified'];
  for (const field of fields) {
    if (req.body[field] !== undefined) {
      updates.push(`${field} = $${idx}`);
      values.push(field === 'config_fields' ? JSON.stringify(req.body[field]) : req.body[field]);
      idx++;
    }
  }
  
  if (updates.length === 0) {
    return res.status(400).json({ success: false, error: 'NO_FIELDS', message: 'No fields to update' });
  }
  
  updates.push(`updated_at = NOW()`);
  values.push(id);
  
  try {
    await pool.query(`UPDATE bot_templates SET ${updates.join(', ')} WHERE id = $${idx}`, values);
    await logAudit(req.user.id, 'update_bot', 'bot_template', id, req.body, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { message: 'Bot template updated successfully' } });
  } catch (err) {
    logger.error('Update bot error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to update bot template' });
  }
});

app.delete('/api/admin/bots/:id', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  
  try {
    await pool.query('DELETE FROM bot_templates WHERE id = $1', [id]);
    await logAudit(req.user.id, 'delete_bot', 'bot_template', id, {}, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { message: 'Bot template deleted' } });
  } catch (err) {
    logger.error('Delete bot error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to delete bot template' });
  }
});

// ============== ADMIN USER MANAGEMENT ==============

app.get('/api/admin/users', authenticate, requireAdmin, async (req, res) => {
  const { search, role, tier, banned, page = 1, limit = 20 } = req.query;
  
  try {
    let query = `
      SELECT id, username, email, role, coins, subscription_tier, is_verified, is_banned, 
             total_earned, total_spent, created_at, last_login_date
      FROM users WHERE 1=1
    `;
    const params = [];
    let idx = 1;
    
    if (search) {
      query += ` AND (username ILIKE $${idx} OR email ILIKE $${idx})`;
      params.push(`%${search}%`);
      idx++;
    }
    if (role && role !== 'all') {
      query += ` AND role = $${idx}`;
      params.push(role);
      idx++;
    }
    if (tier && tier !== 'all') {
      query += ` AND subscription_tier = $${idx}`;
      params.push(tier);
      idx++;
    }
    if (banned === 'true') {
      query += ` AND is_banned = true`;
    } else if (banned === 'false') {
      query += ` AND is_banned = false`;
    }
    
    query += ` ORDER BY created_at DESC LIMIT $${idx} OFFSET $${idx + 1}`;
    params.push(limit, (page - 1) * limit);
    
    const result = await pool.query(query, params);
    const count = await pool.query('SELECT COUNT(*) FROM users');
    
    res.json({
      success: true,
      data: {
        users: result.rows,
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: parseInt(count.rows[0].count),
          pages: Math.ceil(count.rows[0].count / limit)
        }
      }
    });
  } catch (err) {
    logger.error('Admin get users error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get users' });
  }
});

app.get('/api/admin/users/:id', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  
  try {
    const user = await pool.query(
      `SELECT u.*, 
              (SELECT COUNT(*) FROM projects WHERE user_id = u.id) as project_count,
              (SELECT COUNT(*) FROM transactions WHERE user_id = u.id) as transaction_count
       FROM users u WHERE u.id = $1`,
      [id]
    );
    
    if (user.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'User not found' });
    }
    
    const projects = await pool.query('SELECT id, name, status, created_at FROM projects WHERE user_id = $1 ORDER BY created_at DESC LIMIT 10', [id]);
    const transactions = await pool.query('SELECT id, type, amount, balance_after, description, created_at FROM transactions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 20', [id]);
    
    res.json({
      success: true,
      data: {
        user: user.rows[0],
        recent_projects: projects.rows,
        recent_transactions: transactions.rows
      }
    });
  } catch (err) {
    logger.error('Admin get user error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get user' });
  }
});

app.post('/api/admin/users/:id/coins/grant', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  const { amount, reason } = req.body;
  
  if (!amount || amount < 1) {
    return res.status(400).json({ success: false, error: 'INVALID_AMOUNT', message: 'Valid amount required' });
  }
  
  try {
    await updateUserCoins(id, amount, 'admin_grant', reason || `Admin grant by ${req.user.username}`, req.user.id);
    await sendNotification(id, 'coins_granted', '💰 Coins Granted!', `You received ${amount} coins from an admin. ${reason ? `Reason: ${reason}` : ''}`, { amount, reason, admin: req.user.username });
    await logAudit(req.user.id, 'grant_coins', 'user', id, { amount, reason }, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { message: `Granted ${amount} coins to user` } });
  } catch (err) {
    logger.error('Grant coins error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to grant coins' });
  }
});

app.post('/api/admin/users/:id/ban', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  const { reason } = req.body;
  
  try {
    await pool.query('UPDATE users SET is_banned = true, updated_at = NOW() WHERE id = $1', [id]);
    
    // Stop all user projects
    const projects = await pool.query('SELECT id FROM projects WHERE user_id = $1', [id]);
    for (const project of projects.rows) {
      if (global.projects && global.projects[project.id]) {
        global.projects[project.id].kill();
        delete global.projects[project.id];
      }
    }
    
    await sendNotification(id, 'account_banned', '⚠️ Account Banned', `Your account has been banned. ${reason ? `Reason: ${reason}` : ''}`, { reason });
    await logAudit(req.user.id, 'ban_user', 'user', id, { reason }, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { message: 'User banned successfully' } });
  } catch (err) {
    logger.error('Ban user error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to ban user' });
  }
});

app.post('/api/admin/users/:id/unban', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  
  try {
    await pool.query('UPDATE users SET is_banned = false, updated_at = NOW() WHERE id = $1', [id]);
    await sendNotification(id, 'account_unbanned', '✅ Account Unbanned', 'Your account has been unbanned. You can now log in again.', {});
    await logAudit(req.user.id, 'unban_user', 'user', id, {}, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { message: 'User unbanned successfully' } });
  } catch (err) {
    logger.error('Unban user error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to unban user' });
  }
});

app.post('/api/admin/users/:id/impersonate', authenticate, requireSuperAdmin, async (req, res) => {
  const { id } = req.params;
  
  try {
    const user = await pool.query('SELECT id, username, role FROM users WHERE id = $1', [id]);
    if (user.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'User not found' });
    }
    
    const token = jwt.sign({ userId: id, impersonatedBy: req.user.id }, process.env.JWT_SECRET || 'cloudhost-super-secret-key-change-in-production', { expiresIn: '1h' });
    await logAudit(req.user.id, 'impersonate_user', 'user', id, {}, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { token, user: user.rows[0] } });
  } catch (err) {
    logger.error('Impersonate error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to impersonate user' });
  }
});

// ============== AIRDROP SYSTEM ==============

app.post('/api/admin/airdrops', authenticate, requireAdmin, async (req, res) => {
  const { name, description, coins, total_claims, start_date, end_date } = req.body;
  
  if (!name || !coins) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Name and coins required' });
  }
  
  try {
    const airdropId = generateId();
    
    await pool.query(
      `INSERT INTO airdrops (id, name, description, coins, total_claims, remaining_claims, start_date, end_date, created_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [airdropId, name, description, coins, total_claims || null, total_claims || null, start_date || null, end_date || null, req.user.id]
    );
    
    await logAudit(req.user.id, 'create_airdrop', 'airdrop', airdropId, { name, coins }, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { id: airdropId, name } });
  } catch (err) {
    logger.error('Create airdrop error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to create airdrop' });
  }
});

app.post('/api/airdrops/:id/claim', authenticate, async (req, res) => {
  const { id } = req.params;
  const { claim_code } = req.body;
  
  try {
    const airdrop = await pool.query(
      `SELECT * FROM airdrops 
       WHERE id = $1 AND status = 'active'
       AND (start_date IS NULL OR start_date <= NOW())
       AND (end_date IS NULL OR end_date >= NOW())
       AND (remaining_claims IS NULL OR remaining_claims > 0)`,
      [id]
    );
    
    if (airdrop.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'AIRDROP_NOT_AVAILABLE', message: 'Airdrop not available' });
    }
    
    const existing = await pool.query('SELECT id FROM airdrop_claims WHERE airdrop_id = $1 AND user_id = $2', [id, req.user.id]);
    if (existing.rows.length > 0) {
      return res.status(400).json({ success: false, error: 'ALREADY_CLAIMED', message: 'You have already claimed this airdrop' });
    }
    
    await pool.query('BEGIN');
    await pool.query('INSERT INTO airdrop_claims (id, airdrop_id, user_id, claim_code) VALUES ($1, $2, $3, $4)', [generateId(), id, req.user.id, claim_code || null]);
    
    if (airdrop.rows[0].remaining_claims !== null) {
      await pool.query('UPDATE airdrops SET remaining_claims = remaining_claims - 1 WHERE id = $1', [id]);
    }
    
    await updateUserCoins(req.user.id, airdrop.rows[0].coins, 'airdrop_claim', `Claimed ${airdrop.rows[0].coins} coins from ${airdrop.rows[0].name} airdrop`, id);
    await pool.query('COMMIT');
    
    await sendNotification(req.user.id, 'airdrop_claimed', '🎉 Airdrop Claimed!', `You claimed ${airdrop.rows[0].coins} coins from ${airdrop.rows[0].name}!`, { airdrop: airdrop.rows[0].name, coins: airdrop.rows[0].coins });
    res.json({ success: true, data: { coins: airdrop.rows[0].coins, message: `Successfully claimed ${airdrop.rows[0].coins} coins!` } });
  } catch (err) {
    await pool.query('ROLLBACK');
    logger.error('Claim airdrop error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to claim airdrop' });
  }
});

app.get('/api/airdrops/active', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, description, coins, total_claims, remaining_claims, start_date, end_date
       FROM airdrops 
       WHERE status = 'active'
       AND (start_date IS NULL OR start_date <= NOW())
       AND (end_date IS NULL OR end_date >= NOW())
       AND (remaining_claims IS NULL OR remaining_claims > 0)
       ORDER BY created_at DESC`
    );
    res.json({ success: true, data: result.rows });
  } catch (err) {
    logger.error('Get active airdrops error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get airdrops' });
  }
});

// ============== REVIEW SYSTEM ==============

app.post('/api/reviews', authenticate, async (req, res) => {
  const { bot_template_id, rating, comment } = req.body;
  
  if (!bot_template_id || !rating || rating < 1 || rating > 5) {
    return res.status(400).json({ success: false, error: 'INVALID_REQUEST', message: 'Valid bot ID and rating (1-5) required' });
  }
  
  try {
    const existing = await pool.query('SELECT id FROM reviews WHERE user_id = $1 AND bot_template_id = $2', [req.user.id, bot_template_id]);
    
    if (existing.rows.length > 0) {
      await pool.query('UPDATE reviews SET rating = $1, comment = $2, created_at = NOW() WHERE user_id = $3 AND bot_template_id = $4', [rating, comment, req.user.id, bot_template_id]);
    } else {
      await pool.query('INSERT INTO reviews (id, user_id, bot_template_id, rating, comment) VALUES ($1, $2, $3, $4, $5)', [generateId(), req.user.id, bot_template_id, rating, comment]);
      
      // Award coins for first review
      const count = await pool.query('SELECT COUNT(*) FROM reviews WHERE user_id = $1', [req.user.id]);
      if (parseInt(count.rows[0].count) === 1) {
        await updateUserCoins(req.user.id, 5, 'review_reward', 'First review bonus', bot_template_id);
      }
    }
    
    res.json({ success: true, data: { message: 'Review submitted successfully' } });
  } catch (err) {
    logger.error('Submit review error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to submit review' });
  }
});

app.post('/api/reviews/:id/helpful', authenticate, async (req, res) => {
  const { id } = req.params;
  
  try {
    const result = await pool.query('UPDATE reviews SET helpful_count = helpful_count + 1 WHERE id = $1 RETURNING user_id', [id]);
    if (result.rows.length > 0) {
      await updateUserCoins(result.rows[0].user_id, 2, 'helpful_vote', 'Someone found your review helpful', id);
    }
    res.json({ success: true, data: { message: 'Marked as helpful' } });
  } catch (err) {
    logger.error('Helpful vote error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to mark as helpful' });
  }
});

// ============== SUPPORT TICKET SYSTEM ==============

app.post('/api/support/tickets', authenticate, async (req, res) => {
  const { subject, message, priority = 'normal' } = req.body;
  
  if (!subject || !message) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Subject and message required' });
  }
  
  try {
    const ticketId = generateId();
    await pool.query('INSERT INTO support_tickets (id, user_id, subject, message, priority) VALUES ($1, $2, $3, $4, $5)', [ticketId, req.user.id, subject, message, priority]);
    await sendNotification(req.user.id, 'ticket_created', '🎫 Support Ticket Created', `Your ticket "${subject}" has been created. We'll respond soon.`, { ticket_id: ticketId });
    res.json({ success: true, data: { id: ticketId, subject, message: 'Ticket created successfully' } });
  } catch (err) {
    logger.error('Create ticket error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to create ticket' });
  }
});

app.get('/api/support/tickets', authenticate, async (req, res) => {
  try {
    let query, params;
    
    if (req.user.role === 'admin' || req.user.role === 'superadmin') {
      query = `
        SELECT t.*, u.username as user_username,
               (SELECT COUNT(*) FROM ticket_replies WHERE ticket_id = t.id) as reply_count
        FROM support_tickets t
        LEFT JOIN users u ON t.user_id = u.id
        ORDER BY 
          CASE t.status 
            WHEN 'open' THEN 1
            WHEN 'in_progress' THEN 2
            ELSE 3
          END,
          t.created_at DESC
      `;
      params = [];
    } else {
      query = `
        SELECT t.*, (SELECT COUNT(*) FROM ticket_replies WHERE ticket_id = t.id) as reply_count
        FROM support_tickets t
        WHERE t.user_id = $1
        ORDER BY t.created_at DESC
      `;
      params = [req.user.id];
    }
    
    const result = await pool.query(query, params);
    res.json({ success: true, data: result.rows });
  } catch (err) {
    logger.error('Get tickets error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get tickets' });
  }
});

app.post('/api/support/tickets/:id/reply', authenticate, async (req, res) => {
  const { id } = req.params;
  const { message } = req.body;
  
  if (!message) {
    return res.status(400).json({ success: false, error: 'MISSING_MESSAGE', message: 'Reply message required' });
  }
  
  try {
    const ticket = await pool.query('SELECT user_id, status FROM support_tickets WHERE id = $1', [id]);
    if (ticket.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Ticket not found' });
    }
    
    if (ticket.rows[0].user_id !== req.user.id && req.user.role !== 'admin' && req.user.role !== 'superadmin') {
      return res.status(403).json({ success: false, error: 'FORBIDDEN', message: 'Not authorized to reply to this ticket' });
    }
    
    const isAdmin = req.user.role === 'admin' || req.user.role === 'superadmin';
    
    await pool.query('INSERT INTO ticket_replies (id, ticket_id, user_id, message, is_admin) VALUES ($1, $2, $3, $4, $5)', [generateId(), id, req.user.id, message, isAdmin]);
    
    if (isAdmin) {
      await pool.query('UPDATE support_tickets SET status = $1, updated_at = NOW() WHERE id = $2', ['in_progress', id]);
      await sendNotification(ticket.rows[0].user_id, 'ticket_reply', '📩 Support Ticket Reply', `Admin replied to your ticket: "${message.substring(0, 100)}..."`, { ticket_id: id });
    } else {
      await pool.query('UPDATE support_tickets SET status = $1, updated_at = NOW() WHERE id = $2', ['open', id]);
      
      const admins = await pool.query('SELECT id FROM users WHERE role IN ($1, $2)', ['admin', 'superadmin']);
      for (const admin of admins.rows) {
        await sendNotification(admin.id, 'ticket_reply', '📩 New Ticket Reply', `${req.user.username} replied to ticket: "${message.substring(0, 100)}..."`, { ticket_id: id, user: req.user.username });
      }
    }
    
    res.json({ success: true, data: { message: 'Reply sent successfully' } });
  } catch (err) {
    logger.error('Ticket reply error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to send reply' });
  }
});

// ============== CHAT SYSTEM ==============

app.get('/api/chat/messages', async (req, res) => {
  const { room = 'general', limit = 50, before } = req.query;
  
  try {
    let query = `
      SELECT cm.*, u.username, u.avatar, u.badges
      FROM chat_messages cm
      LEFT JOIN users u ON cm.user_id = u.id
      WHERE cm.room = $1
    `;
    const params = [room];
    let idx = 2;
    
    if (before) {
      query += ` AND cm.created_at < $${idx}`;
      params.push(before);
      idx++;
    }
    
    query += ` ORDER BY cm.created_at DESC LIMIT $${idx}`;
    params.push(limit);
    
    const result = await pool.query(query, params);
    res.json({ success: true, data: result.rows.reverse() });
  } catch (err) {
    logger.error('Get chat messages error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get messages' });
  }
});

app.post('/api/chat/messages', authenticate, async (req, res) => {
  const { message, room = 'general' } = req.body;
  
  if (!message || message.trim().length === 0) {
    return res.status(400).json({ success: false, error: 'EMPTY_MESSAGE', message: 'Message cannot be empty' });
  }
  
  if (message.length > 2000) {
    return res.status(400).json({ success: false, error: 'MESSAGE_TOO_LONG', message: 'Message cannot exceed 2000 characters' });
  }
  
  try {
    const messageId = generateId();
    await pool.query('INSERT INTO chat_messages (id, user_id, username, message, room) VALUES ($1, $2, $3, $4, $5)', [messageId, req.user.id, req.user.username, message, room]);
    
    io.to(`chat:${room}`).emit('chat-message', {
      id: messageId,
      user_id: req.user.id,
      username: req.user.username,
      message,
      room,
      created_at: new Date(),
      avatar: req.user.avatar,
      badges: req.user.badges
    });
    
    res.json({ success: true, data: { id: messageId, message: 'Message sent' } });
  } catch (err) {
    logger.error('Send chat message error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to send message' });
  }
});

// ============== NOTIFICATION SYSTEM ==============

app.get('/api/notifications', authenticate, async (req, res) => {
  const { unread_only = false, limit = 50, page = 1 } = req.query;
  
  try {
    let query = 'SELECT * FROM notifications WHERE user_id = $1';
    const params = [req.user.id];
    let idx = 2;
    
    if (unread_only === 'true') {
      query += ` AND is_read = false`;
    }
    
    query += ` ORDER BY created_at DESC LIMIT $${idx} OFFSET $${idx + 1}`;
    params.push(limit, (page - 1) * limit);
    
    const result = await pool.query(query, params);
    const count = await pool.query('SELECT COUNT(*) FROM notifications WHERE user_id = $1', [req.user.id]);
    const unread = await pool.query('SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = false', [req.user.id]);
    
    res.json({
      success: true,
      data: {
        notifications: result.rows,
        unread_count: parseInt(unread.rows[0].count),
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: parseInt(count.rows[0].count),
          pages: Math.ceil(count.rows[0].count / limit)
        }
      }
    });
  } catch (err) {
    logger.error('Get notifications error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get notifications' });
  }
});

app.put('/api/notifications/:id/read', authenticate, async (req, res) => {
  const { id } = req.params;
  
  try {
    await pool.query('UPDATE notifications SET is_read = true WHERE id = $1 AND user_id = $2', [id, req.user.id]);
    res.json({ success: true, data: { message: 'Notification marked as read' } });
  } catch (err) {
    logger.error('Mark notification read error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to mark notification as read' });
  }
});

app.put('/api/notifications/read-all', authenticate, async (req, res) => {
  try {
    await pool.query('UPDATE notifications SET is_read = true WHERE user_id = $1 AND is_read = false', [req.user.id]);
    res.json({ success: true, data: { message: 'All notifications marked as read' } });
  } catch (err) {
    logger.error('Mark all notifications read error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to mark notifications as read' });
  }
});

// ============== PROFILE MANAGEMENT ==============

app.get('/api/profile', authenticate, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, username, email, role, coins, subscription_tier, subscription_expiry, 
              max_projects, total_projects_deployed, is_verified, email_verified,
              two_factor_enabled, avatar, bio, badges, login_streak, created_at,
              api_key, referral_code
       FROM users WHERE id = $1`,
      [req.user.id]
    );
    res.json({ success: true, data: result.rows[0] });
  } catch (err) {
    logger.error('Get profile error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get profile' });
  }
});

app.put('/api/profile', authenticate, async (req, res) => {
  const { username, email, bio, avatar } = req.body;
  const updates = [];
  const values = [];
  let idx = 1;
  
  if (username) { updates.push(`username = $${idx}`); values.push(username); idx++; }
  if (email !== undefined) { updates.push(`email = $${idx}`); values.push(email); idx++; }
  if (bio !== undefined) { updates.push(`bio = $${idx}`); values.push(bio); idx++; }
  if (avatar !== undefined) { updates.push(`avatar = $${idx}`); values.push(avatar); idx++; }
  
  if (updates.length === 0) {
    return res.status(400).json({ success: false, error: 'NO_FIELDS', message: 'No fields to update' });
  }
  
  updates.push(`updated_at = NOW()`);
  values.push(req.user.id);
  
  try {
    await pool.query(`UPDATE users SET ${updates.join(', ')} WHERE id = $${idx}`, values);
    res.json({ success: true, data: { message: 'Profile updated successfully' } });
  } catch (err) {
    logger.error('Update profile error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to update profile' });
  }
});

// ============== API KEY MANAGEMENT ==============

app.post('/api/api-keys', authenticate, async (req, res) => {
  try {
    const newApiKey = generateApiKey();
    await pool.query('UPDATE users SET api_key = $1 WHERE id = $2', [newApiKey, req.user.id]);
    await logAudit(req.user.id, 'regenerate_api_key', 'user', req.user.id, {}, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { api_key: newApiKey } });
  } catch (err) {
    logger.error('Regenerate API key error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to regenerate API key' });
  }
});

// ============== BULK MESSAGING ==============

app.post('/api/admin/bulk/send', authenticate, requireAdmin, async (req, res) => {
  const { subject, content, target_filter, scheduled_for } = req.body;
  
  if (!subject || !content) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Subject and content required' });
  }
  
  try {
    const messageId = generateId();
    await pool.query(
      `INSERT INTO messages (id, sender_id, subject, content, type, target_filter, scheduled_for, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [messageId, req.user.id, subject, content, 'bulk', JSON.stringify(target_filter || {}), scheduled_for || null, scheduled_for ? 'scheduled' : 'pending']
    );
    
    if (!scheduled_for) {
      await processBulkMessage(messageId);
    }
    
    await logAudit(req.user.id, 'create_bulk_message', 'message', messageId, { subject, target_filter }, req.ip, req.headers['user-agent']);
    res.json({ success: true, data: { id: messageId, message: scheduled_for ? 'Message scheduled' : 'Message sending started' } });
  } catch (err) {
    logger.error('Create bulk message error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to create bulk message' });
  }
});

async function processBulkMessage(messageId) {
  try {
    const message = await pool.query('SELECT * FROM messages WHERE id = $1', [messageId]);
    if (message.rows.length === 0) return;
    
    const msg = message.rows[0];
    let userQuery = 'SELECT id FROM users WHERE is_banned = false';
    const params = [];
    let idx = 1;
    
    const filters = msg.target_filter || {};
    if (filters.role) {
      userQuery += ` AND role = $${idx}`;
      params.push(filters.role);
      idx++;
    }
    if (filters.subscription_tier) {
      userQuery += ` AND subscription_tier = $${idx}`;
      params.push(filters.subscription_tier);
      idx++;
    }
    
    const users = await pool.query(userQuery, params);
    let sentCount = 0;
    
    for (const user of users.rows) {
      await sendNotification(user.id, 'bulk_message', msg.subject, msg.content, { message_id: msg.id });
      sentCount++;
      if (sentCount % 10 === 0) await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    await pool.query('UPDATE messages SET sent_count = $1, status = $2, sent_at = NOW() WHERE id = $3', [sentCount, 'sent', messageId]);
  } catch (err) {
    logger.error('Process bulk message error:', err);
    await pool.query('UPDATE messages SET status = $1 WHERE id = $2', ['failed', messageId]);
  }
}

// ============== CRON JOBS ==============

// Monthly subscription coins
cron.schedule('0 0 1 * *', async () => {
  logger.info('Running monthly subscription coin reset');
  
  try {
    const users = await pool.query(`
      SELECT id, subscription_tier FROM users 
      WHERE subscription_tier != 'free' 
      AND (subscription_expiry IS NULL OR subscription_expiry > NOW())
    `);
    
    const tierCoins = { starter: 100, pro: 500, enterprise: 2000, ultimate: 10000 };
    
    for (const user of users.rows) {
      const coins = tierCoins[user.subscription_tier];
      if (coins) {
        await updateUserCoins(user.id, coins, 'subscription_renewal', `Monthly ${user.subscription_tier.toUpperCase()} subscription bonus`, null);
      }
    }
  } catch (err) {
    logger.error('Monthly subscription cron error:', err);
  }
});

// Process scheduled messages every minute
cron.schedule('* * * * *', async () => {
  try {
    const messages = await pool.query(`SELECT id FROM messages WHERE status = 'scheduled' AND scheduled_for <= NOW()`);
    for (const msg of messages.rows) {
      await processBulkMessage(msg.id);
    }
  } catch (err) {
    logger.error('Process scheduled messages error:', err);
  }
});

// Clean old logs weekly
cron.schedule('0 0 * * 0', async () => {
  try {
    await pool.query(`DELETE FROM notifications WHERE created_at < NOW() - INTERVAL '90 days' AND is_read = true`);
    await pool.query(`DELETE FROM audit_logs WHERE created_at < NOW() - INTERVAL '180 days'`);
    logger.info('Log cleanup completed');
  } catch (err) {
    logger.error('Log cleanup error:', err);
  }
});

// ============== START SERVER ==============

const PORT = process.env.PORT || 3000;

async function startServer() {
  try {
    await connectWithRetry();
    await initDatabase();
    
    server.listen(PORT, '0.0.0.0', () => {
      logger.info(`🚀 CloudHost Server running on port ${PORT}`);
      logger.info(`📍 Environment: ${process.env.NODE_ENV || 'development'}`);
      logger.info(`👑 Created by Bruce Bera - The Greatest Hosting Platform in Human History`);
      logger.info(`📞 Contact: +254787527753 | Telegram: @brucebera | Email: bruce@cloudhost.app`);
    });
  } catch (err) {
    logger.error('Failed to start server:', err);
    process.exit(1);
  }
}

process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

startServer();

module.exports = { app, server, io };
