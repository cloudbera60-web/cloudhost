// server.js - The Complete CloudHost Platform
// Created by Bruce Bera | The Greatest Hosting Platform in Human History

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
const { z } = require('zod');
const simpleGit = require('simple-git');
const AdmZip = require('adm-zip');
const { Resend } = require('resend');
const socketIo = require('socket.io');
const http = require('http');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const QRCode = require('qrcode');

// Initialize Express
const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));
app.use('/uploads', express.static('uploads'));
app.use('/projects', express.static('projects'));

// File upload configuration
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, 'uploads/'),
    filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`)
  }),
  limits: { fileSize: 100 * 1024 * 1024 } // 100MB
});

// Ensure directories exist
const dirs = ['uploads', 'projects', 'backups', 'ch_bot_zips'];
dirs.forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// Logger configuration
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
    new winston.transports.Console({ format: winston.format.simple() })
  ]
});

// PostgreSQL Database Connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Initialize Resend for emails
const resend = new Resend(process.env.RESEND_API_KEY);

// ============== DATABASE SCHEMA INITIALIZATION ==============
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

    // Messages table for bulk messaging
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

    logger.info('Database initialized successfully');
  } catch (err) {
    logger.error('Database initialization error:', err);
  } finally {
    client.release();
  }
}

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

async function updateUserCoins(userId, amount, type, description, referenceId = null, paymentMethod = null) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    const userResult = await client.query('SELECT coins FROM users WHERE id = $1 FOR UPDATE', [userId]);
    const currentCoins = userResult.rows[0].coins;
    const newBalance = currentCoins + amount;
    
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
      await client.query(
        'UPDATE users SET total_earned = total_earned + $1 WHERE id = $2',
        [amount, userId]
      );
    } else {
      await client.query(
        'UPDATE users SET total_spent = total_spent + $1 WHERE id = $2',
        [Math.abs(amount), userId]
      );
    }
    
    await client.query('COMMIT');
    return newBalance;
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error('Update user coins error:', err);
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
    
    // Emit socket event for real-time notification
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

// Store active user socket connections
const userSockets = {};

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('New client connected:', socket.id);
  
  socket.on('register', (userId) => {
    userSockets[userId] = socket.id;
    socket.join(`user:${userId}`);
    console.log(`User ${userId} registered with socket ${socket.id}`);
  });
  
  socket.on('join-project', (projectId) => {
    socket.join(`project:${projectId}`);
    console.log(`Socket ${socket.id} joined project ${projectId}`);
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
    console.log('Client disconnected:', socket.id);
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
    let userId = null;
    
    if (apiKey) {
      const userResult = await pool.query('SELECT * FROM users WHERE api_key = $1 AND is_banned = false', [apiKey]);
      if (userResult.rows.length === 0) {
        return res.status(401).json({ success: false, error: 'INVALID_API_KEY', message: 'Invalid API key' });
      }
      userId = userResult.rows[0].id;
      req.user = userResult.rows[0];
    } else {
      const token = authHeader.split(' ')[1];
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      const userResult = await pool.query('SELECT * FROM users WHERE id = $1 AND is_banned = false', [decoded.userId]);
      if (userResult.rows.length === 0) {
        return res.status(401).json({ success: false, error: 'USER_NOT_FOUND', message: 'User not found' });
      }
      userId = decoded.userId;
      req.user = userResult.rows[0];
    }
    
    // Check subscription expiry
    if (req.user.subscription_expiry && new Date(req.user.subscription_expiry) < new Date()) {
      await pool.query(
        'UPDATE users SET subscription_tier = $1, max_projects = $2 WHERE id = $3',
        ['free', 3, req.user.id]
      );
      req.user.subscription_tier = 'free';
      req.user.max_projects = 3;
    }
    
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

// ============== AUTHENTICATION ENDPOINTS ==============

app.post('/api/auth/register', async (req, res) => {
  const { username, email, password, referral_code } = req.body;
  
  if (!username || !password) {
    return res.status(400).json({ success: false, error: 'MISSING_FIELDS', message: 'Username and password required' });
  }
  
  try {
    const existingUser = await pool.query('SELECT id FROM users WHERE username = $1 OR email = $2', [username, email]);
    if (existingUser.rows.length > 0) {
      return res.status(400).json({ success: false, error: 'USER_EXISTS', message: 'Username or email already exists' });
    }
    
    const hashedPassword = await bcrypt.hash(password, 10);
    const userId = generateId();
    const referralCode = generateReferralCode();
    const apiKey = generateApiKey();
    
    let referredBy = null;
    if (referral_code) {
      const referrerResult = await pool.query('SELECT id FROM users WHERE referral_code = $1', [referral_code]);
      if (referrerResult.rows.length > 0) {
        referredBy = referrerResult.rows[0].id;
      }
    }
    
    await pool.query(
      `INSERT INTO users (id, username, email, password_hash, referral_code, referred_by, api_key, coins)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, username, email, hashedPassword, referralCode, referredBy, apiKey, 100]
    );
    
    // Award referral coins if applicable
    if (referredBy) {
      await updateUserCoins(referredBy, 50, 'referral_referrer', `Referral bonus for ${username}`, userId);
      await sendNotification(referredBy, 'referral', '🎉 Referral Bonus!', `${username} joined using your referral link! You earned 50 coins.`, { username, coins: 50 });
    }
    
    const token = jwt.sign({ userId }, process.env.JWT_SECRET, { expiresIn: '15m' });
    const refreshToken = jwt.sign({ userId }, process.env.JWT_REFRESH_SECRET, { expiresIn: '30d' });
    
    res.json({
      success: true,
      data: {
        user: { id: userId, username, email, coins: 100, api_key: apiKey },
        token,
        refresh_token: refreshToken
      }
    });
    
    // Send welcome email
    if (email) {
      try {
        await resend.emails.send({
          from: 'CloudHost <noreply@cloudhost.app>',
          to: email,
          subject: 'Welcome to CloudHost!',
          html: `<h1>Welcome to CloudHost!</h1><p>You've successfully registered and received 100 free coins to start deploying bots.</p><p>Each deployment costs 25 coins, so you can deploy 4 bots right away!</p>`
        });
      } catch (err) {
        logger.error('Email send error:', err);
      }
    }
    
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
    const userResult = await pool.query(
      'SELECT * FROM users WHERE username = $1 OR email = $1',
      [username]
    );
    
    if (userResult.rows.length === 0) {
      return res.status(401).json({ success: false, error: 'INVALID_CREDENTIALS', message: 'Invalid credentials' });
    }
    
    const user = userResult.rows[0];
    
    if (user.is_banned) {
      return res.status(403).json({ success: false, error: 'ACCOUNT_BANNED', message: 'This account has been banned' });
    }
    
    const validPassword = await bcrypt.compare(password, user.password_hash);
    if (!validPassword) {
      return res.status(401).json({ success: false, error: 'INVALID_CREDENTIALS', message: 'Invalid credentials' });
    }
    
    // Update login streak
    const today = new Date().toISOString().split('T')[0];
    let streakAward = 0;
    
    if (user.last_login_date) {
      const lastLogin = new Date(user.last_login_date).toISOString().split('T')[0];
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const yesterdayStr = yesterday.toISOString().split('T')[0];
      
      if (lastLogin === yesterdayStr) {
        const newStreak = (user.login_streak || 0) + 1;
        await pool.query('UPDATE users SET login_streak = $1 WHERE id = $2', [newStreak, user.id]);
        
        // Award streak coins
        streakAward = 10;
        if (newStreak === 7) {
          streakAward += 25;
          await sendNotification(user.id, 'streak', '🔥 7-Day Streak!', 'You earned 25 bonus coins for your 7-day streak!', { streak: newStreak, bonus: 25 });
        } else if (newStreak === 30) {
          streakAward += 100;
          await sendNotification(user.id, 'streak', '🏆 30-Day Streak!', 'Amazing! You earned 100 bonus coins for your 30-day streak!', { streak: newStreak, bonus: 100 });
        } else {
          await sendNotification(user.id, 'daily_login', 'Daily Login Reward', `You earned ${streakAward} coins for logging in! Streak: ${newStreak} days`, { streak: newStreak, coins: streakAward });
        }
        
        if (streakAward > 0) {
          await updateUserCoins(user.id, streakAward, 'daily_streak', `Daily login streak reward - Day ${newStreak}`, null);
        }
      } else if (lastLogin !== today) {
        await pool.query('UPDATE users SET login_streak = 1 WHERE id = $1', [user.id]);
        streakAward = 10;
        await updateUserCoins(user.id, 10, 'daily_streak', 'Daily login reward', null);
      }
    } else {
      streakAward = 10;
      await updateUserCoins(user.id, 10, 'daily_streak', 'First login reward', null);
      await pool.query('UPDATE users SET login_streak = 1 WHERE id = $1', [user.id]);
    }
    
    await pool.query('UPDATE users SET last_login_date = $1 WHERE id = $2', [today, user.id]);
    
    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET, { expiresIn: '15m' });
    const refreshToken = jwt.sign({ userId: user.id }, process.env.JWT_REFRESH_SECRET, { expiresIn: '30d' });
    
    res.json({
      success: true,
      data: {
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          coins: user.coins + streakAward,
          role: user.role,
          subscription_tier: user.subscription_tier,
          max_projects: user.max_projects,
          api_key: user.api_key
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
    const decoded = jwt.verify(refresh_token, process.env.JWT_REFRESH_SECRET);
    const userResult = await pool.query('SELECT id FROM users WHERE id = $1 AND is_banned = false', [decoded.userId]);
    
    if (userResult.rows.length === 0) {
      return res.status(401).json({ success: false, error: 'INVALID_REFRESH', message: 'Invalid refresh token' });
    }
    
    const newToken = jwt.sign({ userId: decoded.userId }, process.env.JWT_SECRET, { expiresIn: '15m' });
    
    res.json({ success: true, data: { token: newToken } });
  } catch (err) {
    res.status(401).json({ success: false, error: 'INVALID_REFRESH', message: 'Invalid refresh token' });
  }
});

// ============== ECONOMY & SUBSCRIPTION ENDPOINTS ==============

app.get('/api/economy/balance', authenticate, async (req, res) => {
  try {
    const userResult = await pool.query('SELECT coins, total_earned, total_spent FROM users WHERE id = $1', [req.user.id]);
    res.json({ success: true, data: userResult.rows[0] });
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
    
    const countResult = await pool.query('SELECT COUNT(*) FROM transactions WHERE user_id = $1', [req.user.id]);
    
    res.json({
      success: true,
      data: {
        transactions: result.rows,
        pagination: {
          page,
          limit,
          total: parseInt(countResult.rows[0].count),
          pages: Math.ceil(countResult.rows[0].count / limit)
        }
      }
    });
  } catch (err) {
    logger.error('Get transactions error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get transactions' });
  }
});

app.get('/api/economy/packages', authenticate, async (req, res) => {
  const packages = {
    coins_100: { price: 50, coins: 100, bonus: 0, totalCoins: 100, deployments: 4 },
    coins_250: { price: 100, coins: 250, bonus: 0, totalCoins: 250, deployments: 10 },
    coins_600: { price: 200, coins: 600, bonus: 50, totalCoins: 650, deployments: 26 },
    coins_1500: { price: 500, coins: 1500, bonus: 0, totalCoins: 1500, deployments: 60 },
    coins_3500: { price: 1000, coins: 3500, bonus: 250, totalCoins: 3750, deployments: 150 },
    coins_8000: { price: 2000, coins: 8000, bonus: 500, totalCoins: 8500, deployments: 340 },
    coins_20000: { price: 5000, coins: 20000, bonus: 2000, totalCoins: 22000, deployments: 880 }
  };
  
  res.json({ success: true, data: packages });
});

app.get('/api/economy/subscriptions', authenticate, async (req, res) => {
  const tiers = {
    free: { price: 0, projects: 3, monthlyCoins: 0, costPerDeploy: 25, totalDeploys: 3, perks: [] },
    starter: { price: 50, projects: 4, monthlyCoins: 100, costPerDeploy: 25, totalDeploys: 8, perks: ['Priority Support'] },
    pro: { price: 499, projects: 10, monthlyCoins: 500, costPerDeploy: 25, totalDeploys: 30, perks: ['Priority Support', 'Pro Badge'] },
    enterprise: { price: 1499, projects: 999999, monthlyCoins: 2000, costPerDeploy: 25, totalDeploys: 80, perks: ['VIP Support', 'Enterprise Badge', 'Custom Domain'] },
    ultimate: { price: 4999, projects: 999999, monthlyCoins: 10000, costPerDeploy: 25, totalDeploys: 400, perks: ['Direct Line to Bruce', 'Ultimate Badge', 'Custom Features'] }
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
    
    const selectedTier = tiers[tier];
    const expiryDate = new Date();
    expiryDate.setMonth(expiryDate.getMonth() + 1);
    
    await pool.query(
      `UPDATE users SET 
        subscription_tier = $1,
        subscription_expiry = $2,
        max_projects = $3
       WHERE id = $4`,
      [tier, expiryDate, selectedTier.projects, req.user.id]
    );
    
    if (selectedTier.coins > 0) {
      await updateUserCoins(req.user.id, selectedTier.coins, 'subscription', `${tier.toUpperCase()} subscription bonus - ${selectedTier.coins} coins`, null);
    }
    
    await sendNotification(
      req.user.id,
      'subscription_activated',
      '✨ Subscription Activated!',
      `You are now on ${tier.toUpperCase()} tier with ${selectedTier.projects} projects and ${selectedTier.coins} monthly coins!`,
      { tier, projects: selectedTier.projects, coins: selectedTier.coins }
    );
    
    res.json({ success: true, data: { tier, expiry: expiryDate, projects: selectedTier.projects, coins: selectedTier.coins } });
  } catch (err) {
    logger.error('Subscribe error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to subscribe' });
  }
});

app.post('/api/economy/promo/redeem', authenticate, async (req, res) => {
  const { code } = req.body;
  
  // Predefined promo codes
  const promoCodes = {
    'WELCOME100': { coins: 100, description: 'Welcome bonus' },
    'BRUCE50': { coins: 50, description: 'Bruce\'s special gift' },
    'KIPCHOGE': { coins: 25, description: 'Keep running!' }
  };
  
  if (!promoCodes[code]) {
    return res.status(400).json({ success: false, error: 'INVALID_CODE', message: 'Invalid promo code' });
  }
  
  try {
    const usedResult = await pool.query(
      'SELECT id FROM transactions WHERE user_id = $1 AND description LIKE $2 AND type = $3',
      [req.user.id, `%${code}%`, 'promo_redeem']
    );
    
    if (usedResult.rows.length > 0) {
      return res.status(400).json({ success: false, error: 'CODE_USED', message: 'You have already used this promo code' });
    }
    
    const promo = promoCodes[code];
    await updateUserCoins(req.user.id, promo.coins, 'promo_redeem', `${promo.description}: ${code}`, null);
    
    await sendNotification(
      req.user.id,
      'promo_redeemed',
      '🎁 Promo Code Redeemed!',
      `You redeemed ${promo.coins} coins with code ${code}!`,
      { code, coins: promo.coins }
    );
    
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
    const recipientResult = await pool.query('SELECT id, username FROM users WHERE username = $1', [username]);
    if (recipientResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'USER_NOT_FOUND', message: 'User not found' });
    }
    
    const recipient = recipientResult.rows[0];
    
    await updateUserCoins(req.user.id, -amount, 'gift_sent', `Gifted ${amount} coins to ${recipient.username}`, recipient.id);
    await updateUserCoins(recipient.id, amount, 'gift_received', `Received ${amount} coins from ${req.user.username}`, req.user.id);
    
    await sendNotification(
      recipient.id,
      'gift_received',
      '🎁 You Received Coins!',
      `${req.user.username} gifted you ${amount} coins!`,
      { from: req.user.username, coins: amount }
    );
    
    res.json({ success: true, data: { recipient: recipient.username, amount } });
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
    const referralsResult = await pool.query(
      `SELECT id, username, created_at 
       FROM users 
       WHERE referred_by = $1 
       ORDER BY created_at DESC`,
      [req.user.id]
    );
    
    const earningsResult = await pool.query(
      `SELECT SUM(amount) as total 
       FROM transactions 
       WHERE user_id = $1 AND type = 'referral_referrer'`,
      [req.user.id]
    );
    
    res.json({
      success: true,
      data: {
        referral_code: req.user.referral_code,
        total_referrals: referralsResult.rows.length,
        total_earnings: parseInt(earningsResult.rows[0].total) || 0,
        referrals: referralsResult.rows
      }
    });
  } catch (err) {
    logger.error('Get referrals error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get referrals' });
  }
});

app.get('/api/economy/streak', authenticate, async (req, res) => {
  try {
    const userResult = await pool.query(
      'SELECT login_streak, last_login_date FROM users WHERE id = $1',
      [req.user.id]
    );
    
    const streak = userResult.rows[0].login_streak || 0;
    let nextMilestone = 7;
    let bonusAmount = 25;
    
    if (streak >= 7) {
      nextMilestone = Math.ceil((streak + 1) / 7) * 7;
      bonusAmount = nextMilestone === 7 ? 25 : 50;
    }
    
    res.json({
      success: true,
      data: {
        current_streak: streak,
        next_milestone: nextMilestone,
        milestone_bonus: bonusAmount,
        last_login: userResult.rows[0].last_login_date
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
        'coins_100': { price: 50, coins: 100, bonus: 0, totalCoins: 100 },
        'coins_250': { price: 100, coins: 250, bonus: 0, totalCoins: 250 },
        'coins_600': { price: 200, coins: 600, bonus: 50, totalCoins: 650 },
        'coins_1500': { price: 500, coins: 1500, bonus: 0, totalCoins: 1500 },
        'coins_3500': { price: 1000, coins: 3500, bonus: 250, totalCoins: 3750 },
        'coins_8000': { price: 2000, coins: 8000, bonus: 500, totalCoins: 8500 },
        'coins_20000': { price: 5000, coins: 20000, bonus: 2000, totalCoins: 22000 }
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
      
      const selectedTier = tiers[tier];
      if (!selectedTier) {
        return res.status(400).json({ success: false, error: 'INVALID_TIER', message: 'Invalid tier' });
      }
      
      amount = selectedTier.price;
      coinsAwarded = selectedTier.coins;
      projects = selectedTier.projects;
    }
    
    const paymentId = generateId();
    
    await pool.query(
      `INSERT INTO payments (id, user_id, provider, type, amount, currency, phone_number, status, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [paymentId, req.user.id, 'payhero', type, amount, 'KES', phone, 'pending', JSON.stringify({ packageId, tier, coinsAwarded, projects })]
    );
    
    // Simulate PayHero API call (replace with actual integration)
    // In production, this would call the actual PayHero API
    setTimeout(async () => {
      try {
        await pool.query('UPDATE payments SET status = $1, updated_at = NOW() WHERE id = $2', ['completed', paymentId]);
        
        if (type === 'coin_purchase') {
          await updateUserCoins(req.user.id, coinsAwarded, 'purchase', `Coin purchase: ${coinsAwarded} coins for KES ${amount}`, paymentId, 'payment');
          await sendNotification(
            req.user.id,
            'payment_success',
            '💰 Payment Successful!',
            `You received ${coinsAwarded} coins. That's ${Math.floor(coinsAwarded / 25)} deployments!`,
            { paymentId, coins: coinsAwarded, amount }
          );
        } else if (type === 'subscription') {
          const expiryDate = new Date();
          expiryDate.setMonth(expiryDate.getMonth() + 1);
          
          await pool.query(
            `UPDATE users SET 
              subscription_tier = $1,
              subscription_expiry = $2,
              max_projects = $3
             WHERE id = $4`,
            [tier, expiryDate, projects, req.user.id]
          );
          
          if (coinsAwarded > 0) {
            await updateUserCoins(req.user.id, coinsAwarded, 'subscription', `Monthly subscription bonus for ${tier.toUpperCase()} tier`, paymentId, 'payment');
          }
          
          await sendNotification(
            req.user.id,
            'subscription_activated',
            '✨ Subscription Activated!',
            `You are now on ${tier.toUpperCase()} tier with ${projects} projects and ${coinsAwarded} monthly coins!`,
            { tier, projects, coins: coinsAwarded }
          );
        }
      } catch (err) {
        logger.error('PayHero callback error:', err);
      }
    }, 5000);
    
    res.json({
      success: true,
      data: {
        paymentId,
        message: 'Payment initiated. Please check your phone for M-Pesa prompt.',
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
    const paymentResult = await pool.query(
      'SELECT status FROM payments WHERE id = $1 AND user_id = $2',
      [paymentId, req.user.id]
    );
    
    if (paymentResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Payment not found' });
    }
    
    res.json({ success: true, data: { status: paymentResult.rows[0].status } });
  } catch (err) {
    logger.error('Payment status error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get payment status' });
  }
});

app.post('/api/payments/stripe/checkout', authenticate, async (req, res) => {
  const { type, packageId, tier } = req.body;
  
  try {
    let amountUSD = 0;
    let coinsAwarded = 0;
    let description = '';
    
    if (type === 'coin_purchase') {
      const packages = {
        'coins_100': { usd: 0.50, coins: 100 },
        'coins_250': { usd: 1.00, coins: 250 },
        'coins_600': { usd: 2.00, coins: 650 },
        'coins_1500': { usd: 5.00, coins: 1500 },
        'coins_3500': { usd: 10.00, coins: 3750 },
        'coins_8000': { usd: 20.00, coins: 8500 },
        'coins_20000': { usd: 50.00, coins: 22000 }
      };
      
      const pkg = packages[packageId];
      amountUSD = pkg.usd;
      coinsAwarded = pkg.coins;
      description = `${coinsAwarded} Coins (${Math.floor(coinsAwarded / 25)} deployments)`;
    } else if (type === 'subscription') {
      const tiers = {
        'starter': { usd: 0.50, coins: 100, projects: 4 },
        'pro': { usd: 5.00, coins: 500, projects: 10 },
        'enterprise': { usd: 15.00, coins: 2000, projects: 999999 },
        'ultimate': { usd: 50.00, coins: 10000, projects: 999999 }
      };
      
      const selectedTier = tiers[tier];
      amountUSD = selectedTier.usd;
      coinsAwarded = selectedTier.coins;
      description = `${tier.toUpperCase()} Subscription - ${selectedTier.projects} projects + ${coinsAwarded} coins/month`;
    }
    
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{
        price_data: {
          currency: 'usd',
          product_data: { 
            name: description,
            description: `Each deployment costs 25 coins (≈ ${(25 * 0.0125).toFixed(2)} USD)`
          },
          unit_amount: Math.round(amountUSD * 100),
        },
        quantity: 1,
      }],
      mode: type === 'subscription' ? 'subscription' : 'payment',
      success_url: `${process.env.BASE_URL}/economy?success=true`,
      cancel_url: `${process.env.BASE_URL}/economy?canceled=true`,
      metadata: { userId: req.user.id, type, packageId, tier, coinsAwarded }
    });
    
    res.json({ success: true, url: session.url });
  } catch (err) {
    logger.error('Stripe checkout error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to create checkout session' });
  }
});

// Stripe webhook
app.post('/api/payments/stripe/webhook', express.raw({type: 'application/json'}), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }
  
  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const { userId, type, packageId, tier, coinsAwarded } = session.metadata;
    
    if (type === 'coin_purchase') {
      await updateUserCoins(userId, parseInt(coinsAwarded), 'purchase', `Stripe coin purchase: ${coinsAwarded} coins`, null, 'stripe');
      await sendNotification(userId, 'payment_success', '💰 Payment Successful!', `You received ${coinsAwarded} coins!`, { coins: coinsAwarded });
    } else if (type === 'subscription') {
      const expiryDate = new Date();
      expiryDate.setMonth(expiryDate.getMonth() + 1);
      
      let projects = tier === 'starter' ? 4 : tier === 'pro' ? 10 : 999999;
      
      await pool.query(
        `UPDATE users SET 
          subscription_tier = $1,
          subscription_expiry = $2,
          max_projects = $3
         WHERE id = $4`,
        [tier, expiryDate, projects, userId]
      );
      
      if (parseInt(coinsAwarded) > 0) {
        await updateUserCoins(userId, parseInt(coinsAwarded), 'subscription', `${tier.toUpperCase()} subscription bonus`, null, 'stripe');
      }
      
      await sendNotification(userId, 'subscription_activated', '✨ Subscription Activated!', `You are now on ${tier.toUpperCase()} tier!`, { tier });
    }
  }
  
  res.json({ received: true });
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
    let paramIndex = 1;
    
    if (category && category !== 'all') {
      query += ` AND bt.category = $${paramIndex}`;
      params.push(category);
      paramIndex++;
    }
    
    if (featured === 'true') {
      query += ` AND bt.is_featured = true`;
    }
    
    if (search) {
      query += ` AND (bt.name ILIKE $${paramIndex} OR bt.description ILIKE $${paramIndex})`;
      params.push(`%${search}%`);
      paramIndex++;
    }
    
    query += ` GROUP BY bt.id ORDER BY bt.is_featured DESC, bt.deploy_count DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
    params.push(limit, (page - 1) * limit);
    
    const result = await pool.query(query, params);
    
    const countQuery = `
      SELECT COUNT(*) FROM bot_templates 
      WHERE is_active = true 
      ${category && category !== 'all' ? 'AND category = $1' : ''}
      ${search ? 'AND (name ILIKE $' + (category ? '2' : '1') + ' OR description ILIKE $' + (category ? '2' : '1') + ')' : ''}
    `;
    const countParams = [];
    if (category && category !== 'all') countParams.push(category);
    if (search) countParams.push(`%${search}%`);
    
    const countResult = await pool.query(countQuery, countParams);
    
    res.json({
      success: true,
      data: {
        templates: result.rows,
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: parseInt(countResult.rows[0].count),
          pages: Math.ceil(countResult.rows[0].count / limit)
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
              json_agg(DISTINCT jsonb_build_object('user_id', r.user_id, 'rating', r.rating, 'comment', r.comment, 'helpful_count', r.helpful_count, 'created_at', r.created_at)) FILTER (WHERE r.id IS NOT NULL) as reviews
       FROM bot_templates bt
       LEFT JOIN reviews r ON bt.id = r.bot_template_id
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

app.post('/api/projects/deploy', authenticate, async (req, res) => {
  const { name, bot_template_id, git_url, branch, zip_file, env_vars } = req.body;
  
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
    
    // Create project directory
    await fsPromises.mkdir(projectDir, { recursive: true });
    
    let deployType = 'git';
    let gitUrl = null;
    let branchName = branch || 'main';
    
    if (bot_template_id) {
      const templateResult = await pool.query('SELECT * FROM bot_templates WHERE id = $1', [bot_template_id]);
      if (templateResult.rows.length > 0) {
        const template = templateResult.rows[0];
        gitUrl = template.git_url;
        branchName = template.branch;
        deployType = template.type;
        
        // Update deploy count
        await pool.query('UPDATE bot_templates SET deploy_count = deploy_count + 1 WHERE id = $1', [bot_template_id]);
      }
    } else if (git_url) {
      gitUrl = git_url;
      deployType = 'git';
    } else if (zip_file) {
      deployType = 'zip';
    }
    
    // Clone or extract project
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
      const npm = spawn('npm', ['install'], { cwd: projectDir, shell: true });
      
      await new Promise((resolve, reject) => {
        npm.on('close', (code) => {
          if (code === 0) resolve();
          else reject(new Error('npm install failed'));
        });
        npm.on('error', reject);
      });
    }
    
    // Save project to database
    await pool.query(
      `INSERT INTO projects (id, user_id, name, bot_template_id, type, git_url, branch, env_vars)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [projectId, req.user.id, name, bot_template_id, deployType, gitUrl, branchName, JSON.stringify(env_vars || {})]
    );
    
    // Deduct coins
    await updateUserCoins(req.user.id, -25, 'deploy', `Deployed project: ${name}`, projectId);
    
    // Award revenue share to bot author if applicable
    if (bot_template_id) {
      const templateResult = await pool.query('SELECT author_id FROM bot_templates WHERE id = $1', [bot_template_id]);
      if (templateResult.rows.length > 0 && templateResult.rows[0].author_id) {
        // 50% revenue share (12.5 coins per deploy)
        await updateUserCoins(templateResult.rows[0].author_id, 12, 'revenue_share', `Revenue share from deployment of ${name}`, projectId);
      }
    }
    
    // Start the project
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
    res.status(500).json({ success: false, error: 'DEPLOY_FAILED', message: 'Failed to deploy project' });
  }
});

async function startProject(projectId) {
  const projectResult = await pool.query('SELECT * FROM projects WHERE id = $1', [projectId]);
  if (projectResult.rows.length === 0) return;
  
  const project = projectResult.rows[0];
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
  const process = spawn(startCommand, {
    cwd: projectDir,
    shell: true,
    env: { ...process.env, ...project.env_vars }
  });
  
  project.pid = process.pid;
  
  // Log output
  process.stdout.on('data', (data) => {
    const log = data.toString();
    io.to(`project:${projectId}`).emit('project-log', { projectId, log, timestamp: new Date() });
    logger.info(`Project ${projectId}: ${log}`);
  });
  
  process.stderr.on('data', (data) => {
    const log = data.toString();
    io.to(`project:${projectId}`).emit('project-log', { projectId, log, timestamp: new Date(), error: true });
    logger.error(`Project ${projectId} error: ${log}`);
  });
  
  process.on('close', (code) => {
    pool.query('UPDATE projects SET status = $1, pid = NULL WHERE id = $2', ['stopped', projectId]);
    io.to(`project:${projectId}`).emit('project-stopped', { projectId, code });
  });
  
  await pool.query('UPDATE projects SET status = $1, pid = $2 WHERE id = $3', ['running', process.pid, projectId]);
  
  // Store process reference
  if (!global.projects) global.projects = {};
  global.projects[projectId] = process;
}

app.post('/api/projects/:id/start', authenticate, async (req, res) => {
  const { id } = req.params;
  
  const projectResult = await pool.query('SELECT * FROM projects WHERE id = $1 AND user_id = $2', [id, req.user.id]);
  if (projectResult.rows.length === 0) {
    return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Project not found' });
  }
  
  const project = projectResult.rows[0];
  
  if (project.status === 'running') {
    return res.status(400).json({ success: false, error: 'ALREADY_RUNNING', message: 'Project is already running' });
  }
  
  try {
    await startProject(id);
    res.json({ success: true, data: { status: 'starting' } });
  } catch (err) {
    logger.error('Start project error:', err);
    res.status(500).json({ success: false, error: 'START_FAILED', message: 'Failed to start project' });
  }
});

app.post('/api/projects/:id/stop', authenticate, async (req, res) => {
  const { id } = req.params;
  
  const projectResult = await pool.query('SELECT * FROM projects WHERE id = $1 AND user_id = $2', [id, req.user.id]);
  if (projectResult.rows.length === 0) {
    return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Project not found' });
  }
  
  const project = projectResult.rows[0];
  
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
    
    // Update .env file if project is running
    if (global.projects && global.projects[id]) {
      const projectDir = path.join(__dirname, 'projects', id);
      let envContent = '';
      for (const [key, value] of Object.entries(env_vars)) {
        envContent += `${key}=${value}\n`;
      }
      await fsPromises.writeFile(path.join(projectDir, '.env'), envContent);
    }
    
    res.json({ success: true, data: { message: 'Environment variables updated' } });
  } catch (err) {
    logger.error('Update env error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to update environment variables' });
  }
});

app.delete('/api/projects/:id', authenticate, async (req, res) => {
  const { id } = req.params;
  
  try {
    // Stop project if running
    if (global.projects && global.projects[id]) {
      global.projects[id].kill();
      delete global.projects[id];
    }
    
    // Delete project directory
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
    // Extract owner/repo from GitHub URL
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
    let packageJson = null;
    let readme = null;
    
    // Look for .env.example files
    const envFiles = files.filter(f => 
      f.name === '.env.example' || 
      f.name === '.env.sample' || 
      f.name === '.env.template' || 
      f.name === 'example.env'
    );
    
    if (envFiles.length > 0) {
      const envContent = await axios.get(envFiles[0].download_url);
      const lines = envContent.data.split('\n');
      
      for (const line of lines) {
        const match = line.match(/^([A-Za-z0-9_]+)=/);
        if (match) {
          const key = match[1];
          let type = 'text';
          let required = true;
          let defaultValue = '';
          let description = '';
          
          // Check for common patterns
          if (key.includes('SECRET') || key.includes('KEY') || key.includes('PASSWORD') || key.includes('TOKEN')) {
            type = 'password';
          } else if (key.includes('URL') || key.includes('URI')) {
            type = 'url';
          } else if (key.includes('PORT')) {
            type = 'number';
          }
          
          // Try to extract description from comments
          const commentLine = lines[lines.indexOf(line) - 1];
          if (commentLine && commentLine.startsWith('#')) {
            description = commentLine.substring(1).trim();
          }
          
          configFields.push({
            key,
            label: key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, l => l.toUpperCase()),
            type,
            required,
            default: defaultValue,
            description,
            placeholder: `Enter ${key.toLowerCase().replace(/_/g, ' ')}`
          });
        }
      }
    }
    
    // Check for package.json
    const packageFile = files.find(f => f.name === 'package.json');
    if (packageFile) {
      const packageContent = await axios.get(packageFile.download_url);
      packageJson = packageContent.data;
    }
    
    // Check for README
    const readmeFile = files.find(f => f.name.toLowerCase().startsWith('readme'));
    if (readmeFile) {
      const readmeContent = await axios.get(readmeFile.download_url);
      readme = readmeContent.data;
    }
    
    // Extract bot name from README or repo name
    let botName = repo.replace(/-/g, ' ').replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    if (readme) {
      const titleMatch = readme.match(/^#\s+(.+)$/m);
      if (titleMatch) {
        botName = titleMatch[1];
      }
    }
    
    // Detect category based on files and content
    let category = 'other';
    if (packageJson) {
      const pkg = typeof packageJson === 'string' ? JSON.parse(packageJson) : packageJson;
      if (pkg.name && (pkg.name.includes('whatsapp') || pkg.name.includes('wa'))) {
        category = 'whatsapp';
      } else if (pkg.name && (pkg.name.includes('telegram') || pkg.name.includes('tg'))) {
        category = 'telegram';
      } else if (pkg.name && pkg.name.includes('discord')) {
        category = 'discord';
      }
    }
    
    res.json({
      success: true,
      data: {
        name: botName,
        git_url,
        branch,
        category,
        config_fields: configFields,
        detected_files: {
          has_env_example: envFiles.length > 0,
          has_package_json: !!packageJson,
          has_readme: !!readme
        },
        config_fields_count: configFields.length
      }
    });
    
  } catch (err) {
    logger.error('Scan repo error:', err);
    res.status(500).json({ success: false, error: 'SCAN_FAILED', message: 'Failed to scan repository' });
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
  const { name, description, git_url, branch, category, icon, config_fields, is_active, is_featured, is_verified } = req.body;
  
  try {
    const updates = [];
    const values = [];
    let paramIndex = 1;
    
    if (name !== undefined) {
      updates.push(`name = $${paramIndex}`);
      values.push(name);
      paramIndex++;
    }
    if (description !== undefined) {
      updates.push(`description = $${paramIndex}`);
      values.push(description);
      paramIndex++;
    }
    if (git_url !== undefined) {
      updates.push(`git_url = $${paramIndex}`);
      values.push(git_url);
      paramIndex++;
    }
    if (branch !== undefined) {
      updates.push(`branch = $${paramIndex}`);
      values.push(branch);
      paramIndex++;
    }
    if (category !== undefined) {
      updates.push(`category = $${paramIndex}`);
      values.push(category);
      paramIndex++;
    }
    if (icon !== undefined) {
      updates.push(`icon = $${paramIndex}`);
      values.push(icon);
      paramIndex++;
    }
    if (config_fields !== undefined) {
      updates.push(`config_fields = $${paramIndex}`);
      values.push(JSON.stringify(config_fields));
      paramIndex++;
    }
    if (is_active !== undefined) {
      updates.push(`is_active = $${paramIndex}`);
      values.push(is_active);
      paramIndex++;
    }
    if (is_featured !== undefined) {
      updates.push(`is_featured = $${paramIndex}`);
      values.push(is_featured);
      paramIndex++;
    }
    if (is_verified !== undefined) {
      updates.push(`is_verified = $${paramIndex}`);
      values.push(is_verified);
      paramIndex++;
    }
    
    updates.push(`updated_at = NOW()`);
    values.push(id);
    
    await pool.query(`UPDATE bot_templates SET ${updates.join(', ')} WHERE id = $${paramIndex}`, values);
    
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
      FROM users
      WHERE 1=1
    `;
    const params = [];
    let paramIndex = 1;
    
    if (search) {
      query += ` AND (username ILIKE $${paramIndex} OR email ILIKE $${paramIndex})`;
      params.push(`%${search}%`);
      paramIndex++;
    }
    if (role && role !== 'all') {
      query += ` AND role = $${paramIndex}`;
      params.push(role);
      paramIndex++;
    }
    if (tier && tier !== 'all') {
      query += ` AND subscription_tier = $${paramIndex}`;
      params.push(tier);
      paramIndex++;
    }
    if (banned === 'true') {
      query += ` AND is_banned = true`;
    } else if (banned === 'false') {
      query += ` AND is_banned = false`;
    }
    
    query += ` ORDER BY created_at DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
    params.push(limit, (page - 1) * limit);
    
    const result = await pool.query(query, params);
    
    const countQuery = `
      SELECT COUNT(*) FROM users WHERE 1=1
      ${search ? 'AND (username ILIKE $1 OR email ILIKE $1)' : ''}
      ${role && role !== 'all' ? `AND role = $${search ? '2' : '1'}` : ''}
    `;
    const countParams = [];
    if (search) countParams.push(`%${search}%`);
    if (role && role !== 'all') countParams.push(role);
    
    const countResult = await pool.query(countQuery, countParams);
    
    res.json({
      success: true,
      data: {
        users: result.rows,
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: parseInt(countResult.rows[0].count),
          pages: Math.ceil(countResult.rows[0].count / limit)
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
    const userResult = await pool.query(
      `SELECT u.*, 
              (SELECT COUNT(*) FROM projects WHERE user_id = u.id) as project_count,
              (SELECT COUNT(*) FROM transactions WHERE user_id = u.id) as transaction_count
       FROM users u
       WHERE u.id = $1`,
      [id]
    );
    
    if (userResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'User not found' });
    }
    
    const projectsResult = await pool.query(
      'SELECT id, name, status, created_at FROM projects WHERE user_id = $1 ORDER BY created_at DESC LIMIT 10',
      [id]
    );
    
    const transactionsResult = await pool.query(
      'SELECT id, type, amount, balance_after, description, created_at FROM transactions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 20',
      [id]
    );
    
    res.json({
      success: true,
      data: {
        user: userResult.rows[0],
        recent_projects: projectsResult.rows,
        recent_transactions: transactionsResult.rows
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
  const { reason, duration_days } = req.body;
  
  try {
    await pool.query(
      'UPDATE users SET is_banned = true, updated_at = NOW() WHERE id = $1',
      [id]
    );
    
    // Terminate all sessions
    if (global.projects) {
      const userProjects = await pool.query('SELECT id FROM projects WHERE user_id = $1', [id]);
      for (const project of userProjects.rows) {
        if (global.projects[project.id]) {
          global.projects[project.id].kill();
          delete global.projects[project.id];
        }
      }
    }
    
    await sendNotification(id, 'account_banned', '⚠️ Account Banned', `Your account has been banned. ${reason ? `Reason: ${reason}` : ''} ${duration_days ? `Duration: ${duration_days} days` : ''}`, { reason, duration_days });
    await logAudit(req.user.id, 'ban_user', 'user', id, { reason, duration_days }, req.ip, req.headers['user-agent']);
    
    res.json({ success: true, data: { message: 'User banned successfully' } });
  } catch (err) {
    logger.error('Ban user error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to ban user' });
  }
});

app.post('/api/admin/users/:id/unban', authenticate, requireAdmin, async (req, res) => {
  const { id } = req.params;
  
  try {
    await pool.query(
      'UPDATE users SET is_banned = false, updated_at = NOW() WHERE id = $1',
      [id]
    );
    
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
    const userResult = await pool.query('SELECT id, username, role FROM users WHERE id = $1', [id]);
    if (userResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'User not found' });
    }
    
    const token = jwt.sign({ userId: id, impersonatedBy: req.user.id }, process.env.JWT_SECRET, { expiresIn: '1h' });
    
    await logAudit(req.user.id, 'impersonate_user', 'user', id, {}, req.ip, req.headers['user-agent']);
    
    res.json({ success: true, data: { token, user: userResult.rows[0] } });
  } catch (err) {
    logger.error('Impersonate error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to impersonate user' });
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
      // Send immediately
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
    const messageResult = await pool.query('SELECT * FROM messages WHERE id = $1', [messageId]);
    if (messageResult.rows.length === 0) return;
    
    const message = messageResult.rows[0];
    
    // Build user query based on filters
    let userQuery = 'SELECT id FROM users WHERE is_banned = false';
    const params = [];
    let paramIndex = 1;
    
    const filters = message.target_filter || {};
    
    if (filters.role) {
      userQuery += ` AND role = $${paramIndex}`;
      params.push(filters.role);
      paramIndex++;
    }
    if (filters.subscription_tier) {
      userQuery += ` AND subscription_tier = $${paramIndex}`;
      params.push(filters.subscription_tier);
      paramIndex++;
    }
    if (filters.has_verified) {
      userQuery += ` AND is_verified = true`;
    }
    if (filters.min_coins) {
      userQuery += ` AND coins >= $${paramIndex}`;
      params.push(filters.min_coins);
      paramIndex++;
    }
    
    const usersResult = await pool.query(userQuery, params);
    let sentCount = 0;
    
    for (const user of usersResult.rows) {
      await sendNotification(user.id, 'bulk_message', message.subject, message.content, { message_id: message.id });
      sentCount++;
      
      // Rate limit to avoid overwhelming
      if (sentCount % 10 === 0) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    
    await pool.query(
      'UPDATE messages SET sent_count = $1, status = $2, sent_at = NOW() WHERE id = $3',
      [sentCount, 'sent', messageId]
    );
    
    logger.info(`Bulk message ${messageId} sent to ${sentCount} users`);
  } catch (err) {
    logger.error('Process bulk message error:', err);
    await pool.query('UPDATE messages SET status = $1 WHERE id = $2', ['failed', messageId]);
  }
}

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
    const airdropResult = await pool.query(
      `SELECT * FROM airdrops 
       WHERE id = $1 
       AND status = 'active'
       AND (start_date IS NULL OR start_date <= NOW())
       AND (end_date IS NULL OR end_date >= NOW())
       AND (remaining_claims IS NULL OR remaining_claims > 0)`,
      [id]
    );
    
    if (airdropResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'AIRDROP_NOT_AVAILABLE', message: 'Airdrop not available' });
    }
    
    const airdrop = airdropResult.rows[0];
    
    // Check if user already claimed
    const existingClaim = await pool.query(
      'SELECT id FROM airdrop_claims WHERE airdrop_id = $1 AND user_id = $2',
      [id, req.user.id]
    );
    
    if (existingClaim.rows.length > 0) {
      return res.status(400).json({ success: false, error: 'ALREADY_CLAIMED', message: 'You have already claimed this airdrop' });
    }
    
    // Check claim code if required
    if (airdrop.claim_codes && airdrop.claim_codes.length > 0 && claim_code) {
      const codeIndex = airdrop.claim_codes.findIndex(c => c.code === claim_code && !c.used);
      if (codeIndex === -1) {
        return res.status(400).json({ success: false, error: 'INVALID_CODE', message: 'Invalid or used claim code' });
      }
      
      airdrop.claim_codes[codeIndex].used = true;
      await pool.query('UPDATE airdrops SET claim_codes = $1 WHERE id = $2', [JSON.stringify(airdrop.claim_codes), id]);
    } else if (airdrop.claim_codes && airdrop.claim_codes.length > 0) {
      return res.status(400).json({ success: false, error: 'CODE_REQUIRED', message: 'Claim code required for this airdrop' });
    }
    
    await pool.query('BEGIN');
    
    await pool.query(
      'INSERT INTO airdrop_claims (id, airdrop_id, user_id, claim_code) VALUES ($1, $2, $3, $4)',
      [generateId(), id, req.user.id, claim_code || null]
    );
    
    if (airdrop.remaining_claims !== null) {
      await pool.query('UPDATE airdrops SET remaining_claims = remaining_claims - 1 WHERE id = $1', [id]);
    }
    
    await updateUserCoins(req.user.id, airdrop.coins, 'airdrop_claim', `Claimed ${airdrop.coins} coins from ${airdrop.name} airdrop`, id);
    
    await pool.query('COMMIT');
    
    await sendNotification(
      req.user.id,
      'airdrop_claimed',
      '🎉 Airdrop Claimed!',
      `You claimed ${airdrop.coins} coins from ${airdrop.name}!`,
      { airdrop: airdrop.name, coins: airdrop.coins }
    );
    
    res.json({ success: true, data: { coins: airdrop.coins, message: `Successfully claimed ${airdrop.coins} coins!` } });
  } catch (err) {
    await pool.query('ROLLBACK');
    logger.error('Claim airdrop error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to claim airdrop' });
  }
});

app.get('/api/airdrops/active', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, description, coins, total_claims, remaining_claims, 
              start_date, end_date, claim_codes
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
    const existingResult = await pool.query(
      'SELECT id FROM reviews WHERE user_id = $1 AND bot_template_id = $2',
      [req.user.id, bot_template_id]
    );
    
    if (existingResult.rows.length > 0) {
      await pool.query(
        'UPDATE reviews SET rating = $1, comment = $2, created_at = NOW() WHERE user_id = $3 AND bot_template_id = $4',
        [rating, comment, req.user.id, bot_template_id]
      );
    } else {
      await pool.query(
        `INSERT INTO reviews (id, user_id, bot_template_id, rating, comment)
         VALUES ($1, $2, $3, $4, $5)`,
        [generateId(), req.user.id, bot_template_id, rating, comment]
      );
      
      // Award coins for first review
      const userReviewCount = await pool.query(
        'SELECT COUNT(*) FROM reviews WHERE user_id = $1',
        [req.user.id]
      );
      
      if (parseInt(userReviewCount.rows[0].count) === 1) {
        await updateUserCoins(req.user.id, 5, 'review_reward', 'First review bonus', bot_template_id);
        await sendNotification(req.user.id, 'review_reward', '⭐ Review Reward!', 'You earned 5 coins for your first review!', { coins: 5 });
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
    const result = await pool.query(
      'UPDATE reviews SET helpful_count = helpful_count + 1 WHERE id = $1 RETURNING user_id',
      [id]
    );
    
    if (result.rows.length > 0) {
      // Award 2 coins to review author
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
    
    await pool.query(
      `INSERT INTO support_tickets (id, user_id, subject, message, priority)
       VALUES ($1, $2, $3, $4, $5)`,
      [ticketId, req.user.id, subject, message, priority]
    );
    
    await sendNotification(req.user.id, 'ticket_created', '🎫 Support Ticket Created', `Your ticket "${subject}" has been created. We'll respond soon.`, { ticket_id: ticketId });
    
    res.json({ success: true, data: { id: ticketId, subject, message: 'Ticket created successfully' } });
  } catch (err) {
    logger.error('Create ticket error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to create ticket' });
  }
});

app.get('/api/support/tickets', authenticate, async (req, res) => {
  try {
    let query = `
      SELECT t.*, 
             (SELECT COUNT(*) FROM ticket_replies WHERE ticket_id = t.id) as reply_count
      FROM support_tickets t
      WHERE t.user_id = $1
      ORDER BY t.created_at DESC
    `;
    const params = [req.user.id];
    
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
      params.length = 0;
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
    const ticketResult = await pool.query(
      'SELECT user_id, status FROM support_tickets WHERE id = $1',
      [id]
    );
    
    if (ticketResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'Ticket not found' });
    }
    
    const ticket = ticketResult.rows[0];
    
    // Check permissions
    if (ticket.user_id !== req.user.id && req.user.role !== 'admin' && req.user.role !== 'superadmin') {
      return res.status(403).json({ success: false, error: 'FORBIDDEN', message: 'Not authorized to reply to this ticket' });
    }
    
    const isAdmin = req.user.role === 'admin' || req.user.role === 'superadmin';
    
    await pool.query(
      `INSERT INTO ticket_replies (id, ticket_id, user_id, message, is_admin)
       VALUES ($1, $2, $3, $4, $5)`,
      [generateId(), id, req.user.id, message, isAdmin]
    );
    
    // Update ticket status
    if (isAdmin) {
      await pool.query(
        'UPDATE support_tickets SET status = $1, updated_at = NOW() WHERE id = $2',
        ['in_progress', id]
      );
      
      await sendNotification(
        ticket.user_id,
        'ticket_reply',
        '📩 Support Ticket Reply',
        `Admin replied to your ticket: "${message.substring(0, 100)}..."`,
        { ticket_id: id }
      );
    } else {
      await pool.query(
        'UPDATE support_tickets SET status = $1, updated_at = NOW() WHERE id = $2',
        ['open', id]
      );
      
      // Notify admins
      const adminResult = await pool.query('SELECT id FROM users WHERE role IN ($1, $2)', ['admin', 'superadmin']);
      for (const admin of adminResult.rows) {
        await sendNotification(
          admin.id,
          'ticket_reply',
          '📩 New Ticket Reply',
          `${req.user.username} replied to ticket: "${message.substring(0, 100)}..."`,
          { ticket_id: id, user: req.user.username }
        );
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
    let paramIndex = 2;
    
    if (before) {
      query += ` AND cm.created_at < $${paramIndex}`;
      params.push(before);
      paramIndex++;
    }
    
    query += ` ORDER BY cm.created_at DESC LIMIT $${paramIndex}`;
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
    
    await pool.query(
      `INSERT INTO chat_messages (id, user_id, username, message, room)
       VALUES ($1, $2, $3, $4, $5)`,
      [messageId, req.user.id, req.user.username, message, room]
    );
    
    // Emit to all users in the room
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
    let query = `
      SELECT * FROM notifications 
      WHERE user_id = $1
    `;
    const params = [req.user.id];
    let paramIndex = 2;
    
    if (unread_only === 'true') {
      query += ` AND is_read = false`;
    }
    
    query += ` ORDER BY created_at DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
    params.push(limit, (page - 1) * limit);
    
    const result = await pool.query(query, params);
    
    const countQuery = `
      SELECT COUNT(*) FROM notifications WHERE user_id = $1
      ${unread_only === 'true' ? 'AND is_read = false' : ''}
    `;
    const countResult = await pool.query(countQuery, [req.user.id]);
    
    res.json({
      success: true,
      data: {
        notifications: result.rows,
        unread_count: unread_only === 'true' ? result.rows.length : await getUnreadCount(req.user.id),
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: parseInt(countResult.rows[0].count),
          pages: Math.ceil(countResult.rows[0].count / limit)
        }
      }
    });
  } catch (err) {
    logger.error('Get notifications error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to get notifications' });
  }
});

async function getUnreadCount(userId) {
  const result = await pool.query(
    'SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = false',
    [userId]
  );
  return parseInt(result.rows[0].count);
}

app.put('/api/notifications/:id/read', authenticate, async (req, res) => {
  const { id } = req.params;
  
  try {
    await pool.query(
      'UPDATE notifications SET is_read = true WHERE id = $1 AND user_id = $2',
      [id, req.user.id]
    );
    res.json({ success: true, data: { message: 'Notification marked as read' } });
  } catch (err) {
    logger.error('Mark notification read error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to mark notification as read' });
  }
});

app.put('/api/notifications/read-all', authenticate, async (req, res) => {
  try {
    await pool.query(
      'UPDATE notifications SET is_read = true WHERE user_id = $1 AND is_read = false',
      [req.user.id]
    );
    res.json({ success: true, data: { message: 'All notifications marked as read' } });
  } catch (err) {
    logger.error('Mark all notifications read error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to mark notifications as read' });
  }
});

// ============== API KEY MANAGEMENT ==============

app.post('/api/api-keys', authenticate, async (req, res) => {
  try {
    const newApiKey = generateApiKey();
    
    await pool.query(
      'UPDATE users SET api_key = $1 WHERE id = $2',
      [newApiKey, req.user.id]
    );
    
    await logAudit(req.user.id, 'regenerate_api_key', 'user', req.user.id, {}, req.ip, req.headers['user-agent']);
    
    res.json({ success: true, data: { api_key: newApiKey } });
  } catch (err) {
    logger.error('Regenerate API key error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to regenerate API key' });
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
       FROM users 
       WHERE id = $1`,
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
  
  try {
    const updates = [];
    const values = [];
    let paramIndex = 1;
    
    if (username) {
      updates.push(`username = $${paramIndex}`);
      values.push(username);
      paramIndex++;
    }
    if (email !== undefined) {
      updates.push(`email = $${paramIndex}`);
      values.push(email);
      paramIndex++;
    }
    if (bio !== undefined) {
      updates.push(`bio = $${paramIndex}`);
      values.push(bio);
      paramIndex++;
    }
    if (avatar !== undefined) {
      updates.push(`avatar = $${paramIndex}`);
      values.push(avatar);
      paramIndex++;
    }
    
    updates.push(`updated_at = NOW()`);
    values.push(req.user.id);
    
    await pool.query(`UPDATE users SET ${updates.join(', ')} WHERE id = $${paramIndex}`, values);
    
    res.json({ success: true, data: { message: 'Profile updated successfully' } });
  } catch (err) {
    logger.error('Update profile error:', err);
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: 'Failed to update profile' });
  }
});

// ============== SYSTEM HEALTH & STATS ==============

app.get('/api/system/health', async (req, res) => {
  try {
    // Check database
    await pool.query('SELECT 1');
    
    // Get stats
    const userCount = await pool.query('SELECT COUNT(*) FROM users');
    const projectCount = await pool.query('SELECT COUNT(*) FROM projects');
    const runningProjectCount = await pool.query("SELECT COUNT(*) FROM projects WHERE status = 'running'");
    
    res.json({
      success: true,
      data: {
        status: 'healthy',
        timestamp: new Date(),
        uptime: process.uptime(),
        database: 'connected',
        stats: {
          users: parseInt(userCount.rows[0].count),
          projects: parseInt(projectCount.rows[0].count),
          running_projects: parseInt(runningProjectCount.rows[0].count)
        }
      }
    });
  } catch (err) {
    logger.error('Health check error:', err);
    res.status(500).json({ success: false, error: 'UNHEALTHY', message: 'System is unhealthy' });
  }
});

// ============== CRON JOBS ==============

// Reset monthly subscription coins
cron.schedule('0 0 1 * *', async () => {
  logger.info('Running monthly subscription coin reset');
  
  try {
    const usersResult = await pool.query(`
      SELECT id, subscription_tier FROM users 
      WHERE subscription_tier != 'free' 
      AND (subscription_expiry IS NULL OR subscription_expiry > NOW())
    `);
    
    const tierCoins = {
      starter: 100,
      pro: 500,
      enterprise: 2000,
      ultimate: 10000
    };
    
    for (const user of usersResult.rows) {
      const coins = tierCoins[user.subscription_tier];
      if (coins) {
        await updateUserCoins(user.id, coins, 'subscription_renewal', `Monthly ${user.subscription_tier.toUpperCase()} subscription bonus`, null);
        await sendNotification(user.id, 'subscription_renewal', '🔄 Subscription Renewed', `Your monthly ${user.subscription_tier.toUpperCase()} subscription has renewed! You received ${coins} coins.`, { tier: user.subscription_tier, coins });
      }
    }
    
    logger.info(`Monthly subscription coins awarded to ${usersResult.rows.length} users`);
  } catch (err) {
    logger.error('Monthly subscription cron error:', err);
  }
});

// Process scheduled bulk messages
cron.schedule('* * * * *', async () => {
  try {
    const messagesResult = await pool.query(`
      SELECT id FROM messages 
      WHERE status = 'scheduled' 
      AND scheduled_for <= NOW()
    `);
    
    for (const message of messagesResult.rows) {
      await processBulkMessage(message.id);
    }
  } catch (err) {
    logger.error('Process scheduled messages error:', err);
  }
});

// Clean up old logs
cron.schedule('0 0 * * 0', async () => {
  logger.info('Running log cleanup');
  
  try {
    // Delete notifications older than 90 days
    await pool.query(`
      DELETE FROM notifications 
      WHERE created_at < NOW() - INTERVAL '90 days'
      AND is_read = true
    `);
    
    // Delete audit logs older than 180 days
    await pool.query(`
      DELETE FROM audit_logs 
      WHERE created_at < NOW() - INTERVAL '180 days'
    `);
    
    logger.info('Log cleanup completed');
  } catch (err) {
    logger.error('Log cleanup error:', err);
  }
});

// ============== START SERVER ==============

const PORT = process.env.PORT || 3000;

async function startServer() {
  await initDatabase();
  
  // Set default admin if none exists
  const adminCheck = await pool.query("SELECT id FROM users WHERE role = 'superadmin'");
  if (adminCheck.rows.length === 0 && process.env.ADMIN_USERNAME && process.env.ADMIN_PASSWORD) {
    const hashedPassword = await bcrypt.hash(process.env.ADMIN_PASSWORD, 10);
    const adminId = generateId();
    const adminApiKey = generateApiKey();
    
    await pool.query(
      `INSERT INTO users (id, username, email, password_hash, role, admin_level, referral_code, api_key, coins, max_projects, is_verified)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [adminId, process.env.ADMIN_USERNAME, process.env.ADMIN_EMAIL || 'admin@cloudhost.app', hashedPassword, 'superadmin', 10, generateReferralCode(), adminApiKey, 10000, 999999, true]
    );
    
    logger.info('Default admin user created');
  }
  
  server.listen(PORT, () => {
    logger.info(`🚀 CloudHost Server running on port ${PORT}`);
    logger.info(`📍 Environment: ${process.env.NODE_ENV || 'development'}`);
    logger.info(`👑 Created by Bruce Bera - The Greatest Hosting Platform in Human History`);
  });
}

startServer().catch(err => {
  logger.error('Failed to start server:', err);
  process.exit(1);
});

module.exports = { app, server, io };
