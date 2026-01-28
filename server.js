// server.js - Main backend server
require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const WebSocket = require('ws');
const cron = require('node-cron');

// Database connection
const connectDB = require('./config/database');
connectDB();

// Services
const broadcastService = require('./services/broadcastService');

// Models
const Contact = require('./models/Contact');
const Conversation = require('./models/Conversation');
const Message = require('./models/Message');
const whatsappService = require('./services/whatsappService');
const whatsappConfig = require('./config/whatsapp');

// Routes
const bulkRoutes = require('./routes/bulk');
const templateRoutes = require('./routes/templates');
const broadcastRoutes = require('./routes/broadcasts');
const conversationRoutes = require('./routes/conversations');
const messageRoutes = require('./routes/messages');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Middleware
const allowedOrigins = [
  process.env.FRONTEND_URL,
  "https://technovo-automation-afplwwbfj-technovas-projects-37226de2.vercel.app",
  "https://technovo-automation-m9n8fz6sl-technovas-projects-37226de2.vercel.app",
  "https://technovo-automation.vercel.app",
  "http://localhost:5173",
  "http://127.0.0.1:5173",
  "http://127.0.0.1:53918",
  "http://localhost:53918",
  "http://localhost:60932",
  "http://localhost:5174",
  "http://localhost:5175",
  "http://127.0.0.1:5174",
  "http://127.0.0.1:5175",
].filter(Boolean);

app.use(cors({
  origin: (origin, callback) => {
    // Postman / server-to-server requests
    if (!origin) return callback(null, true);

    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    
    // Allow any vercel.app subdomain for development
    if (origin.includes('.vercel.app')) {
      console.log(`CORS allowed for Vercel deployment: ${origin}`);
      return callback(null, true);
    }
    
    // Log blocked origins for debugging
    console.log(`CORS blocked: ${origin}`);
    return callback(new Error(`CORS blocked: ${origin}`));
  },
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  preflightContinue: false,
  optionsSuccessStatus: 204
}));

app.options("*", cors());

app.use(express.json());
// API Routes - Moved up before WebSocket and other route handlers
app.use('/api/bulk', bulkRoutes);
app.use('/api/templates', templateRoutes);
app.use('/api/broadcasts', broadcastRoutes);
app.use('/api/conversations', conversationRoutes);
app.use('/api/messages', messageRoutes);

// ============ WEBSOCKET MANAGEMENT ============

const clients = new Map(); // Store connected clients

wss.on('connection', (ws, req) => {
  const userId = req.headers['user-id'] || 'anonymous';
  clients.set(userId, ws);
  
  console.log(`Client connected: ${userId}`);
  
  // Broadcast user list update
  broadcastUserList();

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      handleWebSocketMessage(data, userId, ws);
    } catch (error) {
      console.error('WebSocket message parse error:', error);
    }
  });

  ws.on('close', () => {
    clients.delete(userId);
    console.log(`Client disconnected: ${userId}`);
    broadcastUserList();
  });
});

function handleWebSocketMessage(data, userId, ws) {
  switch (data.type) {
    case 'identify':
      if (data.userId) {
        clients.delete(userId);
        clients.set(data.userId, ws);
        console.log(`Client identified: ${data.userId}`);
        broadcastUserList();
      }
      break;
    default:
      console.log(`Unknown message type from ${userId}:`, data.type);
  }
}

function broadcastUserList() {
  const userList = Array.from(clients.keys());
  broadcast({ type: 'user_list', users: userList });
}

function broadcast(data) {
  const message = JSON.stringify(data);
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Make broadcaster available to routes/controllers
app.locals.broadcast = broadcast;
app.locals.sendToUser = sendToUser;

function sendToUser(userId, data) {
  const client = clients.get(userId);
  if (client && client.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(data));
  }
}

// ============ WHATSAPP WEBHOOK ============

app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  console.log('📞 Webhook verification request:', { mode, token: token?.substring(0, 10) + '...' });

  if (mode === 'subscribe' && token === whatsappConfig.WEBHOOK_VERIFY_TOKEN) {
    console.log('✅ Webhook verified successfully');
    res.status(200).send(challenge);
  } else {
    console.log('❌ Webhook verification failed');
    res.sendStatus(403);
  }
});

app.post('/webhook', async (req, res) => {
  try {
    const data = req.body;
    
    console.log('📨 Webhook received:', JSON.stringify(data, null, 2));

    if (data.object === 'whatsapp_business_account') {
      for (const entry of data.entry) {
        for (const change of entry.changes) {
          if (change.field === 'messages') {
            const messageData = change.value.messages?.[0];
            if (messageData) {
              console.log('💬 New message from:', messageData.from);
              await handleIncomingMessage(messageData, change.value);
            }

            // Handle status updates
            const statusData = change.value.statuses?.[0];
            if (statusData) {
              console.log('📊 Status update:', statusData.status);
              await handleMessageStatus(statusData);
            }
          }
        }
      }
    }

    res.sendStatus(200);
  } catch (error) {
    console.error('❌ Webhook error:', error);
    res.sendStatus(500);
  }
});

async function handleIncomingMessage(messageData, value) {
  try {
    console.log('📥 Processing incoming message...');
    
    const from = messageData.from;
    const text = messageData.text?.body || '';
    const messageId = messageData.id;

    // Find or create contact
    let contact = await Contact.findOne({ phone: from });
    if (!contact) {
      contact = await Contact.create({
        phone: from,
        name: value.contacts?.[0]?.profile?.name || from,
        lastContact: new Date()
      });
    } else {
      contact.lastContact = new Date();
      await contact.save();
    }

    // Find or create conversation
    let conversation = await Conversation.findOne({ contactPhone: from, status: { $in: ['active', 'pending'] } });
    if (!conversation) {
      conversation = await Conversation.create({
        contactId: contact._id,
        contactPhone: from,
        contactName: contact.name,
        lastMessageTime: new Date(),
        lastMessage: text,
        lastMessageFrom: 'contact',
        unreadCount: 1
      });
    } else {
      conversation.lastMessageTime = new Date();
      conversation.lastMessage = text;
      conversation.lastMessageFrom = 'contact';
      conversation.unreadCount += 1;
      await conversation.save();
    }

    // Save message
    const message = await Message.create({
      conversationId: conversation._id,
      sender: 'contact',
      senderName: contact.name,
      text: text,
      whatsappMessageId: messageId,
      status: 'received',
      whatsappTimestamp: new Date(messageData.timestamp * 1000),
      timestamp: new Date() // Explicitly set timestamp
    });

    // Broadcast to all connected clients
    broadcast({
      type: 'new_message',
      conversation: conversation.toObject(),
      message: message.toObject()
    });

    console.log('✅ Message processing complete!\n');
  } catch (error) {
    console.error('❌ Error in handleIncomingMessage:', error);
    throw error;
  }
}

async function handleMessageStatus(statusData) {
  try {
    const messageId = statusData.id;
    const status = statusData.status; // sent, delivered, read, failed

    const message = await Message.findOneAndUpdate(
      { whatsappMessageId: messageId },
      { status: status },
      { new: true }
    );

    if (message) {
      broadcast({
        type: 'message_status',
        messageId: messageId,
        status: status,
        conversationId: message.conversationId
      });
    }
  } catch (error) {
    console.error('Error handling message status:', error);
  }
}

// ============ API ROUTES ============

// Send message
app.post('/api/messages/send', async (req, res) => {
  try {
    const { to, text, conversationId, mediaUrl, mediaType } = req.body;
    
    console.log('🔍 DEBUG /api/messages/send called with:');
    console.log(`   to: ${to}`);
    console.log(`   text: "${text}"`);
    console.log(`   conversationId: ${conversationId}`);
    console.log(`   mediaUrl: ${mediaUrl}`);
    console.log(`   mediaType: ${mediaType}`);

    let result;
    if (mediaUrl && mediaType) {
      result = await whatsappService.sendMediaMessage(to, mediaType, mediaUrl, text);
    } else {
      result = await whatsappService.sendTextMessage(to, text);
    }

    if (!result.success) {
      return res.status(400).json({ success: false, error: result.error });
    }

    const whatsappMessageId = result.data.messages[0].id;

    // Save to database
    const message = await Message.create({
      conversationId: conversationId,
      sender: 'agent',
      text: text,
      mediaUrl: mediaUrl,
      mediaType: mediaType,
      whatsappMessageId: whatsappMessageId,
      status: 'sent',
      timestamp: new Date() // Explicitly set timestamp
    });

    // Update conversation
    await Conversation.findByIdAndUpdate(conversationId, {
      lastMessageTime: new Date(),
      lastMessage: text,
      lastMessageFrom: 'agent'
    });

    // Broadcast to clients
    broadcast({
      type: 'message_sent',
      message: message.toObject()
    });

    res.json({ success: true, message: message });
  } catch (error) {
    console.error('Send message error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get conversations
app.get('/api/conversations', async (req, res) => {
  try {
    const { status, assignedTo, search } = req.query;
    const filters = {};
    
    if (status) filters.status = status;
    if (assignedTo) filters.assignedTo = assignedTo;
    if (search) {
      filters.$or = [
        { contactName: { $regex: search, $options: 'i' } },
        { contactPhone: { $regex: search, $options: 'i' } }
      ];
    }

    const conversations = await Conversation.find(filters)
      .populate('contactId', 'name phone email tags')
      .sort({ lastMessageTime: -1 })
      .limit(100);
    res.json(conversations);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Create conversation
app.post('/api/conversations', async (req, res) => {
  try {
    const { contactId, contactPhone, contactName, status } = req.body;
    
    // Check if conversation already exists
    const existingConversation = await Conversation.findOne({ 
      contactPhone: contactPhone,
      status: { $in: ['active', 'pending'] }
    });
    
    if (existingConversation) {
      return res.status(400).json({ error: 'Conversation already exists for this contact' });
    }
    
    // Create new conversation
    const conversation = await Conversation.create({
      contactId: contactId,
      contactPhone: contactPhone,
      contactName: contactName,
      status: status || 'active',
      lastMessageTime: new Date(),
      unreadCount: 0
    });
    
    res.status(201).json(conversation);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get single conversation
app.get('/api/conversations/:id', async (req, res) => {
  try {
    const conversation = await Conversation.findById(req.params.id)
      .populate('contactId', 'name phone email tags');
    if (!conversation) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    res.json(conversation);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update conversation
app.put('/api/conversations/:id', async (req, res) => {
  try {
    const conversation = await Conversation.findByIdAndUpdate(
      req.params.id,
      req.body,
      { new: true }
    );
    if (!conversation) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    res.json(conversation);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Mark conversation as read
app.put('/api/conversations/:id/read', async (req, res) => {
  try {
    const conversation = await Conversation.findByIdAndUpdate(
      req.params.id,
      { unreadCount: 0 },
      { new: true }
    );
    if (!conversation) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    
    // Broadcast the read status to all clients
    broadcast({
      type: 'conversation_read',
      conversationId: req.params.id,
      unreadCount: 0
    });
    
    res.json(conversation);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get messages for conversation
app.get('/api/conversations/:id/messages', async (req, res) => {
  try {
    const messages = await Message.find({ conversationId: req.params.id })
      .sort({ timestamp: 1 });
    res.json(messages);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get contacts
app.get('/api/contacts', async (req, res) => {
  try {
    const { search, tags } = req.query;
    const filters = {};
    
    if (search) {
      filters.$or = [
        { name: { $regex: search, $options: 'i' } },
        { phone: { $regex: search, $options: 'i' } },
        { email: { $regex: search, $options: 'i' } }
      ];
    }
    if (tags) {
      filters.tags = { $in: tags.split(',') };
    }

    const contacts = await Contact.find(filters).sort({ lastContact: -1 });
    res.json(contacts);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Create contact
app.post('/api/contacts', async (req, res) => {
  try {
    const contact = await Contact.create(req.body);
    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update contact
app.put('/api/contacts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, phone, email, tags, notes, isBlocked } = req.body;
    
    // Find current contact
    const currentContact = await Contact.findById(id);
    if (!currentContact) {
      return res.status(404).json({ error: 'Contact not found' });
    }
    
    // Check if phone number is being changed and if it conflicts with existing contact
    if (phone && phone !== currentContact.phone) {
      const existingContact = await Contact.findOne({ phone, _id: { $ne: id } });
      if (existingContact) {
        return res.status(400).json({ error: 'Another contact with this phone number already exists' });
      }
    }
    
    // Update contact fields
    const updateData = {};
    if (name !== undefined) updateData.name = name;
    if (phone !== undefined) updateData.phone = phone;
    if (email !== undefined) updateData.email = email;
    if (tags !== undefined) updateData.tags = tags;
    if (notes !== undefined) updateData.notes = notes;
    if (isBlocked !== undefined) updateData.isBlocked = isBlocked;
    
    const contact = await Contact.findByIdAndUpdate(
      id,
      updateData,
      { new: true, runValidators: true }
    );
    
    // If name was updated, also update all conversations for this contact
    if (name !== undefined && name !== currentContact.name) {
      await Conversation.updateMany(
        { contactId: id },
        { contactName: name }
      );
    }
    
    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get analytics
app.get('/api/analytics', async (req, res) => {
  try {
    const totalConversations = await Conversation.countDocuments();
    const activeConversations = await Conversation.countDocuments({ status: 'active' });
    
    const last7Days = new Date();
    last7Days.setDate(last7Days.getDate() - 7);
    
    // Message metrics
    const messagesSent = await Message.countDocuments({
      sender: 'agent',
      timestamp: { $gte: last7Days }
    });
    
    const messagesDelivered = await Message.countDocuments({
      sender: 'agent',
      status: 'delivered',
      timestamp: { $gte: last7Days }
    });
    
    const messagesRead = await Message.countDocuments({
      sender: 'agent',
      status: 'read',
      timestamp: { $gte: last7Days }
    });
    
    const messagesFailed = await Message.countDocuments({
      sender: 'agent',
      status: 'failed',
      timestamp: { $gte: last7Days }
    });
    
    const messagesReceived = await Message.countDocuments({
      sender: 'contact',
      timestamp: { $gte: last7Days }
    });

    const analytics = {
      totalConversations,
      activeConversations,
      messagesSent,
      messagesDelivered,
      messagesRead,
      messagesFailed,
      messagesReceived,
      avgResponseTime: '2m 34s',
      responseRate: 94.5,
      customerSatisfaction: 4.7,
      // Growth percentages (mock data for now)
      sentGrowth: '12',
      deliveredGrowth: '8',
      readRateGrowth: '5',
      failedGrowth: '-2'
    };

    res.json(analytics);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📡 WebSocket server ready`);
  console.log(`🌐 Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:5173'}`);
  
  // Start the scheduler for checking scheduled broadcasts
  startScheduler();
});

// Scheduler to check for scheduled broadcasts every minute
function startScheduler() {
  console.log('⏰ Starting broadcast scheduler...');
  
  // Run every minute
  cron.schedule('* * * * *', async () => {
    try {
      console.log('🔍 Checking for scheduled broadcasts...');
      await broadcastService.checkScheduledBroadcasts(broadcast);
    } catch (error) {
      console.error('❌ Error in scheduler:', error);
    }
  });
  
  console.log('✅ Broadcast scheduler started - checking every minute');
}

