// server.js - Main backend server
require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const WebSocket = require('ws');
const cron = require('node-cron');

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled promise rejection:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
});

// Database connection
const connectDB = require('./config/database');
const mongoose = require('mongoose');
connectDB();

// Services
const broadcastService = require('./services/broadcastService');
const missedCallAutomationService = require('./services/missedCallAutomationService');
const templateController = require('./controllers/templateController');

// Models
const Contact = require('./models/Contact');
const Template = require('./models/Template');
const Conversation = require('./models/Conversation');
const Message = require('./models/Message');
const Broadcast = require('./models/Broadcast');
const MetaAdsTransaction = require('./models/MetaAdsTransaction');
const MetaAdsWallet = require('./models/MetaAdsWallet');
const whatsappService = require('./services/whatsappService');
const whatsappConfig = require('./config/whatsapp');
const auth = require('./middleware/auth');
const metaAdsService = require('./services/metaAdsService');
const requireWhatsAppCredentials = require('./middleware/requireWhatsAppCredentials');
const {
  resolveUserIdByPhoneNumberId,
  getWhatsAppCredentialsForUser
} = require('./services/userWhatsAppCredentialsService');

// Routes
const bulkRoutes = require('./routes/bulk');
const templateRoutes = require('./routes/templates');
const broadcastRoutes = require('./routes/broadcasts');
const conversationRoutes = require('./routes/conversations');
const messageRoutes = require('./routes/messages');
const contactRoutes = require('./routes/contacts');
const missedCallRoutes = require('./routes/missedCalls');
const metaAdsRoutes = require('./routes/metaAds');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Middleware
const allowedOrigins = [
  process.env.FRONTEND_URL,
  "https://www.technovahub.in",
  "https://technovahub.in",
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
app.use(express.urlencoded({ extended: true }));
// API Routes - Moved up before WebSocket and other route handlers
app.use('/api/bulk', bulkRoutes);
app.use('/api/templates', templateRoutes);
app.use('/api/broadcasts', broadcastRoutes);
app.use('/api/conversations', conversationRoutes);
app.use('/api/messages', messageRoutes);
app.use('/api/contacts', contactRoutes);
app.use('/api/missedcalls', missedCallRoutes);
app.use('/api/meta-ads', metaAdsRoutes);

// ============ WEBSOCKET MANAGEMENT ============

const clients = new Map(); // Store connected clients

wss.on('connection', (ws, req) => {
  console.log('🔌 New WebSocket connection established');
  const userId = req.headers['user-id'] || 'anonymous';
  console.log('👤 User ID:', userId);
  clients.set(userId, ws);
  
  console.log('📊 Total connected clients:', clients.size);
  
  // Send current user list to all clients
  const userList = Array.from(clients.keys());
  broadcast({ type: 'user_list', users: userList });
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      console.log('📨 WebSocket message received:', data);
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

function emitRealtimeEvent(userId, data) {
  if (userId) {
    sendToUser(String(userId), data);
    return;
  }
  broadcast(data);
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
    const phoneNumberId = value?.metadata?.phone_number_id;
    const userId = await resolveUserIdByPhoneNumberId(phoneNumberId);

    if (!userId) {
      console.warn(`Skipping incoming message: no user mapping found for phone_number_id=${phoneNumberId || 'unknown'}`);
      return;
    }

    // Find or create contact
    let contact = await Contact.findOne({ userId, phone: from });
    if (!contact) {
      contact = await Contact.create({
        userId,
        phone: from,
        name: value.contacts?.[0]?.profile?.name || from,
        sourceType: 'incoming_message',
        lastContact: new Date()
      });
    } else {
      contact.lastContact = new Date();
      await contact.save();
    }

    // Find or create conversation
    let conversation = await Conversation.findOne({ userId, contactPhone: from, status: { $in: ['active', 'pending'] } });
    if (!conversation) {
      conversation = await Conversation.create({
        userId,
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
      const currentUnread = Number(conversation.unreadCount);
      conversation.unreadCount = Number.isFinite(currentUnread)
        ? Math.max(0, currentUnread) + 1
        : 1;
      await conversation.save();
    }

    // Save message
    const message = await Message.create({
      userId,
      conversationId: conversation._id,
      sender: 'contact',
      senderName: contact.name,
      text: text,
      whatsappMessageId: messageId,
      status: 'received',
      whatsappTimestamp: new Date(messageData.timestamp * 1000),
      timestamp: new Date() // Explicitly set timestamp
    });

    // Check if this is a reply to a broadcast message
    const broadcast = await Broadcast.findOne({
      createdById: userId,
      'recipients.phone': from,
      status: { $in: ['sending', 'completed'] }, // Only check active/completed broadcasts
      startedAt: { $exists: true }
    }).sort({ startedAt: -1 }); // Get the most recent broadcast

    if (broadcast) {
      // Check if this is the first reply from this contact in this broadcast
      const previousReplies = await Message.countDocuments({
        conversationId: conversation._id,
        sender: 'contact',
        timestamp: { 
          $gte: broadcast.startedAt,
          $lte: new Date()
        }
      });

      if (previousReplies === 1) { // This is the first reply
        // Increment the replied count
        await Broadcast.updateOne(
          { _id: broadcast._id },
          { $inc: { 'stats.replied': 1 } }
        );

        // Get updated broadcast to emit
        const updatedBroadcast = await Broadcast.findById(broadcast._id);
        
        // Emit update to connected clients with calculated percentage
        emitRealtimeEvent(userId, {
          type: 'broadcast_stats_updated',
          broadcastId: broadcast._id.toString(),
          stats: {
            ...updatedBroadcast.stats,
            repliedPercentage: updatedBroadcast.repliedPercentage,
            repliedPercentageOfTotal: updatedBroadcast.repliedPercentageOfTotal
          }
        });
        
        console.log(`📊 Updated replied count for broadcast "${broadcast.name}": ${updatedBroadcast.stats.replied} (${updatedBroadcast.repliedPercentage}% of sent)`);
      }
    }

    // Broadcast to all connected clients
    emitRealtimeEvent(userId, {
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
    const recipient = statusData.recipient_id;

    console.log('DEBUG: Received status update:', {
      messageId,
      status,
      recipient,
      timestamp: statusData.timestamp,
      conversationStatus: statusData.conversation?.id
    });

    // Find message by WhatsApp message id
    const message = await Message.findOne({ whatsappMessageId: messageId });
    if (!message) {
      console.log('DEBUG: No message found for whatsappMessageId:', messageId);
      return;
    }

    const oldStatus = message.status;
    const updatedMessage = await Message.findOneAndUpdate(
      { _id: message._id },
      { status: status, updatedAt: new Date() },
      { new: true }
    );

    if (!updatedMessage) {
      return;
    }

    // Realtime update for Team Inbox message status
    emitRealtimeEvent(message.userId, {
      type: 'message_status',
      messageId: messageId,
      status: status,
      conversationId: message.conversationId,
      broadcastId: updatedMessage.broadcastId ? String(updatedMessage.broadcastId) : null,
      previousStatus: oldStatus
    });

    // Broadcast stats update must apply ONLY to the exact originating broadcast
    if (updatedMessage.broadcastId && oldStatus !== updatedMessage.status) {
      try {
        const broadcast = await Broadcast.findOne({
          _id: updatedMessage.broadcastId,
          createdById: message.userId,
          status: { $in: ['sending', 'completed'] }
        });

        if (!broadcast) {
          return;
        }

        const newStatus = updatedMessage.status;
        let update = {};

        if (newStatus === 'delivered' && oldStatus === 'sent') {
          update['stats.delivered'] = 1;
        } else if (newStatus === 'read' && oldStatus !== 'read') {
          update['stats.read'] = 1;
          if (oldStatus !== 'delivered') {
            update['stats.delivered'] = 1;
          }
        } else if (newStatus === 'failed' && oldStatus !== 'failed') {
          update['stats.failed'] = 1;
          if (oldStatus === 'sent') {
            update['stats.sent'] = -1;
          }
        }

        if (Object.keys(update).length > 0) {
          await Broadcast.updateOne({ _id: broadcast._id }, { $inc: update });
          const updatedBroadcast = await Broadcast.findById(broadcast._id);

          if (updatedBroadcast) {
            // Safety clamp: never allow negative counters
            let clamped = false;
            ['sent', 'delivered', 'read', 'failed', 'replied'].forEach((k) => {
              if ((updatedBroadcast.stats?.[k] || 0) < 0) {
                updatedBroadcast.stats[k] = 0;
                clamped = true;
              }
            });
            if (clamped) {
              await updatedBroadcast.save();
            }

            emitRealtimeEvent(message.userId, {
              type: 'broadcast_stats_updated',
              broadcastId: broadcast._id.toString(),
              stats: updatedBroadcast.stats,
              statusChange: `${oldStatus} -> ${newStatus}`
            });
          }
        }
      } catch (broadcastError) {
        console.error('Error updating broadcast stats:', broadcastError);
      }
    }
  } catch (error) {
    console.error('Error handling message status:', error);
  }
}
// ============ API ROUTES ============
app.post('/api/messages/send', auth, requireWhatsAppCredentials, async (req, res) => {
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
      result = await whatsappService.sendMediaMessage(to, mediaType, mediaUrl, text, req.whatsappCredentials);
    } else {
      result = await whatsappService.sendTextMessage(to, text, req.whatsappCredentials);
    }

    if (!result.success) {
      return res.status(400).json({ success: false, error: result.error });
    }

    const whatsappMessageId = result.data.messages[0].id;

    // Save to database
    const conversation = await Conversation.findOne({ _id: conversationId, userId: req.user.id });
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const message = await Message.create({
      userId: req.user.id,
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
    await Conversation.findOneAndUpdate({ _id: conversationId, userId: req.user.id }, {
      lastMessageTime: new Date(),
      lastMessage: text,
      lastMessageFrom: 'agent'
    });

    // Broadcast to clients
    emitRealtimeEvent(req.user.id, {
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
app.get('/api/conversations', auth, async (req, res) => {
  try {
    const { status, assignedTo, search } = req.query;
    const filters = { userId: req.user.id };
    
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
app.post('/api/conversations', auth, async (req, res) => {
  try {
    const { contactId, contactPhone, contactName, status } = req.body;
    
    // Check if conversation already exists
    const existingConversation = await Conversation.findOne({ 
      userId: req.user.id,
      contactPhone: contactPhone,
      status: { $in: ['active', 'pending'] }
    });
    
    if (existingConversation) {
      return res.status(400).json({ error: 'Conversation already exists for this contact' });
    }
    
    // Create new conversation
    const conversation = await Conversation.create({
      userId: req.user.id,
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
app.get('/api/conversations/:id', auth, async (req, res) => {
  try {
    const conversation = await Conversation.findOne({ _id: req.params.id, userId: req.user.id })
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
app.put('/api/conversations/:id', auth, async (req, res) => {
  try {
    const conversation = await Conversation.findOneAndUpdate(
      { _id: req.params.id, userId: req.user.id },
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
app.put('/api/conversations/:id/read', auth, async (req, res) => {
  try {
    await Message.updateMany(
      {
        conversationId: req.params.id,
        userId: req.user.id,
        sender: 'contact',
        status: 'received'
      },
      { status: 'read' }
    );

    const conversation = await Conversation.findOneAndUpdate(
      { _id: req.params.id, userId: req.user.id },
      { unreadCount: 0 },
      { new: true }
    );
    if (!conversation) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    
    // Broadcast the read status to all clients
    emitRealtimeEvent(req.user.id, {
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
app.get('/api/conversations/:id/messages', auth, async (req, res) => {
  try {
    const messages = await Message.find({ conversationId: req.params.id, userId: req.user.id })
      .sort({ timestamp: 1 });
    res.json(messages);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get contacts
app.get('/api/contacts', auth, async (req, res) => {
  try {
    const { search, tags } = req.query;
    const filters = { userId: req.user.id };
    
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
app.post('/api/contacts', auth, async (req, res) => {
  try {
    const contact = await Contact.create({ ...req.body, userId: req.user.id, sourceType: 'manual' });
    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update contact
app.put('/api/contacts/:id', auth, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, phone, email, tags, notes, isBlocked } = req.body;
    
    // Find current contact
    const currentContact = await Contact.findOne({ _id: id, userId: req.user.id });
    if (!currentContact) {
      return res.status(404).json({ error: 'Contact not found' });
    }
    
    // Check if phone number is being changed and if it conflicts with existing contact
    if (phone && phone !== currentContact.phone) {
      const existingContact = await Contact.findOne({ phone, _id: { $ne: id }, userId: req.user.id });
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
    
    const contact = await Contact.findOneAndUpdate(
      { _id: id, userId: req.user.id },
      updateData,
      { new: true, runValidators: true }
    );
    
    // If name was updated, also update all conversations for this contact
    if (name !== undefined && name !== currentContact.name) {
      await Conversation.updateMany(
        { contactId: id, userId: req.user.id },
        { contactName: name }
      );
    }
    
    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to investigate broadcast data
app.get('/api/debug-broadcasts', auth, async (req, res) => {
  try {
    const debugInServer = require('./debugInServer');
    await debugInServer();
    res.json({ success: true, message: 'Debug completed - check server logs' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get analytics
app.get('/api/analytics', auth, async (req, res) => {
  try {
    const totalConversations = await Conversation.countDocuments({ userId: req.user.id });
    const activeConversations = await Conversation.countDocuments({ userId: req.user.id, status: 'active' });
    
    const last7Days = new Date();
    last7Days.setDate(last7Days.getDate() - 7);
    
    // Message metrics - count all agent messages first, then break down by status
    const allAgentMessages = await Message.find({
      userId: req.user.id,
      sender: 'agent',
      timestamp: { $gte: last7Days }
    });
    
    const userBroadcasts = await Broadcast.find({
      $or: [
        { createdById: req.user.id },
        { createdBy: req.user.username || req.user.email || req.user.id }
      ]
    }).select('stats').lean();

    const campaignTotals = userBroadcasts.reduce((acc, b) => {
      const sent = Number(b?.stats?.sent || 0);
      const delivered = Number(b?.stats?.delivered || 0);
      const read = Number(b?.stats?.read || 0);
      const failed = Number(b?.stats?.failed || 0);
      return {
        sent: acc.sent + sent,
        delivered: acc.delivered + Math.max(delivered, read),
        read: acc.read + read,
        failed: acc.failed + failed
      };
    }, { sent: 0, delivered: 0, read: 0, failed: 0 });

    const messageTotals = await Message.aggregate([
      {
        $match: {
          userId: req.user.id,
          sender: 'agent'
        }
      },
      {
        $group: {
          _id: null,
          sent: { $sum: 1 },
          delivered: {
            $sum: {
              $cond: [{ $in: ['$status', ['delivered', 'read']] }, 1, 0]
            }
          },
          read: {
            $sum: {
              $cond: [{ $eq: ['$status', 'read'] }, 1, 0]
            }
          },
          failed: {
            $sum: {
              $cond: [{ $eq: ['$status', 'failed'] }, 1, 0]
            }
          }
        }
      }
    ]);

    // Merge message-level + broadcast-level totals.
    // This avoids undercount when some failures are stored only in broadcast stats.
    const headlineTotals = messageTotals[0] || { sent: 0, delivered: 0, read: 0, failed: 0 };
    const messagesFailed = Math.max(Number(headlineTotals.failed || 0), Number(campaignTotals.failed || 0));
    const messagesRead = Math.max(Number(headlineTotals.read || 0), Number(campaignTotals.read || 0));
    const totalDeliveredOrRead = Math.max(Number(headlineTotals.delivered || 0), Number(campaignTotals.delivered || 0));
    const messagesSent = Math.max(
      Number(headlineTotals.sent || 0),
      Number(campaignTotals.sent || 0),
      totalDeliveredOrRead + messagesFailed
    );
    
    const messagesReceived = await Message.countDocuments({
      userId: req.user.id,
      sender: 'contact',
      timestamp: { $gte: last7Days }
    });

    // Build daily trend buckets for the last 7 days using agent messages.
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const dailyMap = new Map();
    const dailyConversationSets = new Map();
    const dailyStatusMap = new Map();

    for (let i = 6; i >= 0; i -= 1) {
      const day = new Date();
      day.setHours(0, 0, 0, 0);
      day.setDate(day.getDate() - i);
      const key = day.toISOString().slice(0, 10);

      dailyMap.set(key, { date: dayNames[day.getDay()], sent: 0, delivered: 0, read: 0, conversations: 0 });
      dailyConversationSets.set(key, new Set());
      dailyStatusMap.set(key, { delivered: 0, read: 0, failed: 0, pending: 0 });
    }

    allAgentMessages.forEach((msg) => {
      const ts = new Date(msg.timestamp);
      ts.setHours(0, 0, 0, 0);
      const key = ts.toISOString().slice(0, 10);
      if (!dailyMap.has(key)) return;

      const bucket = dailyMap.get(key);
      const statusBucket = dailyStatusMap.get(key);
      bucket.sent += 1;

      if (msg.status === 'delivered' || msg.status === 'read') {
        bucket.delivered += 1;
        statusBucket.delivered += 1;
      }
      if (msg.status === 'read') {
        bucket.read += 1;
        statusBucket.read += 1;
      }
      if (msg.status === 'failed') {
        statusBucket.failed += 1;
      }
      if (msg.status === 'pending' || msg.status === 'sent') {
        statusBucket.pending += 1;
      }

      if (msg.conversationId) {
        dailyConversationSets.get(key).add(String(msg.conversationId));
      }
    });

    const dailyTrends = Array.from(dailyMap.entries()).map(([key, value]) => ({
      ...value,
      conversations: dailyConversationSets.get(key).size
    }));

    // Build hourly activity buckets for the last 12 hours.
    const hourlyMap = new Map();
    const hourlyConversationSets = new Map();
    for (let i = 11; i >= 0; i -= 1) {
      const hour = new Date();
      hour.setMinutes(0, 0, 0);
      hour.setHours(hour.getHours() - i);
      const key = hour.toISOString().slice(0, 13);
      const label = hour.toLocaleTimeString('en-US', { hour: 'numeric', hour12: true });
      hourlyMap.set(key, { hour: label, messages: 0, conversations: 0 });
      hourlyConversationSets.set(key, new Set());
    }

    allAgentMessages.forEach((msg) => {
      const hour = new Date(msg.timestamp);
      hour.setMinutes(0, 0, 0);
      const key = hour.toISOString().slice(0, 13);
      if (!hourlyMap.has(key)) return;
      hourlyMap.get(key).messages += 1;
      if (msg.conversationId) {
        hourlyConversationSets.get(key).add(String(msg.conversationId));
      }
    });

    const hourlyActivity = Array.from(hourlyMap.entries()).map(([key, value]) => ({
      ...value,
      conversations: hourlyConversationSets.get(key).size
    }));

    // Message type split: text vs media categories.
    const messageTypeCounts = {
      text: 0,
      image: 0,
      video: 0,
      audio: 0,
      document: 0
    };

    allAgentMessages.forEach((msg) => {
      const mediaType = msg.mediaType || 'text';
      if (Object.prototype.hasOwnProperty.call(messageTypeCounts, mediaType)) {
        messageTypeCounts[mediaType] += 1;
      } else {
        messageTypeCounts.text += 1;
      }
    });

    const messageTypes = [
      { name: 'Text Messages', value: messageTypeCounts.text, color: '#3b82f6' },
      { name: 'Image Messages', value: messageTypeCounts.image, color: '#2563eb' },
      { name: 'Video Messages', value: messageTypeCounts.video, color: '#f59e0b' },
      { name: 'Document/Audio', value: messageTypeCounts.document + messageTypeCounts.audio, color: '#8b5cf6' }
    ];

    const deliveryRate = messagesSent > 0 ? ((totalDeliveredOrRead / messagesSent) * 100).toFixed(1) : '0.0';
    const readRate = messagesSent > 0 ? ((messagesRead / messagesSent) * 100).toFixed(1) : '0.0';

    const analytics = {
      totalConversations,
      activeConversations,
      messagesSent,
      messagesDelivered: totalDeliveredOrRead, // Use total delivered+read for accurate delivery metrics
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
      failedGrowth: '-2',
      dailyTrends,
      hourlyActivity,
      messageTypes,
      performanceMetrics: {
        avgDeliveryTime: '1.2 minutes',
        avgReadTime: '8.5 minutes',
        peakHour: hourlyActivity.length ? hourlyActivity.reduce((a, b) => (a.messages > b.messages ? a : b)).hour : 'N/A',
        bestDay: dailyTrends.length ? dailyTrends.reduce((a, b) => (a.sent > b.sent ? a : b)).date : 'N/A',
        deliveryRate: `${deliveryRate}%`,
        readRate: `${readRate}%`
      }
    };

    res.json(analytics);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Manual sync endpoint for debugging broadcast stats
app.post('/api/broadcasts/:id/sync', auth, async (req, res) => {
  try {
    const { id } = req.params;
    console.log('🔄 Manual sync requested for broadcast:', id);
    
    const ownsBroadcast = await Broadcast.findOne({ _id: id, createdById: req.user.id }).select('_id').lean();
    if (!ownsBroadcast) {
      return res.status(404).json({ success: false, error: 'Broadcast not found' });
    }

    const result = await broadcastService.syncBroadcastStats(id);
    
    if (result.success) {
      console.log('✅ Manual sync completed:', result.data);
      res.json(result);
    } else {
      console.log('❌ Manual sync failed:', result.error);
      res.status(400).json(result);
    }
  } catch (error) {
    console.error('❌ Error in manual sync:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/version', (req, res) => {
  res.json({
    service: 'whatsapp-backend',
    version: 'bulk_direct_meta_v2',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/debug/auth-credentials', auth, async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const credentials = await getWhatsAppCredentialsForUser({
      authHeader,
      userId: req.user?.id || null
    });

    res.json({
      success: true,
      user: req.user,
      hasAuthHeader: Boolean(authHeader),
      credentialsFound: Boolean(credentials),
      credentials: credentials
        ? {
            twilioId: credentials.twilioId,
            whatsappId: credentials.whatsappId,
            whatsappBusiness: credentials.whatsappBusiness,
            accessTokenPreview: `${String(credentials.accessToken).slice(0, 10)}...`
          }
        : null
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      user: req.user
    });
  }
});

// Start server
const PORT = process.env.PORT || 3001;
server.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📡 WebSocket server ready`);
  console.log(`🌐 Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:5173'}`);
  
  // Start the scheduler for checking scheduled broadcasts
  try {
    await Promise.all([Contact.syncIndexes(), Template.syncIndexes(), MetaAdsWallet.syncIndexes(), MetaAdsTransaction.syncIndexes()]);
    console.log('MongoDB indexes synced for user-scoped data models.');
  } catch (indexError) {
    console.error('Failed to sync MongoDB indexes:', indexError.message);
  }

  startScheduler();
  
  console.log('Skipping global template sync on startup: credentials are user-scoped and fetched per request.');
});

// Scheduler to check for scheduled broadcasts every minute
function startScheduler() {
  console.log('⏰ Starting broadcast scheduler...');
  
  // Run every minute
  cron.schedule('* * * * *', async () => {
    try {
      console.log('🔍 Checking for scheduled broadcasts...');
      
      // Check database connection before proceeding
      if (mongoose.connection.readyState !== 1) {
        console.log('⚠️ Database not connected, skipping scheduler run');
        return;
      }
      
      await broadcastService.checkScheduledBroadcasts();
      await missedCallAutomationService.processPendingMissedCalls({ app });
      
      // Also check for recent message status updates and sync broadcast stats
      // removed automatic stat backfill to avoid metric fluctuations
      
    } catch (error) {
      console.error('❌ Scheduler error:', error.message);
      // Don't exit the process, just log the error and continue
    }
  });

  // Sync templates from Meta every 6 hours
  cron.schedule('0 */6 * * *', async () => {
    try {
      console.log('🔄 Starting automatic template sync from Meta...');
      
      // Check database connection before proceeding
      if (mongoose.connection.readyState !== 1) {
        console.log('⚠️ Database not connected, skipping template sync');
        return;
      }
      
      // Create a mock request/response for the template sync
      const syncUserId = process.env.TEMPLATE_SYNC_USER_ID || process.env.DEFAULT_USER_ID || null;
      const mockReq = {
        syncUserId: syncUserId || undefined,
        whatsappCredentials: {
          businessAccountId: process.env.WHATSAPP_BUSINESS_ACCOUNT_ID,
          phoneNumberId: process.env.WHATSAPP_PHONE_NUMBER_ID,
          accessToken: process.env.WHATSAPP_ACCESS_TOKEN,
          webhookVerifyToken: process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN
        }
      };
      const mockRes = {
        status: (code) => ({
          json: (data) => {
            if (code === 200) {
              console.log('✅ Automatic template sync completed:', data.message);
            } else {
              console.error('❌ Automatic template sync failed:', data.error);
            }
          }
        }),
        json: (data) => {
          console.log('✅ Automatic template sync completed:', data.message);
        }
      };
      if (!mockReq.syncUserId) {
        console.warn('⚠️ TEMPLATE_SYNC_USER_ID not set; skipping automatic template sync');
        return;
      }

      if (!mockReq.whatsappCredentials.businessAccountId || !mockReq.whatsappCredentials.phoneNumberId || !mockReq.whatsappCredentials.accessToken) {
        console.warn('⚠️ WhatsApp credentials missing in env; skipping automatic template sync');
        return;
      }

      await templateController.syncWhatsAppTemplates(mockReq, mockRes);
      
    } catch (error) {
      console.error('❌ Automatic template sync error:', error.message);
    }
  });

  console.log('✅ Broadcast scheduler started - checking every minute');
  cron.schedule('*/5 * * * *', async () => {
    try {
      if (mongoose.connection.readyState !== 1) {
        console.log('Skipping Meta Ads sync: database not connected');
        return;
      }

      const result = await metaAdsService.syncAllCampaignAnalytics();
      if (result.synced || result.warnings.length) {
        console.log(`Meta Ads sync completed. Synced: ${result.synced}, warnings: ${result.warnings.length}`);
      }
    } catch (error) {
      console.error('Meta Ads sync error:', error.message);
    }
  });

  console.log('Template auto-sync disabled: run per user via authenticated API.');
}

// Check for recent message status updates and sync broadcast stats
async function checkAndUpdateBroadcastStats() {
  // intentionally disabled to prevent periodic stat rollback/fluctuation
  return;
}




