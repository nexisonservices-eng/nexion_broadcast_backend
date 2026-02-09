// server.js - Main backend server
require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const WebSocket = require('ws');
const cron = require('node-cron');

// Database connection
const connectDB = require('./config/database');
const mongoose = require('mongoose');
connectDB();

// Services
const broadcastService = require('./services/broadcastService');

// Models
const Contact = require('./models/Contact');
const Conversation = require('./models/Conversation');
const Message = require('./models/Message');
const Broadcast = require('./models/Broadcast');
const whatsappService = require('./services/whatsappService');
const whatsappConfig = require('./config/whatsapp');

// Routes
const bulkRoutes = require('./routes/bulk');
const templateRoutes = require('./routes/templates');
const broadcastRoutes = require('./routes/broadcasts');
const conversationRoutes = require('./routes/conversations');
const messageRoutes = require('./routes/messages');
const contactRoutes = require('./routes/contacts');

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
app.use('/api/contacts', contactRoutes);

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

    // Check if this is a reply to a broadcast message
    const broadcast = await Broadcast.findOne({
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
        broadcast({
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
    const recipient = statusData.recipient_id;

    console.log('📊 DEBUG: Received status update:', {
      messageId,
      status,
      recipient,
      timestamp: statusData.timestamp,
      conversationStatus: statusData.conversation?.id
    });

    // First try to find by whatsappMessageId
    let message = await Message.findOne({ whatsappMessageId: messageId });
    
    if (!message) {
      console.log('🔍 DEBUG: Message not found by whatsappMessageId, trying alternative lookup...');
      // Try alternative lookup methods
      message = await Message.findOne({ 
        $or: [
          { whatsappMessageId: messageId },
          { 'whatsappMessageId': messageId }
        ]
      });
    }

    if (message) {
      console.log('📊 DEBUG: Found message to update:', {
        messageId: message._id,
        currentStatus: message.status,
        newStatus: status,
        whatsappMessageId: message.whatsappMessageId
      });

      const updatedMessage = await Message.findOneAndUpdate(
        { whatsappMessageId: messageId },
        { status: status, updatedAt: new Date() },
        { new: true }
      );

      console.log('📊 DEBUG: Message updated successfully:', {
        messageId: updatedMessage._id,
        oldStatus: message.status,
        newStatus: updatedMessage.status
      });

      // Trigger broadcast update
      broadcast({
        type: 'message_status',
        messageId: messageId,
        status: status,
        conversationId: message.conversationId
      });

      // Also update broadcast stats if this is a broadcast message
      if (updatedMessage.conversationId) {
        const conversation = await Conversation.findById(updatedMessage.conversationId);
        if (conversation) {
          console.log('📊 DEBUG: Looking for broadcasts with contact phone:', conversation.contactPhone);
          const broadcasts = await Broadcast.find({
            'recipients.phone': conversation.contactPhone,
            status: { $in: ['sending', 'completed'] } // Only check active/completed broadcasts
          });
          
          for (const broadcast of broadcasts) {
            try {
              console.log('📊 DEBUG: Updating stats for broadcast:', broadcast.name);
              
              // Calculate the proper increment based on status progression
              const oldStatus = message.status;
              const newStatus = updatedMessage.status;
              let update = {};
              
              // Handle status progression to avoid double-counting
              if (oldStatus !== newStatus) {
                if (newStatus === 'delivered' && oldStatus === 'sent') {
                  // Message went from sent to delivered
                  update['stats.delivered'] = 1;
                } else if (newStatus === 'read' && oldStatus !== 'read') {
                  // Message was read (regardless of previous status)
                  update['stats.read'] = 1;
                  // If it wasn't already counted as delivered, count it now
                  if (oldStatus !== 'delivered') {
                    update['stats.delivered'] = 1;
                  }
                } else if (newStatus === 'failed' && oldStatus !== 'failed') {
                  // Message failed, increment failed count
                  update['stats.failed'] = 1;
                  // If it was previously counted as sent, decrement sent
                  if (oldStatus === 'sent') {
                    update['stats.sent'] = -1;
                  }
                }
                
                // Update the broadcast immediately if there are changes
                if (Object.keys(update).length > 0) {
                  await Broadcast.updateOne(
                    { _id: broadcast._id },
                    { $inc: update }
                  );
                  
                  // Get the updated broadcast for logging
                  const updatedBroadcast = await Broadcast.findById(broadcast._id);
                  
                  if (updatedBroadcast) {
                    console.log('⚡ Immediate update for broadcast:', {
                      broadcastId: broadcast._id.toString(),
                      messageStatus: updatedMessage.status,
                      statusChange: `${oldStatus} → ${newStatus}`,
                      updateApplied: update,
                      newStats: updatedBroadcast.stats
                    });
                    
                    // Broadcast the update to all connected clients
                    broadcast({
                      type: 'broadcast_stats_updated',
                      broadcastId: broadcast._id.toString(),
                      stats: updatedBroadcast.stats,
                      statusChange: `${oldStatus} → ${newStatus}`
                    });
                  }
                } else {
                  console.log('📊 DEBUG: No status change needed for broadcast:', broadcast.name);
                }
              } else {
                console.log('📊 DEBUG: Message status unchanged, skipping broadcast update');
              }
            } catch (broadcastError) {
              console.error('❌ Error updating broadcast stats:', broadcastError);
            }
          }
        }
      }
    } else {
      console.log('❌ DEBUG: No message found for whatsappMessageId:', messageId);
    }
  } catch (error) {
    console.error('❌ Error handling message status:', error);
  }
}

// ============ API ROUTES ============
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

// Debug endpoint to investigate broadcast data
app.get('/api/debug-broadcasts', async (req, res) => {
  try {
    const debugInServer = require('./debugInServer');
    await debugInServer();
    res.json({ success: true, message: 'Debug completed - check server logs' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get analytics
app.get('/api/analytics', async (req, res) => {
  try {
    const totalConversations = await Conversation.countDocuments();
    const activeConversations = await Conversation.countDocuments({ status: 'active' });
    
    const last7Days = new Date();
    last7Days.setDate(last7Days.getDate() - 7);
    
    // Message metrics - count all agent messages first, then break down by status
    const allAgentMessages = await Message.find({
      sender: 'agent',
      timestamp: { $gte: last7Days }
    });
    
    const messagesSent = allAgentMessages.length;
    const messagesDelivered = allAgentMessages.filter(msg => msg.status === 'delivered').length;
    const messagesRead = allAgentMessages.filter(msg => msg.status === 'read').length;
    const messagesFailed = allAgentMessages.filter(msg => msg.status === 'failed').length;
    
    // Note: Read messages should also be counted as delivered for accurate metrics
    const totalDeliveredOrRead = allAgentMessages.filter(msg => msg.status === 'delivered' || msg.status === 'read').length;
    
    const messagesReceived = await Message.countDocuments({
      sender: 'contact',
      timestamp: { $gte: last7Days }
    });

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
      failedGrowth: '-2'
    };

    res.json(analytics);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Manual sync endpoint for debugging broadcast stats
app.post('/api/broadcasts/:id/sync', async (req, res) => {
  try {
    const { id } = req.params;
    console.log('🔄 Manual sync requested for broadcast:', id);
    
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
      
      // Check database connection before proceeding
      if (mongoose.connection.readyState !== 1) {
        console.log('⚠️ Database not connected, skipping scheduler run');
        return;
      }
      
      await broadcastService.checkScheduledBroadcasts();
      
      // Also check for recent message status updates and sync broadcast stats
      console.log('🔄 Checking for message status updates...');
      await checkAndUpdateBroadcastStats();
      
    } catch (error) {
      console.error('❌ Scheduler error:', error.message);
      // Don't exit the process, just log the error and continue
    }
  });

  console.log('✅ Broadcast scheduler started - checking every minute');
}

// Check for recent message status updates and sync broadcast stats
async function checkAndUpdateBroadcastStats() {
  try {
    // Find broadcasts that might need syncing (completed in last hour)
    const recentBroadcasts = await Broadcast.find({
      status: 'completed',
      completedAt: { $gte: new Date(Date.now() - 60 * 60 * 1000) } // Last hour
    });

    console.log(`� Checking ${recentBroadcasts.length} recent broadcasts for stats updates`);

    for (const broadcast of recentBroadcasts) {
      // Check if broadcast stats are out of sync
      const startTime = new Date(broadcast.startedAt || broadcast.createdAt);
      const endTime = new Date(broadcast.completedAt || Date.now());
      
      // Find messages for this broadcast (expand time range to catch messages sent before broadcast started)
      const messages = await Message.find({
        sender: 'agent',
        timestamp: { 
          $gte: new Date(startTime.getTime() - 5 * 60 * 1000), // 5 minutes before start
          $lte: new Date(endTime.getTime() + 5 * 60 * 1000)    // 5 minutes after completion
        }
      });

      if (messages.length > 0) {
        // Calculate current stats from messages
        const currentStats = {
          sent: messages.length,
          delivered: messages.filter(msg => msg.status === 'delivered' || msg.status === 'read').length,
          read: messages.filter(msg => msg.status === 'read').length,
          failed: messages.filter(msg => msg.status === 'failed').length,
          replied: 0 // Will be calculated below
        };

        // Count unique contacts who replied to this broadcast
        const recipientPhones = broadcast.recipients.map(r => r.phone);
        const conversations = await Conversation.find({ 
          contactPhone: { $in: recipientPhones } 
        });
        
        const conversationIds = conversations.map(c => c._id);
        const replyMessages = await Message.find({
          conversationId: { $in: conversationIds },
          sender: 'contact',
          timestamp: { $gte: startTime }
        });
        
        // Count unique conversations that have at least one reply
        const uniqueRepliedConversations = new Set(replyMessages.map(msg => msg.conversationId.toString()));
        currentStats.replied = uniqueRepliedConversations.size;

        // Check if stats are different
        const statsChanged = 
          broadcast.stats?.sent !== currentStats.sent ||
          broadcast.stats?.delivered !== currentStats.delivered ||
          broadcast.stats?.read !== currentStats.read ||
          broadcast.stats?.failed !== currentStats.failed ||
          broadcast.stats?.replied !== currentStats.replied;

        if (statsChanged) {
          console.log(`📊 Stats changed for broadcast "${broadcast.name}":`, {
            old: broadcast.stats,
            new: currentStats
          });

          // Update broadcast stats
          await broadcastService.syncBroadcastStats(broadcast._id);
          
          // Get updated broadcast and emit WebSocket event
          const updatedBroadcast = await Broadcast.findById(broadcast._id);
          if (updatedBroadcast) {
            const stringId = broadcast._id.toString();
            console.log('📡 Emitting auto-sync broadcast_stats_updated event:', {
              broadcastId: stringId,
              broadcastName: broadcast.name,
              stats: {
                ...updatedBroadcast.stats,
                repliedPercentage: updatedBroadcast.repliedPercentage,
                repliedPercentageOfTotal: updatedBroadcast.repliedPercentageOfTotal
              }
            });
            app.locals.broadcast({
              type: 'broadcast_stats_updated',
              broadcastId: stringId,
              stats: {
                ...updatedBroadcast.stats,
                repliedPercentage: updatedBroadcast.repliedPercentage,
                repliedPercentageOfTotal: updatedBroadcast.repliedPercentageOfTotal
              }
            });
          }
        }
      }
    }
  } catch (error) {
    console.error('❌ Error checking broadcast stats:', error);
  }
}
