const mongoose = require('mongoose');

const MessageSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  conversationId: { 
    type: mongoose.Schema.Types.ObjectId, 
    ref: 'Conversation', 
    required: true,
    index: true
  },
  sender: { type: String, enum: ['contact', 'agent', 'bot', 'system'], required: true },
  senderName: String,
  senderId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  text: String,
  mediaUrl: String,
  mediaType: { type: String, enum: ['image', 'video', 'audio', 'document'] },
  mediaCaption: String,
  status: { 
    type: String, 
    enum: ['sent', 'delivered', 'read', 'failed', 'received', 'pending'], 
    default: 'sent',
    index: true
  },
  whatsappMessageId: { type: String, unique: true, sparse: true },
  whatsappTimestamp: Date,
  errorMessage: String,
  broadcastId: { type: mongoose.Schema.Types.ObjectId, ref: 'Broadcast', index: true },
  timestamp: { type: Date, default: Date.now, index: true },
  isForwarded: { type: Boolean, default: false },
  forwardedFrom: String,
  replyTo: { type: mongoose.Schema.Types.ObjectId, ref: 'Message' }
});

MessageSchema.index({ userId: 1, conversationId: 1, timestamp: 1 });
MessageSchema.index({ userId: 1, whatsappMessageId: 1 }, { unique: true, sparse: true });

module.exports = mongoose.model('Message', MessageSchema);
