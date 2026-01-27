const mongoose = require('mongoose');

const MessageSchema = new mongoose.Schema({
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
  timestamp: { type: Date, default: Date.now, index: true },
  isForwarded: { type: Boolean, default: false },
  forwardedFrom: String,
  replyTo: { type: mongoose.Schema.Types.ObjectId, ref: 'Message' }
});

MessageSchema.index({ conversationId: 1, timestamp: 1 });

module.exports = mongoose.model('Message', MessageSchema);
