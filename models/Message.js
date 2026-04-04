const mongoose = require('mongoose');

const MessageSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
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
  attachment: {
    storageProvider: { type: String, default: 'cloudinary' },
    direction: { type: String, enum: ['sent', 'received'], default: 'sent' },
    username: String,
    folder: String,
    publicId: String,
    resourceType: { type: String, enum: ['image', 'video', 'raw', 'auto'], default: 'image' },
    fileCategory: { type: String, enum: ['image', 'audio', 'document'], default: undefined },
    mimeType: String,
    originalFileName: String,
    extension: String,
    bytes: { type: Number, default: 0 },
    width: { type: Number, default: null },
    height: { type: Number, default: null },
    pages: { type: Number, default: null },
    secureUrl: String,
    sender: String,
    recipient: String,
    uploadedAt: { type: Date, default: Date.now },
    deletedAt: { type: Date, default: null },
    deletedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null }
  },
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
  rawMessageType: String,
  reactionEmoji: String,
  whatsappContextMessageId: { type: String, index: true },
  interactionType: String,
  interactionId: String,
  interactionTitle: String,
  timestamp: { type: Date, default: Date.now, index: true },
  isForwarded: { type: Boolean, default: false },
  forwardedFrom: String,
  replyTo: { type: mongoose.Schema.Types.ObjectId, ref: 'Message' },
  leadScoring: {
    readScoreApplied: { type: Boolean, default: false },
    readScoreAdded: { type: Number, default: 0 },
    replyScoreApplied: { type: Boolean, default: false },
    replyScoreAdded: { type: Number, default: 0 },
    keywordScoreApplied: { type: Boolean, default: false },
    keywordScoreAdded: { type: Number, default: 0 },
    keywordMatches: [
      {
        keyword: { type: String },
        score: { type: Number, default: 0 }
      }
    ],
    lastScoredAt: Date
  }
});

MessageSchema.index({ companyId: 1, userId: 1, conversationId: 1, timestamp: 1 });
MessageSchema.index({ companyId: 1, userId: 1, whatsappMessageId: 1 }, { unique: true, sparse: true });
MessageSchema.index({ companyId: 1, broadcastId: 1, status: 1 });
MessageSchema.index({ companyId: 1, userId: 1, 'attachment.publicId': 1 });
MessageSchema.index({ companyId: 1, userId: 1, mediaType: 1, timestamp: -1 });

module.exports = mongoose.model('Message', MessageSchema);
