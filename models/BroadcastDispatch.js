const mongoose = require('mongoose');

const BroadcastDispatchSchema = new mongoose.Schema({
  broadcastDispatchKey: { type: String, required: true, unique: true, index: true },
  broadcastId: { type: mongoose.Schema.Types.ObjectId, ref: 'Broadcast', index: true, required: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, required: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  recipientPhone: { type: String, required: true },
  status: {
    type: String,
    enum: ['pending', 'sending', 'sent', 'failed', 'suppressed', 'skipped'],
    default: 'pending',
    index: true
  },
  claimedAt: { type: Date, default: null, index: true },
  sentAt: { type: Date, default: null },
  failedAt: { type: Date, default: null },
  whatsappMessageId: { type: String, default: '' },
  messageText: { type: String, default: '' },
  messageKind: { type: String, default: 'text' },
  templateName: { type: String, default: '' },
  templateLanguage: { type: String, default: '' },
  conversationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Conversation', default: null },
  messageId: { type: mongoose.Schema.Types.ObjectId, ref: 'Message', default: null },
  errorMessage: { type: String, default: '' },
  retryCount: { type: Number, default: 0 },
  lastAttemptAt: { type: Date, default: null },
  chunkId: { type: String, default: '' },
  chunkIndex: { type: Number, default: 0 },
  recipientIndex: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

BroadcastDispatchSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

BroadcastDispatchSchema.index({ broadcastId: 1, status: 1, updatedAt: -1 });
BroadcastDispatchSchema.index({ userId: 1, companyId: 1, createdAt: -1 });

module.exports = mongoose.model('BroadcastDispatch', BroadcastDispatchSchema);
