const mongoose = require('mongoose');

const MessageArchiveSchema = new mongoose.Schema({
  sourceId: { type: mongoose.Schema.Types.ObjectId, required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true },
  conversationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Conversation', index: true },
  originalCreatedAt: { type: Date, index: true },
  archivedAt: { type: Date, default: Date.now, index: true },
  payload: { type: mongoose.Schema.Types.Mixed, required: true }
}, {
  minimize: false,
  collection: 'message_archives'
});

MessageArchiveSchema.index({ companyId: 1, userId: 1, conversationId: 1, originalCreatedAt: -1, _id: -1 });
MessageArchiveSchema.index({ companyId: 1, userId: 1, originalCreatedAt: -1, _id: -1 });

module.exports = mongoose.model('MessageArchive', MessageArchiveSchema);
