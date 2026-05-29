const mongoose = require('mongoose');

const ConversationSummarySchema = new mongoose.Schema({
  conversationId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Conversation',
    required: true,
    unique: true,
    index: true
  },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', index: true, default: null },
  contactPhone: { type: String, index: true, default: '' },
  contactPhoneDigits: { type: String, index: true, default: '' },
  contactName: { type: String, default: '' },
  contactNameLower: { type: String, index: true, default: '' },
  channel: {
    type: String,
    enum: ['whatsapp', 'broadcast_reply', 'instagram', 'facebook', 'group', 'voice', 'manual', 'unknown'],
    default: 'whatsapp',
    index: true
  },
  status: {
    type: String,
    enum: ['active', 'resolved', 'pending', 'archived'],
    default: 'active',
    index: true
  },
  leadStatus: {
    type: String,
    enum: ['new_lead', 'interested', 'follow_up', 'proposal_sent', 'converted', 'closed'],
    default: 'new_lead',
    index: true
  },
  assignedTo: { type: String, default: null },
  assignedToId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  assignedAgent: { type: String, default: null, index: true },
  tags: [{ type: String }],
  important: { type: Boolean, default: false, index: true },
  priority: { type: String, enum: ['low', 'medium', 'high'], default: 'medium' },
  followupAt: { type: Date, default: null, index: true },
  lastMessageTime: { type: Date, default: Date.now, index: true },
  lastMessage: { type: String, default: '' },
  lastMessageMediaType: { type: String, default: '' },
  lastMessageAttachmentName: { type: String, default: '' },
  lastMessageAttachmentPages: { type: Number, default: null },
  lastMessageFrom: { type: String, enum: ['contact', 'agent'], default: 'contact' },
  lastMessageWhatsappMessageId: { type: String, default: '' },
  lastMessageStatus: { type: String, default: '' },
  unreadCount: { type: Number, default: 0 },
  notes: { type: String, default: '' },
  internalNotes: { type: [mongoose.Schema.Types.Mixed], default: [] },
  resolvedAt: { type: Date, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

ConversationSummarySchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

ConversationSummarySchema.index({ companyId: 1, userId: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, assignedTo: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, assignedAgent: 1, leadStatus: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, contactId: 1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, leadStatus: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, unreadCount: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, important: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, followupAt: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, contactPhoneDigits: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, contactNameLower: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, channel: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, assignedTo: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, assignedTo: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, assignedAgent: 1, leadStatus: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, important: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, followupAt: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, unreadCount: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, contactPhoneDigits: 1, lastMessageTime: -1, _id: -1 });
ConversationSummarySchema.index({ companyId: 1, userId: 1, contactNameLower: 1, lastMessageTime: -1, _id: -1 });

module.exports = mongoose.model('ConversationSummary', ConversationSummarySchema);
