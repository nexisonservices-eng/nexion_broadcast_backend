const mongoose = require('mongoose');

const ConversationSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, default: null },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true },
  contactPhone: { type: String, required: true, index: true },
  contactPhoneDigits: { type: String, default: '', index: true },
  contactName: String,
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
  assignedToId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  assignedAgent: { type: String, default: null, index: true },
  tags: [{ type: String }],
  important: { type: Boolean, default: false, index: true },
  priority: { type: String, enum: ['low', 'medium', 'high'], default: 'medium' },
  followupAt: { type: Date, default: null, index: true },
  internalNotes: {
    type: [
      {
        text: { type: String, required: true, trim: true, maxlength: 2000 },
        createdBy: { type: String, default: null },
        createdAt: { type: Date, default: Date.now }
      }
    ],
    default: []
  },
  lastMessageTime: { type: Date, default: Date.now, index: true },
  lastMessage: String,
  lastMessageMediaType: { type: String, default: '' },
  lastMessageAttachmentName: { type: String, default: '' },
  lastMessageAttachmentPages: { type: Number, default: null },
  lastMessageFrom: { type: String, enum: ['contact', 'agent'] },
  lastMessageWhatsappMessageId: { type: String, default: '' },
  lastMessageStatus: { type: String, default: '' },
  unreadCount: { type: Number, default: 0 },
  notes: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  resolvedAt: Date
});

ConversationSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  this.contactPhoneDigits = String(this.contactPhone || '').replace(/\D/g, '');
  if (this.status === 'resolved' && !this.resolvedAt) {
    this.resolvedAt = Date.now();
  }
  next();
});

ConversationSchema.pre(['findOneAndUpdate', 'updateOne', 'updateMany', 'replaceOne'], function(next) {
  const update = this.getUpdate() || {};
  const operatorKeys = Object.keys(update).filter((key) => key.startsWith('$'));
  const isReplacementUpdate = operatorKeys.length === 0;

  if (isReplacementUpdate) {
    if (update.contactPhone !== undefined) {
      update.contactPhoneDigits = String(update.contactPhone || '').replace(/\D/g, '');
    }
    update.updatedAt = Date.now();
    this.setUpdate(update);
    next();
    return;
  }

  const $set = { ...(update.$set || {}) };
  if (update.contactPhone !== undefined && $set.contactPhone === undefined) {
    $set.contactPhone = update.contactPhone;
    delete update.contactPhone;
  }
  if ($set.contactPhone !== undefined) {
    $set.contactPhoneDigits = String($set.contactPhone || '').replace(/\D/g, '');
  }
  $set.updatedAt = Date.now();
  update.$set = $set;
  this.setUpdate(update);
  next();
});

ConversationSchema.index({ companyId: 1, userId: 1, contactPhone: 1, status: 1 });
<<<<<<< Updated upstream
ConversationSchema.index({ companyId: 1, userId: 1, contactPhoneDigits: 1, status: 1 });
=======
>>>>>>> Stashed changes
ConversationSchema.index({ companyId: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, assignedAgent: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, assignedTo: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, unreadCount: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, important: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, followupAt: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, contactPhone: 1, lastMessageTime: -1, _id: -1 });
<<<<<<< Updated upstream
ConversationSchema.index({ companyId: 1, contactPhoneDigits: 1, lastMessageTime: -1, _id: -1 });
=======
>>>>>>> Stashed changes
ConversationSchema.index({ companyId: 1, contactName: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, channel: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, contactId: 1 });
ConversationSchema.index({ companyId: 1, userId: 1, assignedTo: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, assignedAgent: 1, leadStatus: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, important: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, followupAt: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, status: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, unreadCount: 1, lastMessageTime: -1, _id: -1 });
ConversationSchema.index({ companyId: 1, userId: 1, contactPhoneDigits: 1, lastMessageTime: -1, _id: -1 });

module.exports = mongoose.model('Conversation', ConversationSchema);
