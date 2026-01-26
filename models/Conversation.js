const mongoose = require('mongoose');

const ConversationSchema = new mongoose.Schema({
  contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true },
  contactPhone: { type: String, required: true, index: true },
  contactName: String,
  status: { 
    type: String, 
    enum: ['active', 'resolved', 'pending', 'archived'], 
    default: 'active',
    index: true
  },
  assignedTo: { type: String, default: null },
  assignedToId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  tags: [{ type: String }],
  priority: { type: String, enum: ['low', 'medium', 'high'], default: 'medium' },
  lastMessageTime: { type: Date, default: Date.now, index: true },
  lastMessage: String,
  lastMessageFrom: { type: String, enum: ['contact', 'agent'] },
  unreadCount: { type: Number, default: 0 },
  notes: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  resolvedAt: Date
});

ConversationSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  if (this.status === 'resolved' && !this.resolvedAt) {
    this.resolvedAt = Date.now();
  }
  next();
});

ConversationSchema.index({ contactPhone: 1, status: 1 });
ConversationSchema.index({ assignedTo: 1, status: 1 });

module.exports = mongoose.model('Conversation', ConversationSchema);
