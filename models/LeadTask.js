const mongoose = require('mongoose');

const LeadTaskSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true, index: true },
  conversationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Conversation', default: null, index: true },
  title: { type: String, required: true, trim: true, maxlength: 200 },
  description: { type: String, default: '', trim: true, maxlength: 2000 },
  dueAt: { type: Date, default: null, index: true },
  priority: { type: String, enum: ['low', 'medium', 'high'], default: 'medium', index: true },
  status: {
    type: String,
    enum: ['pending', 'in_progress', 'completed', 'cancelled'],
    default: 'pending',
    index: true
  },
  assignedTo: { type: String, default: null, index: true },
  createdBy: { type: String, default: null },
  completedAt: { type: Date, default: null }
}, {
  timestamps: true
});

LeadTaskSchema.index({ companyId: 1, userId: 1, status: 1, dueAt: 1 });
LeadTaskSchema.index({ companyId: 1, userId: 1, contactId: 1, createdAt: -1 });

module.exports = mongoose.model('LeadTask', LeadTaskSchema);
