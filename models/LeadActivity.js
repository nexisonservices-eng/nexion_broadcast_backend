const mongoose = require('mongoose');

const LeadActivitySchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true, index: true },
  conversationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Conversation', default: null, index: true },
  type: {
    type: String,
    enum: [
      'contact_created',
      'contact_updated',
      'stage_changed',
      'owner_changed',
      'status_changed',
      'note_updated',
      'document_uploaded',
      'document_deleted',
      'task_created',
      'task_updated',
      'task_completed',
      'meeting_scheduled'
    ],
    required: true,
    index: true
  },
  meta: { type: mongoose.Schema.Types.Mixed, default: {} },
  createdBy: { type: String, default: null }
}, {
  timestamps: true
});

LeadActivitySchema.index({ companyId: 1, userId: 1, contactId: 1, createdAt: -1 });
LeadActivitySchema.index({ companyId: 1, userId: 1, type: 1, createdAt: -1 });

module.exports = mongoose.model('LeadActivity', LeadActivitySchema);
