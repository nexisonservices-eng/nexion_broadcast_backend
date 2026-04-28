const mongoose = require('mongoose');

const LeadTaskCommentSchema = new mongoose.Schema(
  {
    text: { type: String, required: true, trim: true, maxlength: 1000 },
    createdBy: { type: String, default: null },
    createdAt: { type: Date, default: Date.now }
  },
  { _id: true }
);

const LeadTaskRecurrenceSchema = new mongoose.Schema(
  {
    frequency: {
      type: String,
      enum: ['none', 'daily', 'weekly', 'monthly'],
      default: 'none'
    },
    interval: { type: Number, default: 1, min: 1, max: 90 }
  },
  { _id: false }
);

const LeadTaskSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true, index: true },
  conversationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Conversation', default: null, index: true },
  title: { type: String, required: true, trim: true, maxlength: 200 },
  description: { type: String, default: '', trim: true, maxlength: 2000 },
  taskType: {
    type: String,
    enum: ['follow_up', 'call', 'whatsapp', 'email', 'meeting', 'demo', 'other'],
    default: 'follow_up',
    index: true
  },
  dueAt: { type: Date, default: null, index: true },
  reminderAt: { type: Date, default: null, index: true },
  priority: { type: String, enum: ['low', 'medium', 'high'], default: 'medium', index: true },
  status: {
    type: String,
    enum: ['pending', 'in_progress', 'completed', 'cancelled'],
    default: 'pending',
    index: true
  },
  assignedTo: { type: String, default: null, index: true },
  createdBy: { type: String, default: null },
  automationRule: { type: String, default: '', index: true },
  automationSource: { type: String, default: '', index: true },
  recurrence: { type: LeadTaskRecurrenceSchema, default: () => ({ frequency: 'none', interval: 1 }) },
  comments: { type: [LeadTaskCommentSchema], default: [] },
  completedAt: { type: Date, default: null },
  completedBy: { type: String, default: null }
}, {
  timestamps: true
});

LeadTaskSchema.index({ companyId: 1, userId: 1, status: 1, dueAt: 1 });
LeadTaskSchema.index({ companyId: 1, userId: 1, contactId: 1, createdAt: -1 });

module.exports = mongoose.model('LeadTask', LeadTaskSchema);
