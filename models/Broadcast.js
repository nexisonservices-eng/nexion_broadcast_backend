const mongoose = require('mongoose');

const BroadcastSchema = new mongoose.Schema({
  name: { type: String, required: true },
  message: String,
  templateName: String,
  templateId: { type: mongoose.Schema.Types.ObjectId, ref: 'Template' },
  mediaUrl: String,
  mediaType: String,
  recipients: [{ 
    phone: { type: String, required: true },
    name: String,
    variables: [String]
  }],
  recipientCount: { type: Number, default: 0 },
  status: { 
    type: String, 
    enum: ['draft', 'scheduled', 'sending', 'completed', 'paused', 'cancelled'], 
    default: 'draft',
    index: true
  },
  scheduledAt: Date,
  startedAt: Date,
  completedAt: Date,
  stats: {
    sent: { type: Number, default: 0 },
    delivered: { type: Number, default: 0 },
    read: { type: Number, default: 0 },
    failed: { type: Number, default: 0 },
    replied: { type: Number, default: 0 }
  },
  variables: mongoose.Schema.Types.Mixed,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  createdBy: String,
  createdById: { type: mongoose.Schema.Types.ObjectId, ref: 'User' }
});

BroadcastSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  if (this.recipients && !this.recipientCount) {
    this.recipientCount = this.recipients.length;
  }
  next();
});

BroadcastSchema.index({ status: 1, createdAt: -1 });
BroadcastSchema.index({ createdBy: 1 });

module.exports = mongoose.model('Broadcast', BroadcastSchema);
