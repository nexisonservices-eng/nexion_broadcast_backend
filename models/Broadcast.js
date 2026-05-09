const mongoose = require('mongoose');

const BroadcastSchema = new mongoose.Schema({
  name: { type: String, required: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  messageType: { type: String, enum: ['template', 'text'], default: 'text' },
  message: String,
  templateName: String,
  templateCategory: String,
  templateContent: String,
  language: String,
  templateId: { type: mongoose.Schema.Types.ObjectId, ref: 'Template' },
  retryOfBroadcastId: { type: mongoose.Schema.Types.ObjectId, ref: 'Broadcast', index: true },
  retryAttempt: { type: Number, default: 0 },
  audienceSource: {
    mode: { type: String, default: 'contacts' },
    label: { type: String, default: '' },
    type: { type: String, default: '' },
    segmentId: { type: String, default: '' },
    sourceName: { type: String, default: '' },
    uploadedFileName: { type: String, default: '' },
    recipientCount: { type: Number, default: 0 },
    selectedContactCount: { type: Number, default: 0 },
    hasContactIds: { type: Boolean, default: false }
  },
  audienceSnapshot: mongoose.Schema.Types.Mixed,
  mediaUrl: String,
  mediaType: String,
  recipients: [{ 
    phone: { type: String, required: true },
    name: String,
    variables: [String],
    attributes: mongoose.Schema.Types.Mixed
  }],
  deliveryResults: mongoose.Schema.Types.Mixed,
  recipientCount: { type: Number, default: 0 },
  status: { 
    type: String, 
    enum: ['draft', 'scheduled', 'queued', 'sending', 'completed', 'completed_with_errors', 'paused', 'cancelled', 'failed'], 
    default: 'draft',
    index: true
  },
  queueJobId: { type: String, default: '' },
  queueQueuedAt: { type: Date, default: null },
  queueLastError: { type: String, default: '' },
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
  createdById: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  credentialsSnapshot: {
    accessToken: String,
    businessAccountId: String,
    phoneNumberId: String,
    whatsappToken: String,
    whatsappBusiness: String,
    whatsappId: String,
    twilioId: String
  },
  authHeaderSnapshot: String,
  deliveryPolicy: {
    quietHours: {
      enabled: { type: Boolean, default: false },
      startHour: { type: Number, min: 0, max: 23, default: 22 },
      endHour: { type: Number, min: 0, max: 23, default: 8 },
      timezone: { type: String, default: 'UTC' },
      action: { type: String, enum: ['defer', 'skip'], default: 'defer' }
    },
    batchSize: { type: Number, min: 1, max: 50, default: 50 },
    batchDelaySeconds: { type: Number, min: 0, max: 3600, default: 5 }
  },
  retryPolicy: {
    enabled: { type: Boolean, default: true },
    maxAttempts: { type: Number, min: 1, max: 5, default: 2 },
    backoffSeconds: { type: Number, min: 0, max: 300, default: 4 },
    retryableCodes: [{ type: String }]
  },
  compliancePolicy: {
    respectOptOut: { type: Boolean, default: true },
    suppressionListPhones: [{ type: String }]
  },
  analytics: {
    suppressed: { type: Number, default: 0 },
    deferred: { type: Number, default: 0 },
    retried: { type: Number, default: 0 },
    failureCodeBreakdown: mongoose.Schema.Types.Mixed
  }
});

BroadcastSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  if (this.recipients && !this.recipientCount) {
    this.recipientCount = this.recipients.length;
  }
  next();
});

// Virtual field for replied percentage based on sent count
BroadcastSchema.virtual('repliedPercentage').get(function() {
  if (!this.stats || this.stats.sent === 0) return 0;
  return ((this.stats.replied / this.stats.sent) * 100).toFixed(1);
});

// Virtual field for replied percentage based on total recipients (for reference)
BroadcastSchema.virtual('repliedPercentageOfTotal').get(function() {
  if (!this.recipientCount || this.recipientCount === 0) return 0;
  return ((this.stats.replied / this.recipientCount) * 100).toFixed(1);
});

// Virtual field for read percentage based on sent count
BroadcastSchema.virtual('readPercentage').get(function() {
  if (!this.stats || this.stats.sent === 0) return 0;
  return ((this.stats.read / this.stats.sent) * 100).toFixed(1);
});

// Virtual field for read percentage based on total recipients
BroadcastSchema.virtual('readPercentageOfTotal').get(function() {
  if (!this.recipientCount || this.recipientCount === 0) return 0;
  return ((this.stats.read / this.recipientCount) * 100).toFixed(1);
});

// Virtual field for delivery rate based on sent count
BroadcastSchema.virtual('deliveryRate').get(function() {
  if (!this.stats || this.stats.sent === 0) return 0;
  return ((this.stats.delivered / this.stats.sent) * 100).toFixed(1);
});

// Ensure virtual fields are included in JSON output
BroadcastSchema.set('toJSON', { virtuals: true });
BroadcastSchema.set('toObject', { virtuals: true });

BroadcastSchema.index({ status: 1, createdAt: -1 });
BroadcastSchema.index({ companyId: 1, status: 1, createdAt: -1 });
BroadcastSchema.index({ createdBy: 1 });
BroadcastSchema.index({ createdById: 1, createdAt: -1 });
BroadcastSchema.index({ createdById: 1, status: 1, createdAt: -1 });

module.exports = mongoose.model('Broadcast', BroadcastSchema);
