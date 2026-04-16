const mongoose = require('mongoose');

const ContactSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  name: { type: String, default: '' },
  phone: { type: String, required: true, index: true },
  email: String,
  tags: [{ type: String }],
  stage: {
    type: String,
    enum: ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'won', 'lost'],
    default: 'new',
    index: true
  },
  status: {
    type: String,
    enum: ['new', 'nurturing', 'qualified', 'unqualified', 'won', 'lost'],
    default: 'nurturing',
    index: true
  },
  source: { type: String, default: '', index: true },
  ownerId: { type: String, default: null, index: true },
  nextFollowUpAt: { type: Date, default: null, index: true },
  lastContactAt: { type: Date, default: null, index: true },
  customFields: mongoose.Schema.Types.Mixed,
  notes: String,
  sourceType: {
    type: String,
    enum: ['manual', 'imported', 'incoming_message', 'incoming_call'],
    default: 'manual'
  },
  whatsappOptInStatus: {
    type: String,
    enum: ['unknown', 'opted_in', 'opted_out'],
    default: 'unknown'
  },
  whatsappOptInAt: { type: Date, default: null },
  whatsappOptInSource: { type: String, default: '' },
  whatsappOptInScope: {
    type: String,
    enum: ['marketing', 'service', 'both', 'unknown'],
    default: 'unknown'
  },
  whatsappOptInTextSnapshot: { type: String, default: '' },
  whatsappOptInProofType: { type: String, default: '' },
  whatsappOptInProofId: { type: String, default: '' },
  whatsappOptInProofUrl: { type: String, default: '' },
  whatsappOptInCapturedBy: { type: String, default: '' },
  whatsappOptInPageUrl: { type: String, default: '' },
  whatsappOptInIp: { type: String, default: '' },
  whatsappOptInUserAgent: { type: String, default: '' },
  whatsappOptInMetadata: { type: mongoose.Schema.Types.Mixed, default: null },
  whatsappMarketingWindowStartedAt: { type: Date, default: null },
  whatsappMarketingSendCount: { type: Number, default: 0 },
  whatsappMarketingLastSentAt: { type: Date, default: null },
  whatsappOptOutAt: { type: Date, default: null },
  lastInboundMessageAt: { type: Date, default: null },
  serviceWindowClosesAt: { type: Date, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  lastContact: Date,
  isBlocked: { type: Boolean, default: false },
  leadScore: { type: Number, default: 0, index: true },
  leadScoreBreakdown: {
    read: { type: Number, default: 0 },
    reply: { type: Number, default: 0 },
    keyword: { type: Number, default: 0 }
  },
  lastLeadScoreAt: Date
});

ContactSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  if (!this.lastContactAt && this.lastContact) {
    this.lastContactAt = this.lastContact;
  }
  next();
});

ContactSchema.index({ companyId: 1, userId: 1, phone: 1 }, { unique: true });
ContactSchema.index({ userId: 1, lastContact: -1 });
ContactSchema.index({ companyId: 1, lastContact: -1 });
ContactSchema.index({ companyId: 1, userId: 1, stage: 1, status: 1 });
ContactSchema.index({ companyId: 1, userId: 1, ownerId: 1, nextFollowUpAt: 1 });

module.exports = mongoose.model('Contact', ContactSchema);
