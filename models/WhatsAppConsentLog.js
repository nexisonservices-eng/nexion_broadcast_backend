const mongoose = require('mongoose');

const WhatsAppConsentLogSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
    contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', index: true },
    phone: { type: String, index: true },
    action: {
      type: String,
      enum: ['opt_in', 'opt_out'],
      required: true,
      index: true
    },
    source: { type: String, default: '' },
    scope: { type: String, default: '' },
    consentText: { type: String, default: '' },
    proofType: { type: String, default: '' },
    proofId: { type: String, default: '' },
    proofUrl: { type: String, default: '' },
    capturedBy: { type: String, default: '' },
    pageUrl: { type: String, default: '' },
    ip: { type: String, default: '' },
    userAgent: { type: String, default: '' },
    metadata: { type: mongoose.Schema.Types.Mixed, default: null }
    ,
    archivedAt: { type: Date, default: null },
    isArchived: { type: Boolean, default: false, index: true }
  },
  { timestamps: true }
);

WhatsAppConsentLogSchema.index({ userId: 1, createdAt: -1 });
WhatsAppConsentLogSchema.index({ companyId: 1, createdAt: -1 });
WhatsAppConsentLogSchema.index({ phone: 1, createdAt: -1 });
WhatsAppConsentLogSchema.index({ isArchived: 1, createdAt: -1 });

module.exports = mongoose.model('WhatsAppConsentLog', WhatsAppConsentLogSchema);
