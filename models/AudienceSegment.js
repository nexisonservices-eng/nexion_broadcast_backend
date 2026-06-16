const mongoose = require('mongoose');

const SegmentContactSchema = new mongoose.Schema(
  {
    contactId: { type: String, default: '' },
    phone: { type: String, required: true },
    name: { type: String, default: '' },
    sourceType: { type: String, default: 'manual' },
    whatsappOptInStatus: { type: String, default: 'unknown' }
  },
  { _id: false }
);

const AudienceSegmentSchema = new mongoose.Schema(
  {
    name: { type: String, required: true, trim: true },
    description: { type: String, default: '', trim: true },
    sourceType: { type: String, default: 'manual', trim: true },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, required: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true, default: null },
    filters: { type: mongoose.Schema.Types.Mixed, default: {} },
    contacts: { type: [SegmentContactSchema], default: [] },
    recipientCount: { type: Number, default: 0 },
    createdBy: { type: String, default: '', trim: true },
    updatedBy: { type: String, default: '', trim: true }
  },
  {
    timestamps: true
  }
);

AudienceSegmentSchema.index({ userId: 1, companyId: 1, name: 1 }, { unique: true });
AudienceSegmentSchema.index({ userId: 1, companyId: 1, updatedAt: -1 });

module.exports = mongoose.model('AudienceSegment', AudienceSegmentSchema);
