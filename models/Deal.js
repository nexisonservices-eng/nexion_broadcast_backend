const mongoose = require('mongoose');

const DealSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
    contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true, index: true },
    conversationId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Conversation',
      default: null,
      index: true
    },
    title: { type: String, required: true, trim: true, maxlength: 200 },
    stage: {
      type: String,
      enum: ['discovery', 'qualified', 'proposal', 'negotiation', 'won', 'lost'],
      default: 'discovery',
      index: true
    },
    status: {
      type: String,
      enum: ['open', 'won', 'lost'],
      default: 'open',
      index: true
    },
    value: { type: Number, default: 0, min: 0 },
    probability: { type: Number, default: 0, min: 0, max: 100 },
    currency: { type: String, default: 'INR', trim: true, maxlength: 12 },
    expectedCloseAt: { type: Date, default: null, index: true },
    ownerId: { type: String, default: null, index: true },
    productName: { type: String, default: '', trim: true, maxlength: 200 },
    source: { type: String, default: '', trim: true, maxlength: 200 },
    notes: { type: String, default: '', trim: true, maxlength: 4000 },
    lostReason: { type: String, default: '', trim: true, maxlength: 1000 },
    wonAt: { type: Date, default: null },
    lostAt: { type: Date, default: null },
    createdBy: { type: String, default: null },
    updatedBy: { type: String, default: null }
  },
  {
    timestamps: true
  }
);

DealSchema.index({ companyId: 1, userId: 1, stage: 1, status: 1, expectedCloseAt: 1 });
DealSchema.index({ companyId: 1, userId: 1, contactId: 1, updatedAt: -1 });
DealSchema.index({ companyId: 1, userId: 1, ownerId: 1, status: 1, expectedCloseAt: 1 });

module.exports = mongoose.model('Deal', DealSchema);
