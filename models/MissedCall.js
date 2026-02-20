const mongoose = require('mongoose');

const MissedCallSchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      required: true,
      index: true
    },
    fromNumber: { type: String, required: true, index: true },
    toNumber: { type: String, default: '', index: true },
    callerName: { type: String, default: '' },
    direction: {
      type: String,
      enum: ['inbound', 'outbound'],
      default: 'inbound'
    },
    status: {
      type: String,
      enum: ['missed', 'resolved'],
      default: 'missed',
      index: true
    },
    provider: { type: String, default: 'webhook' },
    notes: { type: String, default: '' },
    calledAt: { type: Date, default: Date.now, index: true },
    resolvedAt: { type: Date, default: null },
    payload: { type: mongoose.Schema.Types.Mixed, default: {} }
    ,
    automation: {
      enabled: { type: Boolean, default: true },
      mode: {
        type: String,
        enum: ['immediate', 'night_batch'],
        default: 'immediate'
      },
      status: {
        type: String,
        enum: ['pending', 'processing', 'sent', 'failed', 'disabled'],
        default: 'pending',
        index: true
      },
      templateName: { type: String, default: '' },
      templateLanguage: { type: String, default: 'en_US' },
      templateVariables: { type: [mongoose.Schema.Types.Mixed], default: [] },
      delayMinutes: { type: Number, default: 5 },
      nightHour: { type: Number, default: 21 },
      nightMinute: { type: Number, default: 0 },
      timezone: { type: String, default: 'Asia/Kolkata' },
      nextRunAt: { type: Date, default: null, index: true },
      sentAt: { type: Date, default: null },
      messageId: { type: String, default: '' },
      attempts: { type: Number, default: 0 },
      lastError: { type: String, default: '' }
    }
  },
  {
    timestamps: true
  }
);

MissedCallSchema.index({ userId: 1, calledAt: -1 });

module.exports = mongoose.model('MissedCall', MissedCallSchema);
