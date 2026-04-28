const mongoose = require('mongoose');

const CrmAutomationRunSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null, index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', default: null, index: true },
    triggerSource: {
      type: String,
      enum: ['scheduler', 'manual_preview', 'manual_run'],
      required: true,
      index: true
    },
    automationActor: { type: String, default: null },
    dryRun: { type: Boolean, default: false },
    candidateCount: { type: Number, default: 0 },
    createdCount: { type: Number, default: 0 },
    byRule: { type: mongoose.Schema.Types.Mixed, default: {} },
    tasksPreview: { type: Array, default: [] },
    contactUpdatesPreview: { type: Array, default: [] },
    ownerNotificationsPreview: { type: Array, default: [] },
    emailNotifications: {
      attempted: { type: Number, default: 0 },
      delivered: { type: Number, default: 0 },
      skipped: { type: Number, default: 0 },
      failed: { type: Number, default: 0 }
    },
    slaHours: { type: Number, default: 0 },
    generatedAt: { type: Date, default: Date.now, index: true },
    status: {
      type: String,
      enum: ['success', 'error'],
      default: 'success',
      index: true
    },
    errorMessage: { type: String, default: '' }
  },
  {
    timestamps: true
  }
);

CrmAutomationRunSchema.index({ companyId: 1, userId: 1, generatedAt: -1 });
CrmAutomationRunSchema.index({ triggerSource: 1, generatedAt: -1 });

module.exports = mongoose.model('CrmAutomationRun', CrmAutomationRunSchema);
