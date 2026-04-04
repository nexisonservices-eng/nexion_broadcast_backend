const mongoose = require('mongoose');

const WhatsAppWorkflowSchema = new mongoose.Schema(
  {
    workflowId: { type: String, required: true, trim: true },
    name: { type: String, required: true, trim: true, default: 'Untitled WhatsApp Workflow' },
    description: { type: String, default: '', trim: true },
    status: {
      type: String,
      enum: ['draft', 'active', 'archived'],
      default: 'draft'
    },
    nodes: {
      type: [mongoose.Schema.Types.Mixed],
      default: []
    },
    edges: {
      type: [mongoose.Schema.Types.Mixed],
      default: []
    },
    userId: { type: String, required: true, index: true },
    companyId: { type: String, default: '', index: true },
    createdBy: { type: String, default: '', trim: true },
    version: { type: Number, default: 1 },
    lastRunAt: { type: Date, default: null },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} }
  },
  {
    timestamps: true
  }
);

WhatsAppWorkflowSchema.index({ companyId: 1, userId: 1, workflowId: 1 }, { unique: true });
WhatsAppWorkflowSchema.index({ companyId: 1, userId: 1, updatedAt: -1 });

module.exports = mongoose.model('WhatsAppWorkflow', WhatsAppWorkflowSchema);
