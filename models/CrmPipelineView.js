const mongoose = require('mongoose');

const CrmPipelineViewSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      index: true
    },
    companyId: {
      type: String,
      default: '',
      index: true
    },
    label: {
      type: String,
      required: true,
      trim: true,
      maxlength: 80
    },
    filters: {
      search: { type: String, default: '' },
      queue: { type: String, default: 'all' },
      status: { type: String, default: 'all' },
      owner: { type: String, default: 'all' },
      viewMode: { type: String, default: 'list', enum: ['list', 'board'] }
    },
    isDefault: {
      type: Boolean,
      default: false,
      index: true
    }
  },
  {
    timestamps: true
  }
);

CrmPipelineViewSchema.index({ userId: 1, label: 1 }, { unique: true });
CrmPipelineViewSchema.index({ userId: 1, isDefault: 1 });

module.exports = mongoose.model('CrmPipelineView', CrmPipelineViewSchema);
