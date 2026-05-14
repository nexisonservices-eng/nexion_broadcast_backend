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
      sortOrder: { type: String, default: 'newest', enum: ['newest', 'oldest'] },
      archive: { type: String, default: 'active', enum: ['active', 'archived', 'all'] }
    },
    presetType: {
      type: String,
      default: 'filter_preset',
      enum: ['filter_preset'],
      index: true
    }
  },
  {
    timestamps: true
  }
);

CrmPipelineViewSchema.index({ userId: 1, presetType: 1, label: 1 }, { unique: true });

module.exports = mongoose.model('CrmPipelineView', CrmPipelineViewSchema);
