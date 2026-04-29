const mongoose = require('mongoose');

const CrmPipelineStageSchema = new mongoose.Schema(
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
    key: {
      type: String,
      required: true,
      trim: true
    },
    label: {
      type: String,
      required: true,
      trim: true,
      maxlength: 80
    },
    color: {
      type: String,
      default: '#5f8fc3',
      trim: true
    },
    order: {
      type: Number,
      default: 0,
      index: true
    },
    isArchived: {
      type: Boolean,
      default: false,
      index: true
    }
  },
  {
    timestamps: true
  }
);

CrmPipelineStageSchema.index({ userId: 1, companyId: 1, key: 1 }, { unique: true });
CrmPipelineStageSchema.index({ userId: 1, companyId: 1, isArchived: 1, order: 1 });

module.exports = mongoose.model('CrmPipelineStage', CrmPipelineStageSchema);
