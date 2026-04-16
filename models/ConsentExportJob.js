const mongoose = require('mongoose');

const ConsentExportJobSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
    requestedBy: { type: String, default: '' },
    email: { type: String, default: '' },
    filters: { type: mongoose.Schema.Types.Mixed, default: null },
    status: {
      type: String,
      enum: ['queued', 'processing', 'completed', 'failed'],
      default: 'queued',
      index: true
    },
    error: { type: String, default: '' },
    checksum: { type: String, default: '' },
    downloadUrl: { type: String, default: '' },
    completedAt: { type: Date, default: null }
  },
  { timestamps: true }
);

ConsentExportJobSchema.index({ status: 1, createdAt: 1 });

module.exports = mongoose.model('ConsentExportJob', ConsentExportJobSchema);
