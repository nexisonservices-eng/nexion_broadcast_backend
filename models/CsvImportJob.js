const mongoose = require('mongoose');

const CsvImportJobSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, required: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true, default: null },
    originalFileName: { type: String, default: '' },
    storedFileName: { type: String, default: '' },
    filePath: { type: String, default: '' },
    queueJobId: { type: String, default: '' },
    status: {
      type: String,
      enum: ['queued', 'processing', 'completed', 'failed', 'cancelled'],
      default: 'queued',
      index: true
    },
    totalRows: { type: Number, default: 0 },
    processedRows: { type: Number, default: 0 },
    successCount: { type: Number, default: 0 },
    failedCount: { type: Number, default: 0 },
    duplicateCount: { type: Number, default: 0 },
    skippedCount: { type: Number, default: 0 },
    currentStage: { type: String, default: 'queued' },
    percentComplete: { type: Number, default: 0 },
    etaMs: { type: Number, default: null },
    errorMessage: { type: String, default: '' },
    startedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    lastProgressAt: { type: Date, default: null },
    headers: [{ type: String }]
  },
  { timestamps: true }
);

CsvImportJobSchema.index({ userId: 1, companyId: 1, createdAt: -1 });
CsvImportJobSchema.index({ userId: 1, companyId: 1, status: 1, createdAt: -1 });

module.exports = mongoose.model('CsvImportJob', CsvImportJobSchema);
