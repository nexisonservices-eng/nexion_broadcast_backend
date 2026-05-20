const mongoose = require('mongoose');

const BroadcastDispatchArchiveSchema = new mongoose.Schema({
  sourceId: { type: mongoose.Schema.Types.ObjectId, required: true, index: true },
  broadcastId: { type: mongoose.Schema.Types.ObjectId, ref: 'Broadcast', index: true },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true },
  originalCreatedAt: { type: Date, index: true },
  archivedAt: { type: Date, default: Date.now, index: true },
  payload: { type: mongoose.Schema.Types.Mixed, required: true }
}, {
  minimize: false,
  collection: 'broadcast_dispatch_archives'
});

BroadcastDispatchArchiveSchema.index({ companyId: 1, broadcastId: 1, originalCreatedAt: -1, _id: -1 });
BroadcastDispatchArchiveSchema.index({ companyId: 1, userId: 1, originalCreatedAt: -1, _id: -1 });

module.exports = mongoose.model('BroadcastDispatchArchive', BroadcastDispatchArchiveSchema);
