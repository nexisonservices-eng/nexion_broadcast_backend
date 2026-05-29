const mongoose = require('mongoose');

const RealtimeOutboxEventSchema = new mongoose.Schema(
  {
    eventType: { type: String, required: true, index: true },
    scope: {
      type: String,
      enum: ['user', 'company', 'broadcast', 'room', 'global'],
      default: 'global',
      index: true
    },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null, index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', default: null, index: true },
    conversationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Conversation', default: null, index: true },
    room: { type: String, default: '', index: true },
    dedupeKey: { type: String, required: true, unique: true, index: true },
    payload: { type: mongoose.Schema.Types.Mixed, required: true },
    status: {
      type: String,
      enum: ['pending', 'processing', 'published', 'failed', 'dead'],
      default: 'pending',
      index: true
    },
    priority: { type: Number, default: 0, index: true },
    attempts: { type: Number, default: 0 },
    availableAt: { type: Date, default: Date.now, index: true },
    lockedAt: { type: Date, default: null, index: true },
    lockOwner: { type: String, default: '', index: true },
    publishedAt: { type: Date, default: null, index: true },
    lastError: { type: String, default: '' },
    nextAttemptAt: { type: Date, default: null, index: true },
    source: { type: String, default: 'teamInbox', index: true }
  },
  {
    timestamps: true,
    minimize: false
  }
);

RealtimeOutboxEventSchema.index({
  status: 1,
  availableAt: 1,
  priority: -1,
  createdAt: 1
});
RealtimeOutboxEventSchema.index({
  status: 1,
  nextAttemptAt: 1,
  lockedAt: 1
});

module.exports = mongoose.models.RealtimeOutboxEvent ||
  mongoose.model('RealtimeOutboxEvent', RealtimeOutboxEventSchema);
