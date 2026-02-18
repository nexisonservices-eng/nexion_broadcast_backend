const mongoose = require('mongoose');

const ContactSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  name: { type: String, default: '' },
  phone: { type: String, required: true, index: true },
  email: String,
  tags: [{ type: String }],
  customFields: mongoose.Schema.Types.Mixed,
  notes: String,
  sourceType: {
    type: String,
    enum: ['manual', 'imported', 'incoming_message'],
    default: 'manual'
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  lastContact: Date,
  isBlocked: { type: Boolean, default: false }
});

ContactSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

ContactSchema.index({ userId: 1, phone: 1 }, { unique: true });
ContactSchema.index({ userId: 1, lastContact: -1 });

module.exports = mongoose.model('Contact', ContactSchema);
