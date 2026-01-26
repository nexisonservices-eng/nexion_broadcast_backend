const mongoose = require('mongoose');

const ContactSchema = new mongoose.Schema({
  name: { type: String, default: '' },
  phone: { type: String, unique: true, required: true, index: true },
  email: String,
  tags: [{ type: String }],
  customFields: mongoose.Schema.Types.Mixed,
  notes: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  lastContact: Date,
  isBlocked: { type: Boolean, default: false }
});

ContactSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

module.exports = mongoose.model('Contact', ContactSchema);
