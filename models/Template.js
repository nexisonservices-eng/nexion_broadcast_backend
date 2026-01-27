const mongoose = require('mongoose');

const TemplateSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true },
  type: { 
    type: String, 
    enum: ['official', 'custom'], 
    default: 'official',
    required: true 
  },
  category: { type: String, default: 'general' },
  language: { type: String, default: 'en_US' },
  content: {
    header: {
      type: { type: String, enum: ['text', 'image', 'video', 'document'] },
      text: String,
      mediaUrl: String
    },
    body: { type: String, required: true },
    footer: String,
    buttons: [{
      type: { type: String, enum: ['quick_reply', 'url', 'phone_number'] },
      text: String,
      url: String,
      phoneNumber: String
    }]
  },
  variables: [{ 
    name: String,
    example: String,
    required: { type: Boolean, default: false }
  }],
  status: { 
    type: String, 
    enum: ['draft', 'pending', 'approved', 'rejected'], 
    default: 'draft' 
  },
  whatsappTemplateId: String,
  isActive: { type: Boolean, default: true },
  usageCount: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  createdBy: String,
  createdById: { type: mongoose.Schema.Types.ObjectId, ref: 'User' }
});

TemplateSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

TemplateSchema.index({ status: 1, isActive: 1 });

module.exports = mongoose.model('Template', TemplateSchema);
