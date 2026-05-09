const mongoose = require('mongoose');



const ContactSchema = new mongoose.Schema({

  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, default: null },

  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true, default: null },

  stage: { type: String, default: 'new', index: true },

  status: { type: String, default: 'new', index: true },

  lastStageChangedAt: { type: Date, default: null },

  name: { type: String, default: '' },

  phone: { type: String, required: true, index: true },

  email: String,

  tags: [{ type: String }],

  customFields: mongoose.Schema.Types.Mixed,

  notes: String,

  createdAt: { type: Date, default: Date.now },

  updatedAt: { type: Date, default: Date.now },

  lastContact: Date,

  isBlocked: { type: Boolean, default: false }

}, {
  strict: false
});



ContactSchema.pre('save', function(next) {

  this.updatedAt = Date.now();

  next();

});

ContactSchema.index({ companyId: 1, userId: 1, phone: 1 });
ContactSchema.index({ companyId: 1, userId: 1, lastContact: -1, createdAt: -1, _id: -1 });
ContactSchema.index({ companyId: 1, userId: 1, lastInboundMessageAt: -1, createdAt: -1, _id: -1 });
ContactSchema.index({ companyId: 1, userId: 1, whatsappOptInStatus: 1, sourceType: 1, lastContact: -1, createdAt: -1 });



module.exports = mongoose.model('Contact', ContactSchema);

