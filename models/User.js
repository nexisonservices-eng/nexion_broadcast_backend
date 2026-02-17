const mongoose = require('mongoose');

const UserSchema = new mongoose.Schema({
  name: { type: String, required: true },
  email: { type: String, unique: true, required: true },
  password: { type: String, required: true },
  role: { 
    type: String, 
    enum: ['super_admin', 'admin', 'agent', 'manager'], 
    default: 'agent' 
  },
  // WhatsApp credentials for admin users
  whatsappCredentials: {
    phoneNumberId: String,
    accessToken: String,
    businessAccountId: String,
    webhookVerifyToken: { type: String, default: () => 'verify_token_' + Math.random().toString(36).substring(2, 15) }
  },
  // Parent admin for non-super admin users
  parentAdminId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  avatar: String,
  isActive: { type: Boolean, default: true },
  lastSeen: Date,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

UserSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

module.exports = mongoose.model('User', UserSchema);
