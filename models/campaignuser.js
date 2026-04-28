const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const { normalizeRole } = require('../utils/accessControl');

const campaignUserSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: [true, 'Name is required'],
      trim: true,
      maxlength: [50, 'Name cannot exceed 50 characters']
    },
    email: {
      type: String,
      required: [true, 'Email is required'],
      unique: true,
      lowercase: true,
      trim: true,
      match: [
        /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/,
        'Please provide a valid email'
      ]
    },
    password: {
      type: String,
      required: [true, 'Password is required'],
      minlength: [6, 'Password must be at least 6 characters'],
      select: false
    },
    role: {
      type: String,
      enum: ['superadmin', 'admin', 'manager', 'agent'],
      default: 'agent'
    },
    avatar: {
      type: String,
      default: null
    },
    isActive: {
      type: Boolean,
      default: true
    },
    lastLogin: {
      type: Date
    },
    metaAccessToken: {
      type: String,
      select: false
    },
    metaAdAccountId: {
      type: String
    }
  },
  {
    timestamps: true
  }
);

campaignUserSchema.pre('validate', function preValidate(next) {
  // Keep legacy incoming values compatible while enforcing canonical role storage.
  this.role = normalizeRole(this.role);
  next();
});

campaignUserSchema.pre('save', async function preSave(next) {
  if (!this.isModified('password')) {
    return next();
  }

  try {
    const salt = await bcrypt.genSalt(10);
    this.password = await bcrypt.hash(this.password, salt);
    next();
  } catch (error) {
    next(error);
  }
});

campaignUserSchema.methods.comparePassword = async function comparePassword(candidatePassword) {
  return bcrypt.compare(candidatePassword, this.password);
};

campaignUserSchema.methods.generateAuthToken = function generateAuthToken() {
  const jwt = require('jsonwebtoken');
  return jwt.sign(
    { id: this._id, email: this.email, role: normalizeRole(this.role) },
    process.env.JWT_SECRET,
    { expiresIn: process.env.JWT_EXPIRE }
  );
};

module.exports = mongoose.models.CampaignUser || mongoose.model('CampaignUser', campaignUserSchema);
