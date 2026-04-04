const mongoose = require('mongoose');

const googleCalendarConnectionSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      index: true
    },
    companyId: {
      type: String,
      default: '',
      index: true
    },
    email: {
      type: String,
      default: ''
    },
    name: {
      type: String,
      default: ''
    },
    picture: {
      type: String,
      default: ''
    },
    accessToken: {
      type: String,
      default: ''
    },
    refreshToken: {
      type: String,
      default: ''
    },
    tokenType: {
      type: String,
      default: 'Bearer'
    },
    scope: [{ type: String }],
    expiresAt: {
      type: Date,
      default: null
    },
    connectedAt: {
      type: Date,
      default: Date.now
    },
    lastSyncedAt: {
      type: Date,
      default: Date.now
    },
    lastError: {
      type: String,
      default: ''
    }
  },
  {
    timestamps: true
  }
);

googleCalendarConnectionSchema.index({ userId: 1, companyId: 1 }, { unique: true });

module.exports = mongoose.model('GoogleCalendarConnection', googleCalendarConnectionSchema);
