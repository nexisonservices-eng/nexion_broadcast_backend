const mongoose = require('mongoose');

const metaAdsConnectionSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      unique: true,
      index: true
    },
    platformUserId: {
      type: String,
      default: ''
    },
    name: {
      type: String,
      default: ''
    },
    accessToken: {
      type: String,
      required: true
    },
    tokenType: {
      type: String,
      default: 'bearer'
    },
    scopes: [{ type: String }],
    selectedAdAccountId: {
      type: String,
      default: ''
    },
    selectedPageId: {
      type: String,
      default: ''
    },
    selectedWhatsappNumber: {
      type: String,
      default: ''
    },
    connectedAt: {
      type: Date,
      default: Date.now
    },
    lastValidatedAt: {
      type: Date,
      default: Date.now
    }
  },
  {
    timestamps: true
  }
);

module.exports = mongoose.model('MetaAdsConnection', metaAdsConnectionSchema);
