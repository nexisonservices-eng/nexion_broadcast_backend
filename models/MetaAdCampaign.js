const mongoose = require('mongoose');

const metaAdCampaignSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      index: true
    },
    campaignName: {
      type: String,
      required: true,
      trim: true
    },
    objective: {
      type: String,
      default: 'OUTCOME_LEADS'
    },
    status: {
      type: String,
      default: 'DRAFT'
    },
    configuredPageId: {
      type: String,
      default: ''
    },
    configuredInstagramActorId: {
      type: String,
      default: ''
    },
    whatsappNumber: {
      type: String,
      default: ''
    },
    budget: {
      dailyBudget: { type: Number, default: 0 },
      lifetimeBudget: { type: Number, default: 0 },
      currency: { type: String, default: 'INR' }
    },
    targeting: {
      countries: [{ type: String }],
      ageMin: { type: Number, default: 21 },
      ageMax: { type: Number, default: 45 },
      genders: [{ type: Number }],
      interests: [
        {
          id: String,
          name: String
        }
      ],
      customAudienceIds: [{ type: String }]
    },
    creative: {
      primaryText: { type: String, default: '' },
      headline: { type: String, default: '' },
      description: { type: String, default: '' },
      callToAction: { type: String, default: 'WHATSAPP_MESSAGE' },
      mediaType: { type: String, default: 'image' },
      mediaUrl: { type: String, default: '' },
      mediaHash: { type: String, default: '' }
    },
    placement: {
      publisherPlatforms: [{ type: String }],
      facebookPositions: [{ type: String }],
      instagramPositions: [{ type: String }]
    },
    schedule: {
      startTime: { type: Date, default: null },
      endTime: { type: Date, default: null }
    },
    meta: {
      adAccountId: { type: String, default: '' },
      campaignId: { type: String, default: '' },
      adSetId: { type: String, default: '' },
      creativeId: { type: String, default: '' },
      adId: { type: String, default: '' },
      previewShareableLink: { type: String, default: '' }
    },
    analytics: {
      impressions: { type: Number, default: 0 },
      reach: { type: Number, default: 0 },
      clicks: { type: Number, default: 0 },
      leads: { type: Number, default: 0 },
      spend: { type: Number, default: 0 },
      ctr: { type: Number, default: 0 },
      cpc: { type: Number, default: 0 },
      cpl: { type: Number, default: 0 },
      lastSyncedAt: { type: Date, default: null }
    },
    accounting: {
      reservedBudget: { type: Number, default: 0 },
      totalDebited: { type: Number, default: 0 },
      reconciledSpend: { type: Number, default: 0 },
      lastReconciledAt: { type: Date, default: null }
    },
    setupSnapshot: {
      connected: { type: Boolean, default: false },
      selectedAdAccountId: { type: String, default: '' },
      selectedPageId: { type: String, default: '' },
      linkedWhatsappNumber: { type: String, default: '' }
    },
    apiMode: {
      type: String,
      default: 'mock'
    },
    lastError: {
      type: String,
      default: ''
    },
    wizard: {
      currentStep: { type: Number, default: 1 },
      completedSteps: [{ type: Number }],
      lastSavedAt: { type: Date, default: null }
    }
  },
  {
    timestamps: true
  }
);

module.exports = mongoose.model('MetaAdCampaign', metaAdCampaignSchema);
