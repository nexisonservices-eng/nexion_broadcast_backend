const mongoose = require('mongoose');

const KeywordRuleSchema = new mongoose.Schema(
  {
    keyword: { type: String, required: true, trim: true },
    score: { type: Number, default: 1 }
  },
  { _id: false }
);

const LeadScoringAutomationSchema = new mongoose.Schema(
  {
    isEnabled: { type: Boolean, default: false },
    stageThreshold: { type: Number, default: 45 },
    stageOnThreshold: {
      type: String,
      enum: ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'won', 'lost'],
      default: 'qualified'
    },
    taskThreshold: { type: Number, default: 60 },
    taskTitle: { type: String, default: 'High intent lead follow-up', trim: true },
    recommendedTemplate: { type: String, default: '', trim: true },
    ownerNotification: { type: Boolean, default: true }
  },
  { _id: false }
);

const LeadScoringConfigSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
    readScore: { type: Number, default: 2 },
    replyScore: { type: Number, default: 5 },
    keywordRules: { type: [KeywordRuleSchema], default: [] },
    whatsappOptInScope: { type: String, default: 'marketing', trim: true },
    whatsappOptInKeywordRules: { type: [KeywordRuleSchema], default: [] },
    isEnabled: { type: Boolean, default: true },
    automation: { type: LeadScoringAutomationSchema, default: () => ({}) },
    updatedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' }
  },
  {
    timestamps: true
  }
);

LeadScoringConfigSchema.index({ companyId: 1, userId: 1 }, { unique: true });

module.exports = mongoose.model('LeadScoringConfig', LeadScoringConfigSchema);
