const mongoose = require('mongoose');

const KeywordRuleSchema = new mongoose.Schema(
  {
    keyword: { type: String, required: true, trim: true },
    score: { type: Number, default: 1 }
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
    isEnabled: { type: Boolean, default: true },
    updatedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' }
  },
  {
    timestamps: true
  }
);

LeadScoringConfigSchema.index({ companyId: 1, userId: 1 }, { unique: true });

module.exports = mongoose.model('LeadScoringConfig', LeadScoringConfigSchema);
