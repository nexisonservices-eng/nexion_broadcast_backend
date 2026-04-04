const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');
const LeadScoringConfig = require('../models/LeadScoringConfig');

const DEFAULT_LEAD_SCORING_SETTINGS = Object.freeze({
  readScore: 2,
  replyScore: 5,
  keywordRules: [],
  isEnabled: true
});

const toFiniteNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toNonNegativeNumber = (value, fallback = 0) => {
  const parsed = toFiniteNumber(value, fallback);
  return parsed < 0 ? fallback : parsed;
};

const normalizeKeywordRules = (rules = []) => {
  const normalized = [];
  const seen = new Set();

  const source = Array.isArray(rules) ? rules : [];
  source.forEach((item) => {
    const rawKeyword =
      typeof item === 'string'
        ? item
        : item && typeof item === 'object'
          ? item.keyword
          : '';

    const keyword = String(rawKeyword || '').trim().toLowerCase();
    if (!keyword || seen.has(keyword)) return;
    seen.add(keyword);

    const score = toNonNegativeNumber(
      typeof item === 'object' && item !== null ? item.score : 1,
      1
    );

    normalized.push({
      keyword,
      score
    });
  });

  return normalized;
};

const normalizeLeadScoringPayload = (payload = {}) => {
  const updates = {};

  if (Object.prototype.hasOwnProperty.call(payload, 'readScore')) {
    updates.readScore = toNonNegativeNumber(payload.readScore, DEFAULT_LEAD_SCORING_SETTINGS.readScore);
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'replyScore')) {
    updates.replyScore = toNonNegativeNumber(payload.replyScore, DEFAULT_LEAD_SCORING_SETTINGS.replyScore);
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'isEnabled')) {
    updates.isEnabled = Boolean(payload.isEnabled);
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'keywordRules')) {
    updates.keywordRules = normalizeKeywordRules(payload.keywordRules);
  } else if (Object.prototype.hasOwnProperty.call(payload, 'keywords')) {
    updates.keywordRules = normalizeKeywordRules(payload.keywords);
  }

  return updates;
};

const toPlainConfig = (config) => ({
  readScore: toNonNegativeNumber(config?.readScore, DEFAULT_LEAD_SCORING_SETTINGS.readScore),
  replyScore: toNonNegativeNumber(config?.replyScore, DEFAULT_LEAD_SCORING_SETTINGS.replyScore),
  keywordRules: normalizeKeywordRules(config?.keywordRules || []),
  isEnabled: config?.isEnabled !== false
});

const getLeadScoringSettings = async ({ userId, companyId }) => {
  if (!userId) {
    return { ...DEFAULT_LEAD_SCORING_SETTINGS };
  }

  const existing = await LeadScoringConfig.findOne({ userId, companyId }).lean();
  if (!existing) {
    return { ...DEFAULT_LEAD_SCORING_SETTINGS };
  }

  return toPlainConfig(existing);
};

const updateLeadScoringSettings = async ({ userId, companyId, updatedBy, payload = {} }) => {
  if (!userId) {
    throw new Error('userId is required to update lead scoring settings');
  }

  const updates = normalizeLeadScoringPayload(payload);
  if (!Object.keys(updates).length) {
    throw new Error('No valid lead scoring fields provided');
  }

  const updated = await LeadScoringConfig.findOneAndUpdate(
    { userId, companyId },
    {
      $set: {
        ...updates,
        updatedBy: updatedBy || userId
      },
      $setOnInsert: {
        userId,
        companyId
      }
    },
    {
      upsert: true,
      new: true,
      setDefaultsOnInsert: true
    }
  ).lean();

  return toPlainConfig(updated);
};

const resolveConversationAndContact = async ({ conversationId, userId, companyId }) => {
  if (!conversationId || !userId) return null;

  const conversation = await Conversation.findOne({
    _id: conversationId,
    userId,
    companyId
  })
    .select('contactId')
    .lean();

  if (!conversation?.contactId) return null;

  return conversation;
};

const applyScoreToContact = async ({
  contactId,
  userId,
  companyId,
  totalDelta = 0,
  breakdown = {}
}) => {
  const safeTotal = toFiniteNumber(totalDelta, 0);
  const readDelta = toFiniteNumber(breakdown.read, 0);
  const replyDelta = toFiniteNumber(breakdown.reply, 0);
  const keywordDelta = toFiniteNumber(breakdown.keyword, 0);

  if (!contactId || !userId || safeTotal === 0) return null;

  return Contact.findOneAndUpdate(
    {
      _id: contactId,
      userId,
      companyId
    },
    {
      $inc: {
        leadScore: safeTotal,
        'leadScoreBreakdown.read': readDelta,
        'leadScoreBreakdown.reply': replyDelta,
        'leadScoreBreakdown.keyword': keywordDelta
      },
      $set: {
        lastLeadScoreAt: new Date()
      }
    },
    {
      new: true
    }
  ).lean();
};

const getKeywordMatches = ({ text = '', keywordRules = [] }) => {
  const normalizedText = String(text || '').toLowerCase();
  if (!normalizedText.trim()) return [];

  return keywordRules.filter((rule) => normalizedText.includes(String(rule.keyword || '').toLowerCase()));
};

const applyReadScoreForMessage = async ({ messageId, userId, companyId }) => {
  if (!messageId || !userId) return null;

  const settings = await getLeadScoringSettings({ userId, companyId });
  if (!settings.isEnabled) return null;

  const readScore = toNonNegativeNumber(settings.readScore, 0);
  if (readScore <= 0) return null;

  const message = await Message.findOneAndUpdate(
    {
      _id: messageId,
      userId,
      companyId,
      sender: 'agent',
      'leadScoring.readScoreApplied': { $ne: true }
    },
    {
      $set: {
        'leadScoring.readScoreApplied': true,
        'leadScoring.readScoreAdded': readScore,
        'leadScoring.lastScoredAt': new Date()
      }
    },
    { new: true }
  ).lean();

  if (!message) return null;

  const conversation = await resolveConversationAndContact({
    conversationId: message.conversationId,
    userId: message.userId,
    companyId: message.companyId
  });
  if (!conversation?.contactId) return null;

  const contact = await applyScoreToContact({
    contactId: conversation.contactId,
    userId: message.userId,
    companyId: message.companyId,
    totalDelta: readScore,
    breakdown: { read: readScore }
  });

  return {
    event: 'read',
    points: readScore,
    contact
  };
};

const applyIncomingMessageScore = async ({ messageId, userId, companyId, text = '' }) => {
  if (!messageId || !userId) return null;

  const settings = await getLeadScoringSettings({ userId, companyId });
  if (!settings.isEnabled) return null;

  const replyScore = toNonNegativeNumber(settings.replyScore, 0);
  const keywordMatches = getKeywordMatches({ text, keywordRules: settings.keywordRules });
  const keywordScore = keywordMatches.reduce(
    (sum, item) => sum + toNonNegativeNumber(item.score, 0),
    0
  );
  const totalDelta = replyScore + keywordScore;

  if (totalDelta <= 0) return null;

  const message = await Message.findOneAndUpdate(
    {
      _id: messageId,
      userId,
      companyId,
      sender: 'contact',
      'leadScoring.replyScoreApplied': { $ne: true },
      'leadScoring.keywordScoreApplied': { $ne: true }
    },
    {
      $set: {
        'leadScoring.replyScoreApplied': true,
        'leadScoring.replyScoreAdded': replyScore,
        'leadScoring.keywordScoreApplied': true,
        'leadScoring.keywordScoreAdded': keywordScore,
        'leadScoring.keywordMatches': keywordMatches,
        'leadScoring.lastScoredAt': new Date()
      }
    },
    { new: true }
  ).lean();

  if (!message) return null;

  const conversation = await resolveConversationAndContact({
    conversationId: message.conversationId,
    userId: message.userId,
    companyId: message.companyId
  });
  if (!conversation?.contactId) return null;

  const contact = await applyScoreToContact({
    contactId: conversation.contactId,
    userId: message.userId,
    companyId: message.companyId,
    totalDelta,
    breakdown: {
      reply: replyScore,
      keyword: keywordScore
    }
  });

  return {
    event: 'reply_and_keyword',
    points: totalDelta,
    detail: {
      replyScore,
      keywordScore,
      keywordMatches
    },
    contact
  };
};

module.exports = {
  DEFAULT_LEAD_SCORING_SETTINGS,
  getLeadScoringSettings,
  updateLeadScoringSettings,
  applyReadScoreForMessage,
  applyIncomingMessageScore
};
