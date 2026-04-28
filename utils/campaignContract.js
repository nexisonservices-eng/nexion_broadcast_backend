const toCleanString = (value) => String(value || '').trim();

const toSafeNumber = (value, fallback = null) => {
  if (value === '' || value === null || value === undefined) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toSafeDate = (value, fallback = null) => {
  if (!value) return fallback;
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? fallback : parsed;
};

const parseObjectLike = (value) => {
  if (!value) return {};
  if (typeof value === 'object' && !Array.isArray(value)) return value;
  if (typeof value !== 'string') return {};
  const raw = value.trim();
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
};

const normalizeCampaignContractPayload = (payload = {}) => {
  const source = payload && typeof payload === 'object' ? payload : {};
  const audience = parseObjectLike(source.audience);
  const deliveryPolicy = parseObjectLike(source.deliveryPolicy);
  const retryPolicy = parseObjectLike(source.retryPolicy);
  const compliancePolicy = parseObjectLike(source.compliancePolicy);
  const analytics = parseObjectLike(source.analytics);

  const merged = {
    ...source
  };

  if (audience.name !== undefined && !merged.name) merged.name = audience.name;
  if (audience.platform !== undefined && !merged.platform) merged.platform = audience.platform;
  if (audience.objective !== undefined && !merged.objective) merged.objective = audience.objective;
  if (audience.targeting !== undefined && !merged.targeting) merged.targeting = audience.targeting;
  if (audience.ageMin !== undefined && merged.ageMin === undefined) merged.ageMin = audience.ageMin;
  if (audience.ageMax !== undefined && merged.ageMax === undefined) merged.ageMax = audience.ageMax;
  if (audience.gender !== undefined && merged.gender === undefined) merged.gender = audience.gender;
  if (audience.interests !== undefined && merged.interests === undefined) merged.interests = audience.interests;
  if (audience.behaviors !== undefined && merged.behaviors === undefined) merged.behaviors = audience.behaviors;

  if (deliveryPolicy.dailyBudget !== undefined && merged.dailyBudget === undefined) {
    merged.dailyBudget = deliveryPolicy.dailyBudget;
  }
  if (deliveryPolicy.lifetimeBudget !== undefined && merged.lifetimeBudget === undefined) {
    merged.lifetimeBudget = deliveryPolicy.lifetimeBudget;
  }
  if (deliveryPolicy.startDate !== undefined && merged.startDate === undefined) {
    merged.startDate = deliveryPolicy.startDate;
  }
  if (deliveryPolicy.endDate !== undefined && merged.endDate === undefined) {
    merged.endDate = deliveryPolicy.endDate;
  }
  if (deliveryPolicy.status !== undefined && merged.status === undefined) {
    merged.status = deliveryPolicy.status;
  }

  if (analytics.spent !== undefined && merged.spent === undefined) merged.spent = analytics.spent;
  if (analytics.impressions !== undefined && merged.impressions === undefined) {
    merged.impressions = analytics.impressions;
  }
  if (analytics.clicks !== undefined && merged.clicks === undefined) merged.clicks = analytics.clicks;
  if (analytics.revenue !== undefined && merged.revenue === undefined) merged.revenue = analytics.revenue;
  if (analytics.ctr !== undefined && merged.ctr === undefined) merged.ctr = analytics.ctr;
  if (analytics.cpc !== undefined && merged.cpc === undefined) merged.cpc = analytics.cpc;

  merged.audience = audience;
  merged.deliveryPolicy = deliveryPolicy;
  merged.retryPolicy = retryPolicy;
  merged.compliancePolicy = compliancePolicy;
  merged.analytics = analytics;

  return merged;
};

const shapeCampaignContract = (campaign = {}) => {
  const source = campaign && typeof campaign === 'object' ? campaign : {};
  const fallbackBudgetType = source.dailyBudget ? 'daily' : 'lifetime';
  const derivedBudgetType = toCleanString(source?.deliveryPolicy?.budgetType || source?.budgetType).toLowerCase();
  const budgetType = derivedBudgetType === 'daily' || derivedBudgetType === 'lifetime'
    ? derivedBudgetType
    : fallbackBudgetType;

  return {
    audience: {
      name: toCleanString(source.name),
      platform: toCleanString(source.platform || 'both') || 'both',
      objective: toCleanString(source.objective || 'awareness') || 'awareness',
      targeting: toCleanString(source.targeting),
      ageMin: toSafeNumber(source.ageMin, 18),
      ageMax: toSafeNumber(source.ageMax, 65),
      gender: toCleanString(source.gender || 'all') || 'all',
      interests: toCleanString(source.interests),
      behaviors: toCleanString(source.behaviors)
    },
    deliveryPolicy: {
      status: toCleanString(source.status || 'draft') || 'draft',
      lifecycleStatus: toCleanString(source.lifecycleStatus || 'draft') || 'draft',
      deliveryStatus: toCleanString(source.deliveryStatus || 'not_published') || 'not_published',
      budgetType,
      dailyBudget: toSafeNumber(source.dailyBudget, null),
      lifetimeBudget: toSafeNumber(source.lifetimeBudget, null),
      startDate: toSafeDate(source.startDate, null),
      endDate: toSafeDate(source.endDate, null),
      quietHours:
        source.deliveryPolicy && typeof source.deliveryPolicy === 'object'
          ? source.deliveryPolicy.quietHours || null
          : null
    },
    retryPolicy: {
      enabled: Boolean(source?.retryPolicy?.enabled),
      maxAttempts: toSafeNumber(source?.retryPolicy?.maxAttempts, 3),
      backoffSeconds: toSafeNumber(source?.retryPolicy?.backoffSeconds, 30),
      retryOnFailureCodes: Array.isArray(source?.retryPolicy?.retryOnFailureCodes)
        ? source.retryPolicy.retryOnFailureCodes
        : []
    },
    compliancePolicy: {
      respectOptOut: source?.compliancePolicy?.respectOptOut !== false,
      suppressionListPhones: Array.isArray(source?.compliancePolicy?.suppressionListPhones)
        ? source.compliancePolicy.suppressionListPhones
        : [],
      legalBasis: toCleanString(source?.compliancePolicy?.legalBasis),
      retentionDays: toSafeNumber(source?.compliancePolicy?.retentionDays, null)
    },
    analytics: {
      spent: toSafeNumber(source.spent, 0),
      impressions: toSafeNumber(source.impressions, 0),
      clicks: toSafeNumber(source.clicks, 0),
      ctr: toSafeNumber(source.ctr, 0),
      cpc: toSafeNumber(source.cpc, 0),
      revenue: toSafeNumber(source.revenue, 0),
      roas: toSafeNumber(source.roas, 0),
      roi: toSafeNumber(source.roi, 0)
    }
  };
};

module.exports = {
  normalizeCampaignContractPayload,
  shapeCampaignContract
};
