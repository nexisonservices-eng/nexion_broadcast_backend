const express = require('express');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');

const router = express.Router();

const toSafeNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toUsageQuotaRows = ({ usage = {}, limits = {} }) => {
  const usageObject = usage && typeof usage === 'object' ? usage : {};
  const limitsObject = limits && typeof limits === 'object' ? limits : {};
  const keys = Array.from(new Set([...Object.keys(usageObject), ...Object.keys(limitsObject)]));

  return keys
    .map((key) => {
      const used = toSafeNumber(usageObject[key], 0);
      const rawLimit = limitsObject[key];
      const hasLimit = rawLimit !== undefined && rawLimit !== null && String(rawLimit).trim() !== '';
      const limit = hasLimit ? toSafeNumber(rawLimit, 0) : null;
      const remaining = limit === null ? null : Math.max(limit - used, 0);
      const percentUsed = limit && limit > 0 ? Math.min(Math.round((used / limit) * 10000) / 100, 100) : null;

      return {
        key,
        used,
        limit,
        remaining,
        percentUsed
      };
    })
    .sort((left, right) => String(left.key).localeCompare(String(right.key)));
};

router.use(auth);
router.use(
  requireTenantPolicy({
    auditEvent: 'usage_policy'
  })
);

router.get('/quota', async (req, res) => {
  try {
    const planCode = String(req?.user?.planCode || '').trim().toLowerCase() || 'unknown';
    const subscriptionStatus = String(req?.user?.subscriptionStatus || '').trim().toLowerCase() || 'unknown';
    const featureFlags =
      req?.planFeatures && typeof req.planFeatures === 'object'
        ? req.planFeatures
        : req?.user?.featureFlags && typeof req.user.featureFlags === 'object'
          ? req.user.featureFlags
          : {};
    const trialUsage =
      req?.user?.trialUsage && typeof req.user.trialUsage === 'object' ? req.user.trialUsage : {};
    const trialLimits =
      req?.user?.trialLimits && typeof req.user.trialLimits === 'object' ? req.user.trialLimits : {};

    return res.json({
      success: true,
      data: {
        tenant: {
          companyId: String(req.companyId || '')
        },
        plan: {
          code: planCode,
          subscriptionStatus,
          isTrial: planCode === 'trial'
        },
        features: featureFlags,
        quota: toUsageQuotaRows({
          usage: trialUsage,
          limits: trialLimits
        }),
        generatedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to load usage quota'
    });
  }
});

module.exports = router;
