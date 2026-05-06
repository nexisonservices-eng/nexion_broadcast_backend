const jwt = require('jsonwebtoken');
const { fetchUserContext } = require('../services/adminAuthService');
const { requireJwtSecret } = require('../utils/securityConfig');
const { normalizeRole } = require('../utils/accessControl');

const normalizeIdLike = (value) => {
  const normalized = String(value || '').trim();
  if (!normalized || ['undefined', 'null', 'none'].includes(normalized.toLowerCase())) {
    return '';
  }
  return normalized;
};

module.exports = (req, res, next) => {
  const run = async () => {
    try {
      const authHeader = req.headers.authorization || '';
      if (!authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ success: false, error: 'Unauthorized: token missing' });
      }

      const token = authHeader.split(' ')[1];
      let decodedFallback = null;
      try {
        decodedFallback = jwt.verify(token, requireJwtSecret('auth token verification'));
      } catch (error) {
        decodedFallback = null;
      }

      // Preferred: resolve from admin backend
      try {
        const context = await fetchUserContext(authHeader);
        if (context?.userId || context?.email) {
          const fallback = decodedFallback || {};
          const contextUserId = normalizeIdLike(context.userId || context.id);
          const fallbackUserId = normalizeIdLike(fallback.userId || fallback.id);
          const contextCompanyId = normalizeIdLike(context.companyId);
          const fallbackCompanyId = normalizeIdLike(fallback.companyId);
          req.user = {
            id: contextUserId || fallbackUserId || '',
            email: String(context.email || fallback.email || '').trim(),
            role: context.role || fallback.role,
            username: context.username || fallback.username,
            companyId: contextCompanyId || fallbackCompanyId || null,
            companyRole: context.companyRole || fallback.companyRole,
            planCode: context.planCode || fallback.planCode,
            featureFlags: context.featureFlags || fallback.featureFlags,
            subscriptionStatus: context.subscriptionStatus || fallback.subscriptionStatus,
            trialUsage: context.trialUsage || fallback.trialUsage || {},
            trialLimits: context.trialLimits || fallback.trialLimits || {},
            documentStatus: context.documentStatus || fallback.documentStatus || 'not_required',
            workspaceAccessState: context.workspaceAccessState || fallback.workspaceAccessState || '',
            canPerformActions:
              typeof context.canPerformActions === 'boolean'
                ? context.canPerformActions
                : typeof fallback.canPerformActions === 'boolean'
                  ? fallback.canPerformActions
                  : true,
            canViewAnalytics:
              typeof context.canViewAnalytics === 'boolean'
                ? context.canViewAnalytics
                : typeof fallback.canViewAnalytics === 'boolean'
                ? fallback.canViewAnalytics
                : true
          };
          req.companyId = contextCompanyId || fallbackCompanyId || null;
          req.planFeatures = context.featureFlags || fallback.featureFlags || {};
          req.user.normalizedRole = normalizeRole(req.user.companyRole || req.user.role);
          req.authContext = {
            tenant: req.companyId || null,
            role: req.user.normalizedRole,
            feature: req.planFeatures || {}
          };
          if (req.user.id || req.user.email) {
            return next();
          }
        }
      } catch (error) {
        // fallback to local JWT parsing
      }

      const decoded = decodedFallback || jwt.verify(token, requireJwtSecret('auth token verification'));
      req.user = {
        id: decoded.userId || decoded.id,
        email: decoded.email,
        role: decoded.role,
        username: decoded.username,
        companyId: decoded.companyId,
        companyRole: decoded.companyRole,
        planCode: decoded.planCode,
        featureFlags: decoded.featureFlags,
        subscriptionStatus: decoded.subscriptionStatus,
        trialUsage: decoded.trialUsage || {},
        trialLimits: decoded.trialLimits || {},
        documentStatus: decoded.documentStatus || "not_required",
        workspaceAccessState: decoded.workspaceAccessState || "",
        canPerformActions: typeof decoded.canPerformActions === "boolean" ? decoded.canPerformActions : true,
        canViewAnalytics: typeof decoded.canViewAnalytics === "boolean" ? decoded.canViewAnalytics : true
      };
      req.companyId = decoded.companyId || null;
      req.planFeatures = decoded.featureFlags || {};
      req.user.normalizedRole = normalizeRole(req.user.companyRole || req.user.role);
      req.authContext = {
        tenant: req.companyId || null,
        role: req.user.normalizedRole,
        feature: req.planFeatures || {}
      };

      if (!req.user.id && !req.user.email) {
        return res.status(401).json({ success: false, error: 'Unauthorized: invalid token payload' });
      }
      next();
    } catch (error) {
      if (String(error?.message || '').includes('JWT_SECRET is required')) {
        return res.status(500).json({
          success: false,
          error: 'Server auth configuration error. Please set JWT_SECRET.'
        });
      }
      return res.status(401).json({ success: false, error: 'Unauthorized: invalid token' });
    }
  };

  run();
};
