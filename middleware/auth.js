const jwt = require('jsonwebtoken');
const { fetchUserContext } = require('../services/adminAuthService');
const { requireJwtSecret } = require('../utils/securityConfig');

module.exports = (req, res, next) => {
  const run = async () => {
    try {
      const authHeader = req.headers.authorization || '';
      if (!authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ success: false, error: 'Unauthorized: token missing' });
      }

      // Preferred: resolve from admin backend
      try {
        const context = await fetchUserContext(authHeader);
        if (context?.userId || context?.email) {
          req.user = {
            id: context.userId || context.id,
            email: context.email,
            role: context.role,
            username: context.username,
            companyId: context.companyId,
            companyRole: context.companyRole,
            planCode: context.planCode,
            featureFlags: context.featureFlags,
            subscriptionStatus: context.subscriptionStatus,
            trialUsage: context.trialUsage || {},
            trialLimits: context.trialLimits || {},
            documentStatus: context.documentStatus || "not_required",
            workspaceAccessState: context.workspaceAccessState || "",
            canPerformActions: typeof context.canPerformActions === "boolean" ? context.canPerformActions : true,
            canViewAnalytics: typeof context.canViewAnalytics === "boolean" ? context.canViewAnalytics : true
          };
          req.companyId = context.companyId || null;
          req.planFeatures = context.featureFlags || {};
          return next();
        }
      } catch (error) {
        // fallback to local JWT parsing
      }

      const token = authHeader.split(' ')[1];
      const decoded = jwt.verify(token, requireJwtSecret('auth token verification'));
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
