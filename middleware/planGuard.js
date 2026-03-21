const resolveRequestedCount = (req, featureKey) => {
  if (featureKey === 'broadcastMessaging') {
    if (Array.isArray(req.body?.recipients) && req.body.recipients.length > 0) {
      return req.body.recipients.length;
    }
    return Number(req.broadcastMessageCount || 1);
  }
  return 1;
};

module.exports = (featureKey) => (req, res, next) => {
  const flags = req.planFeatures || {};
  if (!flags[featureKey]) {
    return res.status(403).json({ success: false, error: 'Feature not enabled for plan' });
  }

  if (req.user?.canPerformActions === false) {
    return res.status(403).json({ success: false, error: 'Workspace is in read-only mode. Actions are blocked until activation.' });
  }

  if (String(req.user?.planCode || '').toLowerCase() === 'trial') {
    const usage = req.user?.trialUsage || {};
    const limits = req.user?.trialLimits || {};
    const requestedCount = resolveRequestedCount(req, featureKey);
    const usedMessages = Number(usage.whatsappMessages || 0);
    const messageLimit = Number(limits.whatsappMessages || 50);
    if (featureKey === 'broadcastMessaging' && usedMessages + requestedCount > messageLimit) {
      return res.status(403).json({ success: false, error: 'Trial message limit reached. Upgrade to continue.' });
    }
  }

  return next();
};
