const express = require('express');
const { cleanupUserDelete } = require('../services/userDeleteCleanupService');
const {
  runDataRetentionMaintenance,
  archiveOldMessages,
  archiveOldBroadcastDispatches
} = require('../services/dataRetentionService');

const router = express.Router();

const requireInternalApiKey = (req, res, next) => {
  const expected = String(process.env.ADMIN_INTERNAL_API_KEY || process.env.INTERNAL_API_KEY || '').trim();
  const provided = String(req.headers['x-internal-api-key'] || '').trim();
  if (!expected) return res.status(500).json({ success: false, error: 'Internal API key is not configured' });
  if (!provided || provided !== expected) return res.status(401).json({ success: false, error: 'Unauthorized internal request' });
  return next();
};

router.post('/user-delete', requireInternalApiKey, async (req, res) => {
  try {
    const result = await cleanupUserDelete(req.body || {});
    return res.json({ success: true, data: result });
  } catch (error) {
    return res.status(Number(error?.status || 500)).json({
      success: false,
      error: error?.message || 'User cleanup failed'
    });
  }
});

router.post('/data-retention', requireInternalApiKey, async (_req, res) => {
  try {
    const result = await runDataRetentionMaintenance();
    return res.json({ success: true, data: result });
  } catch (error) {
    return res.status(Number(error?.status || 500)).json({
      success: false,
      error: error?.message || 'Data retention failed'
    });
  }
});

router.post('/messages/archive', requireInternalApiKey, async (_req, res) => {
  try {
    const result = await archiveOldMessages();
    return res.json({ success: true, data: result });
  } catch (error) {
    return res.status(Number(error?.status || 500)).json({
      success: false,
      error: error?.message || 'Message archive failed'
    });
  }
});

router.post('/broadcast-dispatch/archive', requireInternalApiKey, async (_req, res) => {
  try {
    const result = await archiveOldBroadcastDispatches();
    return res.json({ success: true, data: result });
  } catch (error) {
    return res.status(Number(error?.status || 500)).json({
      success: false,
      error: error?.message || 'Broadcast dispatch archive failed'
    });
  }
});

module.exports = router;
