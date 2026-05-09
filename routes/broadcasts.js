const express = require('express');
const router = express.Router();
const broadcastController = require('../controllers/broadcastController');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const requirePlanFeature = require('../middleware/planGuard');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');
const { normalizeRole } = require('../utils/accessControl');

router.use(auth);
router.use(
  requireTenantPolicy({
    requiredFeatures: ['broadcastDashboard', 'broadcastMessaging', 'teamInbox'],
    auditEvent: 'broadcast_policy'
  })
);

const ADMIN_INTERNAL_API_KEY = String(process.env.ADMIN_INTERNAL_API_KEY || '').trim();

const requireSchedulerTriggerAccess = (req, res, next) => {
  const internalKey = String(req.headers['x-internal-api-key'] || '').trim();
  const role = normalizeRole(req.user?.normalizedRole || req.user?.companyRole || req.user?.role);
  const isSuperAdmin = role === 'superadmin';
  const isInternalCall =
    ADMIN_INTERNAL_API_KEY && internalKey && internalKey === ADMIN_INTERNAL_API_KEY;

  if (isSuperAdmin || isInternalCall) {
    return next();
  }

  return res.status(403).json({
    success: false,
    error: 'Forbidden: scheduler trigger requires internal key or super admin role'
  });
};

router.get('/', (req, res) => broadcastController.getBroadcasts(req, res));
router.get('/queue/metrics', (req, res) => broadcastController.getQueueMetrics(req, res));
router.get('/analytics/reliability', (req, res) => broadcastController.getReliabilitySummary(req, res));
router.get('/:id', (req, res) => broadcastController.getBroadcastById(req, res));
router.post('/', requireWhatsAppCredentials, (req, res) => broadcastController.createBroadcast(req, res));
router.post('/:id/send', requirePlanFeature('broadcastMessaging'), requireWhatsAppCredentials, (req, res) => broadcastController.sendBroadcast(req, res));
router.post('/:id/pause', (req, res) => broadcastController.pauseBroadcast(req, res));
router.post('/:id/resume', (req, res) => broadcastController.resumeBroadcast(req, res));
router.post('/:id/cancel', (req, res) => broadcastController.cancelScheduledBroadcast(req, res));
router.post(
  '/:id/retry-failed',
  requirePlanFeature('broadcastMessaging'),
  requireWhatsAppCredentials,
  (req, res) => broadcastController.retryFailedRecipients(req, res)
);
router.post(
  '/check-scheduled',
  requireSchedulerTriggerAccess,
  (req, res) => broadcastController.checkScheduledBroadcasts(req, res)
);
router.post('/:id/sync-stats', (req, res) => broadcastController.syncBroadcastStats(req, res));
router.delete('/:id', (req, res) => broadcastController.deleteBroadcast(req, res));

module.exports = router;
