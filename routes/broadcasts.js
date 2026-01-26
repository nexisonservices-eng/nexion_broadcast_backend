const express = require('express');
const router = express.Router();
const broadcastController = require('../controllers/broadcastController');

router.get('/', (req, res) => broadcastController.getBroadcasts(req, res));
router.get('/:id', (req, res) => broadcastController.getBroadcastById(req, res));
router.post('/', (req, res) => broadcastController.createBroadcast(req, res));
router.post('/:id/send', (req, res) => broadcastController.sendBroadcast(req, res));
router.post('/check-scheduled', (req, res) => broadcastController.checkScheduledBroadcasts(req, res));
router.post('/:id/sync-stats', (req, res) => broadcastController.syncBroadcastStats(req, res));
router.delete('/:id', (req, res) => broadcastController.deleteBroadcast(req, res));

module.exports = router;
