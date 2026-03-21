const express = require('express');
const auth = require('../middleware/auth');
const metaAdsService = require('../services/metaAdsService');

const router = express.Router();

router.use(auth);

router.get('/filters', async (req, res) => {
  try {
    const data = await metaAdsService.fetchInsightsFilters({ userId: req.user.id });
    res.json(data);
  } catch (error) {
    console.error('Error fetching insight filters:', error);
    res.status(error.status || 500).json({
      success: false,
      message: error.message || 'Error fetching insight filters',
      details: error.details || null
    });
  }
});

router.get('/', async (req, res) => {
  try {
    const data = await metaAdsService.fetchInsightsDashboard({
      userId: req.user.id,
      range: req.query.range || '30d',
      campaignId: req.query.campaignId || '',
      adSetId: req.query.adSetId || ''
    });

    res.json(data);
  } catch (error) {
    console.error('Error fetching insights dashboard:', error);
    res.status(error.status || 500).json({
      success: false,
      message: error.message || 'Error fetching insights dashboard',
      details: error.details || null
    });
  }
});

module.exports = router;

