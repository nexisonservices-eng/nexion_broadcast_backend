const express = require('express');
const mongoose = require('mongoose');
const auth = require('../middleware/auth');
const Campaign = require('../models/campaign');
const { normalizeRole } = require('../utils/accessControl');
const { fetchSuperAdminCustomers } = require('../services/adminAuthService');

const router = express.Router();
router.use(auth);
router.use((req, res, next) => {
  if (normalizeRole(req.user?.role) !== 'superadmin' && normalizeRole(req.user?.normalizedRole) !== 'superadmin') {
    return res.status(403).json({ success: false, error: 'Super Admin access is required.' });
  }
  return next();
});

const campaignStatus = (campaign = {}) =>
  campaign.deliveryStatus || campaign.lifecycleStatus || campaign.status || 'unknown';

const campaignRow = (campaign = {}) => ({
  id: String(campaign._id || ''),
  name: campaign.name || 'Untitled campaign',
  status: campaignStatus(campaign),
  createdAt: campaign.createdAt || null,
  updatedAt: campaign.updatedAt || null
});

router.get('/customers', async (req, res) => {
  try {
    const users = await fetchSuperAdminCustomers(req.headers.authorization || '');
    const adminsByCompany = new Map();

    users.forEach((user) => {
      const companyId = String(user?.companyId?._id || user?.companyId || '').trim();
      if (!companyId) return;
      const companyRole = String(user?.companyRole || '').trim().toLowerCase();
      if (companyRole !== 'admin') return;
      if (!adminsByCompany.has(companyId)) adminsByCompany.set(companyId, user);
    });

    const companyIds = [...adminsByCompany.keys()];
    const campaigns = companyIds.length
      ? await Campaign.find({ companyId: { $in: companyIds }, status: { $ne: 'archived' } })
        .select('companyId createdAt updatedAt')
        .sort({ updatedAt: -1, createdAt: -1 })
        .lean()
      : [];
    const activityByCompany = new Map();
    campaigns.forEach((campaign) => {
      const companyId = String(campaign.companyId || '');
      const current = activityByCompany.get(companyId) || { count: 0, lastActivityAt: null };
      current.count += 1;
      const activityAt = campaign.updatedAt || campaign.createdAt || null;
      if (activityAt && (!current.lastActivityAt || new Date(activityAt) > new Date(current.lastActivityAt))) {
        current.lastActivityAt = activityAt;
      }
      activityByCompany.set(companyId, current);
    });

    const customers = companyIds.map((companyId) => {
      const user = adminsByCompany.get(companyId) || {};
      const activity = activityByCompany.get(companyId) || { count: 0, lastActivityAt: null };
      return {
        id: companyId,
        name: String(user.companyName || user.username || user.email || 'Customer').trim(),
        email: String(user.email || '').trim(),
        campaignCount: activity.count,
        lastActivityAt: activity.lastActivityAt
      };
    }).sort((a, b) => a.name.localeCompare(b.name));

    return res.json({ success: true, data: customers });
  } catch (error) {
    console.error('[SuperAdmin Customer Targeting] customer list failed:', error.message);
    return res.status(Number(error.status) || 502).json({ success: false, error: error.message });
  }
});

router.get('/customers/:companyId/campaigns', async (req, res) => {
  try {
    const companyId = String(req.params.companyId || '').trim();
    if (!mongoose.Types.ObjectId.isValid(companyId)) {
      return res.status(400).json({ success: false, error: 'Invalid customer ID.' });
    }
    const users = await fetchSuperAdminCustomers(req.headers.authorization || '');
    const customer = users.find((user) =>
      String(user?.companyId?._id || user?.companyId || '') === companyId &&
      String(user?.companyRole || '').toLowerCase() === 'admin'
    );
    if (!customer) return res.status(404).json({ success: false, error: 'Customer not found.' });

    const campaigns = await Campaign.find({ companyId, status: { $ne: 'archived' } })
      .select('name status lifecycleStatus deliveryStatus createdAt updatedAt')
      .sort({ createdAt: -1 })
      .lean();
    return res.json({
      success: true,
      customer: {
        id: companyId,
        name: String(customer.companyName || customer.username || customer.email || 'Customer').trim(),
        email: String(customer.email || '').trim()
      },
      data: campaigns.map(campaignRow)
    });
  } catch (error) {
    console.error('[SuperAdmin Customer Targeting] campaign list failed:', error.message);
    return res.status(Number(error.status) || 502).json({ success: false, error: error.message });
  }
});

router.get('/customers/:companyId/campaigns/:campaignId', async (req, res) => {
  try {
    const { companyId, campaignId } = req.params;
    if (!mongoose.Types.ObjectId.isValid(companyId) || !mongoose.Types.ObjectId.isValid(campaignId)) {
      return res.status(400).json({ success: false, error: 'Invalid customer or campaign ID.' });
    }
    const campaign = await Campaign.findOne({ _id: campaignId, companyId, status: { $ne: 'archived' } }).lean();
    if (!campaign) return res.status(404).json({ success: false, error: 'Campaign not found for this customer.' });

    const users = await fetchSuperAdminCustomers(req.headers.authorization || '');
    const customer = users.find((user) =>
      String(user?.companyId?._id || user?.companyId || '') === String(companyId) &&
      String(user?.companyRole || '').toLowerCase() === 'admin'
    );
    if (!customer) return res.status(404).json({ success: false, error: 'Customer not found.' });

    return res.json({
      success: true,
      customer: {
        id: String(companyId),
        name: String(customer.companyName || customer.username || customer.email || 'Customer').trim(),
        email: String(customer.email || '').trim()
      },
      data: {
        ...campaignRow(campaign),
        targetingSnapshot: campaign.targetingSnapshot || null,
        targeting: campaign.targeting || '',
        ageMin: campaign.ageMin ?? null,
        ageMax: campaign.ageMax ?? null,
        gender: campaign.gender || '',
        interests: campaign.interests || '',
        behaviors: campaign.behaviors || ''
      }
    });
  } catch (error) {
    console.error('[SuperAdmin Customer Targeting] campaign detail failed:', error.message);
    return res.status(Number(error.status) || 502).json({ success: false, error: error.message });
  }
});

module.exports = router;
