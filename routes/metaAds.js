const express = require('express');
const multer = require('multer');
const jwt = require('jsonwebtoken');

const auth = require('../middleware/auth');
const MetaAdCampaign = require('../models/MetaAdCampaign');
const MetaAdsTransaction = require('../models/MetaAdsTransaction');
const MetaAdsWallet = require('../models/MetaAdsWallet');
const metaAdsService = require('../services/metaAdsService');

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });
const STATE_SECRET = process.env.JWT_SECRET || 'technova_jwt_secret_key_2024';

const getBackendOrigin = (req) => `${req.protocol}://${req.get('host')}`;
const getCallbackUrl = (req) => `${getBackendOrigin(req)}/api/meta-ads/oauth/callback`;
const buildFacebookAuthUrl = (req) => {
  const state = jwt.sign(
    {
      userId: req.user.id,
      origin: req.body?.origin || process.env.FRONTEND_URL || '',
      issuedAt: Date.now()
    },
    STATE_SECRET,
    { expiresIn: '10m' }
  );

  return metaAdsService.getLoginDialogUrl({
    redirectUri: getCallbackUrl(req),
    state
  });
};

const buildSetupState = async (userId) => {
  const setupBundle = await metaAdsService.getSetupBundle({ userId });
  const latestCampaign = await MetaAdCampaign.findOne({ userId }).sort({ updatedAt: -1 }).lean();
  const availablePageIds = new Set(
    (setupBundle.pages || []).map((page) => String(page?.id || '')).filter(Boolean)
  );
  const latestCampaignPageId = String(latestCampaign?.setupSnapshot?.selectedPageId || '').trim();
  const safeSelectedPageId =
    (latestCampaignPageId && availablePageIds.has(latestCampaignPageId) ? latestCampaignPageId : '') ||
    setupBundle.pageId ||
    '';

  return {
    connected: Boolean(setupBundle.connected),
    selectedAdAccountId: setupBundle.adAccountId || '',
    selectedPageId: safeSelectedPageId,
    linkedWhatsappNumber:
      latestCampaign?.setupSnapshot?.linkedWhatsappNumber ||
      setupBundle?.selectedWhatsappNumber ||
      setupBundle?.whatsappNumbers?.[0]?.display_phone_number ||
      '',
    availablePages: setupBundle.pages || [],
    availableBusinesses: setupBundle.businesses || [],
    availableAdAccounts: setupBundle.adAccounts || [],
    availableWhatsappNumbers: setupBundle.whatsappNumbers || [],
    adAccountId: setupBundle.adAccountId || '',
    mode: setupBundle.mode || 'mock',
    setupError: setupBundle.setupError || '',
    hasPageAccess: Boolean((setupBundle.pages || []).length && (setupBundle.pageId || (setupBundle.pages || [])[0]?.id)),
    authSource: setupBundle.authSource || 'env',
    profileName: setupBundle.profileName || ''
  };
};

const buildSummary = (campaigns = []) => {
  return campaigns.reduce(
    (acc, campaign) => {
      const spend = Number(campaign?.analytics?.spend || 0);
      const clicks = Number(campaign?.analytics?.clicks || 0);
      const leads = Number(campaign?.analytics?.leads || 0);
      const impressions = Number(campaign?.analytics?.impressions || 0);

      acc.totalCampaigns += 1;
      acc.activeCampaigns += String(campaign.status).toUpperCase() === 'ACTIVE' ? 1 : 0;
      acc.totalSpend += spend;
      acc.totalClicks += clicks;
      acc.totalLeads += leads;
      acc.totalImpressions += impressions;
      return acc;
    },
    {
      totalCampaigns: 0,
      activeCampaigns: 0,
      totalSpend: 0,
      totalClicks: 0,
      totalLeads: 0,
      totalImpressions: 0
    }
  );
};

const getOrCreateWallet = async (userId) => {
  let wallet = await MetaAdsWallet.findOne({ userId });
  if (!wallet) {
    wallet = await MetaAdsWallet.create({ userId, balance: 0 });
  }
  return wallet;
};

const buildWalletState = async (userId) => {
  const wallet = await getOrCreateWallet(userId);
  const transactions = await MetaAdsTransaction.find({ userId })
    .sort({ createdAt: -1 })
    .limit(10)
    .lean();

  return {
    balance: Number(wallet.balance || 0),
    currency: 'INR',
    transactions
  };
};

const getDraftCampaignForUser = async (userId, draftId) => {
  if (!draftId) return null;
  return MetaAdCampaign.findOne({ _id: draftId, userId });
};

const mergeCompletedSteps = (existing = [], nextStep) => {
  return Array.from(new Set([...(existing || []), nextStep])).sort((a, b) => a - b);
};

router.get('/overview', auth, async (req, res) => {
  try {
    const campaigns = await MetaAdCampaign.find({ userId: req.user.id }).sort({ createdAt: -1 }).lean();
    const summary = buildSummary(campaigns);

    res.json({
      success: true,
      setup: await buildSetupState(req.user.id),
      summary: {
        ...summary,
        averageCtr:
          summary.totalImpressions > 0
            ? Number(((summary.totalClicks / summary.totalImpressions) * 100).toFixed(2))
            : 0,
        averageCpl:
          summary.totalLeads > 0 ? Number((summary.totalSpend / summary.totalLeads).toFixed(2)) : 0
      },
      wallet: await buildWalletState(req.user.id),
      campaigns
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/diagnostics', auth, async (req, res) => {
  try {
    const diagnostics = await metaAdsService.getConnectionDiagnostics({ userId: req.user.id });
    res.json({
      success: true,
      diagnostics
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.details || null
    });
  }
});

router.post('/connect/auth-url', auth, async (req, res) => {
  try {
    res.json({
      success: true,
      authUrl: buildFacebookAuthUrl(req)
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

router.post('/auth/facebook', auth, async (req, res) => {
  try {
    res.json({
      success: true,
      authUrl: buildFacebookAuthUrl(req),
      message: 'Facebook OAuth URL generated successfully.'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

router.get('/oauth/callback', async (req, res) => {
  const { code, state, error: authError, error_message: authErrorMessage } = req.query;

  if (authError) {
    return res.status(200).send(`
      <script>
        if (window.opener) {
          window.opener.postMessage({ type: 'meta_oauth_error', error: ${JSON.stringify(authErrorMessage || authError)} }, '*');
        }
        window.close();
      </script>
    `);
  }

  try {
    const decoded = jwt.verify(String(state || ''), STATE_SECRET);
    const tokenData = await metaAdsService.exchangeCodeForAccessToken({
      code,
      redirectUri: getCallbackUrl(req)
    });

    await metaAdsService.saveUserConnection({
      userId: decoded.userId,
      accessToken: tokenData.access_token,
      scopes: Array.isArray(tokenData.granted_scopes) ? tokenData.granted_scopes : []
    });

    const setup = await buildSetupState(decoded.userId);
    const targetOrigin = decoded.origin || process.env.FRONTEND_URL || '*';

    return res.status(200).send(`
      <script>
        if (window.opener) {
          window.opener.postMessage(
            { type: 'meta_oauth_success', setup: ${JSON.stringify(setup)} },
            ${JSON.stringify(targetOrigin === '*' ? '*' : targetOrigin)}
          );
        }
        window.close();
      </script>
    `);
  } catch (error) {
    return res.status(200).send(`
      <script>
        if (window.opener) {
          window.opener.postMessage({ type: 'meta_oauth_error', error: ${JSON.stringify(error.message)} }, '*');
        }
        window.close();
      </script>
    `);
  }
});

router.post('/connect', auth, async (req, res) => {
  try {
    res.json({
      success: true,
      setup: await buildSetupState(req.user.id),
      message: 'Meta account access is ready for campaign setup.'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/adaccounts', auth, async (req, res) => {
  try {
    const adAccounts = await metaAdsService.getUserAdAccounts({ userId: req.user.id });
    res.json({
      success: true,
      adAccounts,
      selectedAdAccountId: (await buildSetupState(req.user.id)).selectedAdAccountId || ''
    });
  } catch (error) {
    res.status(error.status || 500).json({
      success: false,
      error: error.message
    });
  }
});

router.get('/wallet', auth, async (req, res) => {
  try {
    res.json({
      success: true,
      wallet: await buildWalletState(req.user.id)
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/wallet/topup', auth, async (req, res) => {
  try {
    const amount = Number(req.body?.amount || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ success: false, error: 'Enter a valid top-up amount.' });
    }

    const wallet = await getOrCreateWallet(req.user.id);
    wallet.balance = Number(wallet.balance || 0) + amount;
    await wallet.save();

    await MetaAdsTransaction.create({
      userId: req.user.id,
      amount,
      type: 'credit',
      note: 'Wallet top-up'
    });

    res.json({
      success: true,
      message: 'Credits added successfully.',
      wallet: await buildWalletState(req.user.id)
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/settings/selection', auth, async (req, res) => {
  try {
    await metaAdsService.saveUserSelections({
      userId: req.user.id,
      adAccountId: req.body?.adAccountId,
      pageId: req.body?.pageId,
      whatsappNumber: req.body?.whatsappNumber
    });

    res.json({
      success: true,
      setup: await buildSetupState(req.user.id),
      message: 'Meta setup selections saved.'
    });
  } catch (error) {
    res.status(error.status || 500).json({
      success: false,
      error: error.message
    });
  }
});

router.post('/save-adaccount', auth, async (req, res) => {
  try {
    await metaAdsService.saveUserAdAccountSelection({
      userId: req.user.id,
      adAccountId: req.body?.adAccountId
    });

    res.json({
      success: true,
      setup: await buildSetupState(req.user.id),
      message: 'Selected Meta ad account saved successfully.'
    });
  } catch (error) {
    res.status(error.status || 500).json({
      success: false,
      error: error.message
    });
  }
});

router.post('/campaigns', auth, upload.single('creativeFile'), async (req, res) => {
  try {
    const payload = req.body?.payload ? JSON.parse(req.body.payload) : req.body;
    const setup = await buildSetupState(req.user.id);
    const selectedAdAccountId = payload?.adAccountId || setup.selectedAdAccountId || setup.adAccountId || metaAdsService.getEnvConfig().adAccountId;
    const selectedPageId = payload?.configuredPageId || setup.selectedPageId || metaAdsService.getEnvConfig().pageId;
    const selectedWhatsappNumber = payload?.whatsappNumber || setup.linkedWhatsappNumber || '';

    const creativeUpload = await metaAdsService.uploadCreativeAsset({
      fileBuffer: req.file?.buffer,
      fileName: req.file?.originalname,
      mediaUrl: payload?.creative?.mediaUrl,
      userId: req.user.id,
      adAccountId: selectedAdAccountId
    });

    const stack = await metaAdsService.createMetaAdStack({
      campaign: {
        ...payload,
        adAccountId: selectedAdAccountId,
        configuredPageId: selectedPageId,
        whatsappNumber: selectedWhatsappNumber
      },
      creativeUpload,
      userId: req.user.id
    });

    const createdCampaign = await MetaAdCampaign.create({
      userId: req.user.id,
      campaignName: payload.campaignName,
      objective: payload.objective,
      status: payload.status || 'PAUSED',
      configuredPageId: selectedPageId,
      configuredInstagramActorId: payload.configuredInstagramActorId,
      whatsappNumber: selectedWhatsappNumber,
      budget: payload.budget,
      targeting: payload.targeting,
      creative: {
        ...payload.creative,
        mediaHash: creativeUpload.mediaHash || payload?.creative?.mediaHash || ''
      },
      placement: payload.placement,
      schedule: payload.schedule,
      meta: {
        adAccountId: stack.adAccountId || selectedAdAccountId,
        campaignId: stack.campaignId,
        adSetId: stack.adSetId,
        creativeId: stack.creativeId,
        adId: stack.adId
      },
      accounting: {
        reservedBudget: 0,
        totalDebited: 0,
        reconciledSpend: 0,
        lastReconciledAt: null
      },
      setupSnapshot: {
        connected: true,
        selectedAdAccountId,
        selectedPageId: selectedPageId,
        linkedWhatsappNumber: selectedWhatsappNumber
      },
      apiMode: stack.apiMode
    });

    const latestAnalytics = await metaAdsService.fetchCampaignInsights(createdCampaign);
    if (latestAnalytics) {
      createdCampaign.analytics = latestAnalytics;
      await createdCampaign.save();
    }

    res.status(201).json({
      success: true,
      message: 'Meta ad campaign created successfully.',
      campaign: createdCampaign
    });
  } catch (error) {
    res.status(error.status || 400).json({
      success: false,
      error: error.message,
      stage: error.stage || 'Campaign request',
      details: error.details || error.response?.data || null
    });
  }
});

router.post('/campaigns/step/campaign', auth, async (req, res) => {
  try {
    const draftId = req.body?.draftId;
    const campaignName = String(req.body?.campaignName || '').trim();
    const objective = String(req.body?.objective || 'OUTCOME_LEADS').trim();
    const adAccountId = String(req.body?.adAccountId || '').trim();

    if (!campaignName) {
      return res.status(400).json({ success: false, error: 'Campaign name is required.' });
    }

    let draft = await getDraftCampaignForUser(req.user.id, draftId);
    if (!draft) {
      draft = new MetaAdCampaign({
        userId: req.user.id,
        campaignName,
        objective,
        status: 'DRAFT',
        meta: { adAccountId },
        wizard: {
          currentStep: 2,
          completedSteps: [1],
          lastSavedAt: new Date()
        }
      });
    } else {
      draft.campaignName = campaignName;
      draft.objective = objective;
      if (adAccountId) {
        draft.meta = { ...(draft.meta?.toObject?.() || draft.meta || {}), adAccountId };
      }
      draft.status = draft.status || 'DRAFT';
      draft.wizard = {
        currentStep: 2,
        completedSteps: mergeCompletedSteps(draft.wizard?.completedSteps, 1),
        lastSavedAt: new Date()
      };
    }

    await draft.save();

    res.json({
      success: true,
      message: 'Campaign step saved.',
      draftId: String(draft._id),
      campaign: draft
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/campaigns/step/adset', auth, async (req, res) => {
  try {
    const draft = await getDraftCampaignForUser(req.user.id, req.body?.draftId);
    if (!draft) {
      return res.status(404).json({ success: false, error: 'Campaign draft not found.' });
    }

    draft.targeting = {
      ...(draft.targeting?.toObject?.() || draft.targeting || {}),
      countries: Array.isArray(req.body?.countries) ? req.body.countries : draft.targeting?.countries || ['IN'],
      ageMin: Number(req.body?.ageMin || draft.targeting?.ageMin || 21),
      ageMax: Number(req.body?.ageMax || draft.targeting?.ageMax || 45)
    };
    draft.budget = {
      ...(draft.budget?.toObject?.() || draft.budget || {}),
      dailyBudget: Number(req.body?.dailyBudget || draft.budget?.dailyBudget || 500),
      currency: String(req.body?.currency || draft.budget?.currency || 'INR')
    };
    draft.wizard = {
      currentStep: 3,
      completedSteps: mergeCompletedSteps(draft.wizard?.completedSteps, 2),
      lastSavedAt: new Date()
    };

    await draft.save();

    res.json({
      success: true,
      message: 'Ad set step saved.',
      draftId: String(draft._id),
      campaign: draft
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/campaigns/step/ad', auth, upload.single('creativeFile'), async (req, res) => {
  try {
    const payload = req.body?.payload ? JSON.parse(req.body.payload) : req.body;
    const draft = await getDraftCampaignForUser(req.user.id, payload?.draftId);
    if (!draft) {
      return res.status(404).json({ success: false, error: 'Campaign draft not found.' });
    }

    const setup = await buildSetupState(req.user.id);
    const selectedAdAccountId =
      draft.meta?.adAccountId ||
      draft.setupSnapshot?.selectedAdAccountId ||
      setup.selectedAdAccountId ||
      setup.adAccountId;

    const creativeUpload = await metaAdsService.uploadCreativeAsset({
      fileBuffer: req.file?.buffer,
      fileName: req.file?.originalname,
      mediaUrl: payload?.mediaUrl,
      userId: req.user.id,
      adAccountId: selectedAdAccountId
    });

    draft.creative = {
      ...(draft.creative?.toObject?.() || draft.creative || {}),
      primaryText: String(payload?.primaryText || draft.creative?.primaryText || ''),
      headline: String(payload?.headline || draft.creative?.headline || draft.campaignName || ''),
      description: String(payload?.description || draft.creative?.description || ''),
      callToAction: String(payload?.callToAction || draft.creative?.callToAction || 'WHATSAPP_MESSAGE'),
      mediaUrl: creativeUpload.mediaUrl || payload?.mediaUrl || draft.creative?.mediaUrl || '',
      mediaHash: creativeUpload.mediaHash || draft.creative?.mediaHash || ''
    };
    draft.wizard = {
      currentStep: 4,
      completedSteps: mergeCompletedSteps(draft.wizard?.completedSteps, 3),
      lastSavedAt: new Date()
    };

    await draft.save();

    res.json({
      success: true,
      message: 'Ad creative step saved.',
      draftId: String(draft._id),
      campaign: draft
    });
  } catch (error) {
    res.status(error.status || 400).json({
      success: false,
      error: error.message,
      details: error.details || null
    });
  }
});

router.post('/campaigns/:id/publish', auth, async (req, res) => {
  try {
    const draft = await MetaAdCampaign.findOne({ _id: req.params.id, userId: req.user.id });
    if (!draft) {
      return res.status(404).json({ success: false, error: 'Campaign draft not found.' });
    }

    const setup = await buildSetupState(req.user.id);
    const selectedAdAccountId =
      draft.meta?.adAccountId ||
      draft.setupSnapshot?.selectedAdAccountId ||
      setup.selectedAdAccountId ||
      setup.adAccountId ||
      metaAdsService.getEnvConfig().adAccountId;
    const selectedPageId =
      draft.configuredPageId ||
      draft.setupSnapshot?.selectedPageId ||
      setup.selectedPageId ||
      setup.availablePages?.[0]?.id ||
      '';
    const selectedWhatsappNumber =
      draft.whatsappNumber ||
      draft.setupSnapshot?.linkedWhatsappNumber ||
      setup.linkedWhatsappNumber ||
      '';

    if (!setup.availablePages?.length || !selectedPageId) {
      return res.status(400).json({
        success: false,
        error: 'Creative creation cannot continue because no accessible Facebook Page is available for the current Meta login.',
        stage: 'Creative creation',
        details: {
          requestedPageId:
            draft.configuredPageId ||
            draft.setupSnapshot?.selectedPageId ||
            metaAdsService.getEnvConfig().pageId ||
            '',
          accessiblePages: setup.availablePages || [],
          setupError: setup.setupError || '',
          action:
            'Reconnect Facebook and grant page access. Then choose a Facebook Page in Settings before publishing.'
        }
      });
    }

    const stack = await metaAdsService.createMetaAdStack({
      campaign: {
        campaignName: draft.campaignName,
        objective: draft.objective,
        status: 'PAUSED',
        adAccountId: selectedAdAccountId,
        configuredPageId: selectedPageId,
        configuredInstagramActorId: draft.configuredInstagramActorId,
        whatsappNumber: selectedWhatsappNumber,
        budget: draft.budget,
        targeting: draft.targeting,
        creative: draft.creative,
        placement: draft.placement,
        schedule: draft.schedule
      },
      creativeUpload: {
        mediaHash: draft.creative?.mediaHash || '',
        mediaUrl: draft.creative?.mediaUrl || ''
      },
      userId: req.user.id
    });

    draft.status = 'PAUSED';
    draft.configuredPageId = selectedPageId;
    draft.whatsappNumber = selectedWhatsappNumber;
    draft.meta = {
      ...(draft.meta?.toObject?.() || draft.meta || {}),
      adAccountId: stack.adAccountId || selectedAdAccountId,
      campaignId: stack.campaignId,
      adSetId: stack.adSetId,
      creativeId: stack.creativeId,
      adId: stack.adId
    };
    draft.setupSnapshot = {
      connected: true,
      selectedAdAccountId,
      selectedPageId,
      linkedWhatsappNumber: selectedWhatsappNumber
    };
    draft.apiMode = stack.apiMode;
    draft.wizard = {
      currentStep: 4,
      completedSteps: mergeCompletedSteps(draft.wizard?.completedSteps, 4),
      lastSavedAt: new Date()
    };

    const latestAnalytics = await metaAdsService.fetchCampaignInsights(draft);
    if (latestAnalytics) {
      draft.analytics = latestAnalytics;
    }

    await draft.save();

    res.json({
      success: true,
      message: 'Campaign published successfully.',
      campaign: draft
    });
  } catch (error) {
    res.status(error.status || 400).json({
      success: false,
      error: error.message,
      stage: error.stage || 'Campaign publish',
      details: error.details || null
    });
  }
});

router.get('/campaigns', auth, async (req, res) => {
  try {
    const campaigns = await MetaAdCampaign.find({ userId: req.user.id }).sort({ createdAt: -1 });
    res.json({ success: true, campaigns });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/campaigns/:id/status', auth, async (req, res) => {
  try {
    const campaign = await MetaAdCampaign.findOne({ _id: req.params.id, userId: req.user.id });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    const status = String(req.body?.status || '').toUpperCase();
    if (status === 'ACTIVE') {
      const wallet = await getOrCreateWallet(req.user.id);
      const requiredBalance = Math.max(0, Number(campaign?.budget?.dailyBudget || 0));
      if (Number(wallet.balance || 0) < requiredBalance) {
        return res.status(400).json({
          success: false,
          error: `Insufficient wallet balance. Add at least ₹${requiredBalance} before activating this campaign.`,
          stage: 'Wallet balance check'
        });
      }

      if (String(campaign.status).toUpperCase() !== 'ACTIVE' && requiredBalance > 0) {
        wallet.balance = Number(wallet.balance || 0) - requiredBalance;
        await wallet.save();

        await MetaAdsTransaction.create({
          userId: req.user.id,
          campaignId: campaign._id,
          amount: requiredBalance,
          type: 'debit',
          note: `Activation reserve for ${campaign.campaignName}`
        });

        campaign.accounting = {
          reservedBudget: Number(campaign?.accounting?.reservedBudget || 0) + requiredBalance,
          totalDebited: Number(campaign?.accounting?.totalDebited || 0) + requiredBalance,
          reconciledSpend: Number(campaign?.accounting?.reconciledSpend || 0),
          lastReconciledAt: campaign?.accounting?.lastReconciledAt || null
        };
      }
    }

    const result = await metaAdsService.updateCampaignDeliveryStatus({
      campaign,
      userId: req.user.id,
      status
    });

    campaign.status = result.status;
    campaign.lastError = '';
    await campaign.save();

    res.json({
      success: true,
      message: `Campaign ${result.status === 'ACTIVE' ? 'activated' : 'paused'} successfully.`,
      campaign,
      wallet: await buildWalletState(req.user.id)
    });
  } catch (error) {
    res.status(error.status || 400).json({
      success: false,
      error: error.message,
      stage: error.stage || 'Campaign status update',
      details: error.details || error.response?.data || null
    });
  }
});

router.post('/campaigns/:id/sync', auth, async (req, res) => {
  try {
    const campaign = await MetaAdCampaign.findOne({ _id: req.params.id, userId: req.user.id });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    await metaAdsService.syncCampaignAnalyticsRecord(campaign);

    res.json({ success: true, campaign });
  } catch (error) {
    const campaign = await MetaAdCampaign.findOne({ _id: req.params.id, userId: req.user.id });
    if (campaign) {
      campaign.lastError = error.message || 'Insights sync failed';
      await campaign.save();
      return res.status(200).json({
        success: true,
        warning: error.message,
        stage: error.stage || 'Insights sync',
        campaign
      });
    }

    res.status(error.status || 400).json({
      success: false,
      error: error.message,
      stage: error.stage || 'Insights sync',
      details: error.details || error.response?.data || null
    });
  }
});

router.post('/campaigns/sync-all', auth, async (req, res) => {
  try {
    const campaigns = await MetaAdCampaign.find({ userId: req.user.id, status: { $in: ['ACTIVE', 'PAUSED'] } });
    let synced = 0;
    const warnings = [];

    for (const campaign of campaigns) {
      try {
        await metaAdsService.syncCampaignAnalyticsRecord(campaign);
        synced += 1;
      } catch (error) {
        campaign.lastError = error.message || 'Analytics sync failed';
        await campaign.save();
        warnings.push({
          campaignId: String(campaign._id),
          campaignName: campaign.campaignName,
          error: error.message
        });
      }
    }

    res.json({
      success: true,
      synced,
      warnings
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
