const express = require('express');
const crypto = require('crypto');

const auth = require('../middleware/auth');
const Campaign = require('../models/campaign');
const MetaAdsTransaction = require('../models/MetaAdsTransaction');
const MetaAdsWallet = require('../models/MetaAdsWallet');
const metaAdsService = require('../services/metaAdsService');
const { getMetaConfigForUser, getMetaConfigByUserId } = require('../services/userMetaCredentialsService');

const router = express.Router();
const STATE_SECRET = process.env.JWT_SECRET || 'technova_jwt_secret_key_2024';

const normalizeOrigin = (value) => String(value || '').trim().replace(/\/+$/, '');
const getBackendOrigin = (req) =>
  normalizeOrigin(process.env.PUBLIC_BACKEND_URL) || `${req.protocol}://${req.get('host')}`;
const getCallbackUrl = (req) => `${getBackendOrigin(req)}/api/meta-ads/oauth/callback`;
const getResolvedRedirectUri = (req, metaConfig = null) =>
  normalizeOrigin(metaConfig?.redirectUri) || getCallbackUrl(req);

const encodeStatePayload = (payload) => Buffer.from(JSON.stringify(payload)).toString('base64url');
const signStatePayload = (payload, secret) =>
  crypto.createHmac('sha256', String(secret || '')).update(payload).digest('hex');

const buildSignedState = ({ userId, origin }) => {
  const payload = encodeStatePayload({
    userId,
    origin,
    issuedAt: Date.now(),
    expiresAt: Date.now() + 10 * 60 * 1000
  });
  const signature = signStatePayload(payload, STATE_SECRET);
  return `${payload}.${signature}`;
};

const parseSignedState = (state) => {
  const [payload = '', signature = ''] = String(state || '').split('.');
  if (!payload || !signature) {
    throw new Error('Meta OAuth state is invalid.');
  }

  let decoded;
  try {
    decoded = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
  } catch {
    throw new Error('Meta OAuth state payload is invalid.');
  }

  return {
    payload,
    signature,
    decoded
  };
};

const buildFacebookAuthUrl = async (req) => {
  const authHeader = req.headers.authorization || '';
  const metaConfig = await getMetaConfigForUser({ authHeader });
  if (!metaConfig?.appId || !metaConfig?.appSecret) {
    const error = new Error('Meta App ID and Meta App Secret must be configured by super admin for this admin.');
    error.status = 400;
    throw error;
  }

  const state = buildSignedState({
    userId: req.user.id,
    origin: req.body?.origin || process.env.FRONTEND_URL || ''
  });

  return metaAdsService.getLoginDialogUrl({
    redirectUri: getResolvedRedirectUri(req, metaConfig),
    state,
    appId: metaConfig.appId,
    apiVersion: metaConfig.apiVersion
  });
};

const buildSummary = (campaigns = []) =>
  campaigns.reduce(
    (acc, campaign) => {
      const spend = Number(campaign?.spent || campaign?.analytics?.spend || 0);
      const clicks = Number(campaign?.clicks || campaign?.analytics?.clicks || 0);
      const leads = Number(campaign?.analytics?.leads || 0);
      const impressions = Number(campaign?.impressions || campaign?.analytics?.impressions || 0);

      acc.totalCampaigns += 1;
      acc.activeCampaigns += String(campaign.status || '').toUpperCase() === 'ACTIVE' ? 1 : 0;
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

router.get('/overview', auth, async (req, res) => {
  try {
    const campaigns = await Campaign.find({ createdBy: req.user.id }).sort({ createdAt: -1 }).lean();
    const summary = buildSummary(campaigns);
    const setup = await metaAdsService.getSetupBundle({ userId: req.user.id });

    res.json({
      success: true,
      setup,
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
    res.json({ success: true, diagnostics });
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
      authUrl: await buildFacebookAuthUrl(req)
    });
  } catch (error) {
    res.status(error.status || 500).json({ success: false, error: error.message });
  }
});

router.post('/auth/facebook', auth, async (req, res) => {
  try {
    res.json({
      success: true,
      authUrl: await buildFacebookAuthUrl(req),
      message: 'Facebook OAuth URL generated successfully.'
    });
  } catch (error) {
    res.status(error.status || 500).json({ success: false, error: error.message });
  }
});

router.get('/oauth/callback', async (req, res) => {
  const { code, state, error: authError, error_message: authErrorMessage } = req.query;

  const renderCallbackPage = ({ message, payload, targetOrigin = '*' }) => `
      <!doctype html>
      <html lang="en">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>Meta Connect</title>
          <style>
            body { font-family: Arial, sans-serif; background:#f5f7fb; color:#102042; display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; }
            .card { background:#fff; padding:24px 28px; border-radius:16px; box-shadow:0 18px 48px rgba(16,32,66,.14); max-width:480px; width:calc(100% - 32px); }
            h1 { margin:0 0 12px; font-size:22px; }
            p { margin:0 0 16px; line-height:1.5; }
            button { border:0; border-radius:10px; background:#2563eb; color:#fff; padding:10px 16px; font-weight:600; cursor:pointer; }
          </style>
        </head>
        <body>
          <div class="card">
            <h1>${message}</h1>
            <p>You can close this window and return to the app if it does not close automatically.</p>
            <button onclick="window.close()">Close Window</button>
          </div>
          <script>
            (function () {
              try {
                if (window.opener && !window.opener.closed) {
                  window.opener.postMessage(${JSON.stringify(payload)}, ${JSON.stringify(targetOrigin === '*' ? '*' : targetOrigin)});
                  setTimeout(function () { window.close(); }, 250);
                }
              } catch (error) {
                console.error('Meta OAuth callback handoff failed:', error);
              }
            }());
          </script>
        </body>
      </html>
    `;

  if (authError) {
    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Meta connection failed',
          payload: { type: 'meta_oauth_error', error: String(authErrorMessage || authError || 'Meta OAuth failed.') }
        })
      );
  }

  try {
    const { payload, signature, decoded } = parseSignedState(state);
    const metaConfig = await getMetaConfigByUserId(decoded.userId);
    if (!metaConfig?.appId || !metaConfig?.appSecret) {
      throw new Error('Meta app credentials are not configured for this admin.');
    }

    const expectedSignature = signStatePayload(payload, STATE_SECRET);
    if (signature !== expectedSignature) {
      throw new Error('Meta OAuth state signature mismatch.');
    }
    if (!decoded?.expiresAt || Number(decoded.expiresAt) < Date.now()) {
      throw new Error('Meta OAuth state expired.');
    }

    const tokenData = await metaAdsService.exchangeCodeForAccessToken({
      code,
      redirectUri: getResolvedRedirectUri(req, metaConfig),
      appId: metaConfig.appId,
      appSecret: metaConfig.appSecret,
      apiVersion: metaConfig.apiVersion
    });

    await metaAdsService.saveUserConnection({
      userId: decoded.userId,
      accessToken: tokenData.access_token,
      scopes: Array.isArray(tokenData.granted_scopes) ? tokenData.granted_scopes : []
    });

    const setup = await metaAdsService.getSetupBundle({ userId: decoded.userId });
    const targetOrigin = decoded.origin || process.env.FRONTEND_URL || '*';

    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Meta account connected',
          payload: { type: 'meta_oauth_success', setup },
          targetOrigin
        })
      );
  } catch (error) {
    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Meta connection failed',
          payload: { type: 'meta_oauth_error', error: String(error.message || 'Meta OAuth failed.') }
        })
      );
  }
});

router.post('/connect', auth, async (req, res) => {
  try {
    res.json({
      success: true,
      setup: await metaAdsService.getSetupBundle({ userId: req.user.id }),
      message: 'Meta account access is ready for campaign setup.'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/adaccounts', auth, async (req, res) => {
  try {
    const adAccounts = await metaAdsService.getUserAdAccounts({ userId: req.user.id });
    const setup = await metaAdsService.getSetupBundle({ userId: req.user.id });
    res.json({
      success: true,
      adAccounts,
      selectedAdAccountId: setup.adAccountId || ''
    });
  } catch (error) {
    res.status(error.status || 500).json({ success: false, error: error.message });
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
      setup: await metaAdsService.getSetupBundle({ userId: req.user.id }),
      message: 'Meta setup selections saved.'
    });
  } catch (error) {
    res.status(error.status || 500).json({ success: false, error: error.message });
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
      setup: await metaAdsService.getSetupBundle({ userId: req.user.id }),
      message: 'Selected Meta ad account saved successfully.'
    });
  } catch (error) {
    res.status(error.status || 500).json({ success: false, error: error.message });
  }
});

module.exports = router;
