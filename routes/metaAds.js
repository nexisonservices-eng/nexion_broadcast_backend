const express = require('express');
const crypto = require('crypto');

const auth = require('../middleware/auth');
const Campaign = require('../models/campaign');
const MetaAdsTransaction = require('../models/MetaAdsTransaction');
const MetaAdsWallet = require('../models/MetaAdsWallet');
const metaAdsService = require('../services/metaAdsService');
const { getMetaConfigForUser, getMetaConfigByUserId } = require('../services/userMetaCredentialsService');
const { requireJwtSecret } = require('../utils/securityConfig');
const {
  DEFAULT_PHONE_KEYS,
  DEFAULT_NAME_KEYS,
  DEFAULT_EMAIL_KEYS,
  DEFAULT_CONSENT_KEYS,
  DEFAULT_APPROVED_VALUES,
  fetchMetaLead,
  buildResolvedLeadPayload,
  syncMetaLeadConsent
} = require('../services/metaLeadConsentService');

const router = express.Router();
const OAUTH_STATE_CACHE_TTL_MS = 15 * 60 * 1000;
const oauthStateConfigCache = new Map();

const normalizeOrigin = (value) => String(value || '').trim().replace(/\/+$/, '');
const isSafeFrontendOrigin = (value) => /^https?:\/\/[^/\s]+$/i.test(normalizeOrigin(value));
const escapeHtml = (value) =>
  String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
const getBackendOrigin = (req) =>
  normalizeOrigin(process.env.PUBLIC_BACKEND_URL) || `${req.protocol}://${req.get('host')}`;
const getCallbackUrl = (req) => `${getBackendOrigin(req)}/api/meta-ads/oauth/callback`;
const getResolvedRedirectUri = (req, metaConfig = null) =>
  normalizeOrigin(metaConfig?.redirectUri) || getCallbackUrl(req);
const resolveMetaOAuthConfig = (metaConfig = null) => ({
  appId: String(metaConfig?.appId || process.env.META_APP_ID || '').trim(),
  appSecret: String(metaConfig?.appSecret || process.env.META_APP_SECRET || '').trim(),
  redirectUri: normalizeOrigin(metaConfig?.redirectUri),
  apiVersion: String(metaConfig?.apiVersion || process.env.META_API_VERSION || 'v22.0').trim(),
  credentialOwnerUserId: String(metaConfig?.credentialOwnerUserId || '').trim()
});

const encodeStatePayload = (payload) => Buffer.from(JSON.stringify(payload)).toString('base64url');
const signStatePayload = (payload, secret) =>
  crypto
    .createHmac('sha256', String(secret || requireJwtSecret('Meta OAuth state signing')))
    .update(payload)
    .digest('hex');

const buildSignedState = ({ userId, origin, credentialOwnerUserId }) => {
  const payload = encodeStatePayload({
    userId,
    credentialOwnerUserId,
    origin,
    issuedAt: Date.now(),
    expiresAt: Date.now() + 10 * 60 * 1000
  });
  const signature = signStatePayload(payload);
  return `${payload}.${signature}`;
};

const setCachedOAuthConfig = (state, metaConfig) => {
  const cacheKey = String(state || '').trim();
  if (!cacheKey || !metaConfig?.appId || !metaConfig?.appSecret) return;

  oauthStateConfigCache.set(cacheKey, {
    metaConfig,
    expiresAt: Date.now() + OAUTH_STATE_CACHE_TTL_MS
  });
};

const getCachedOAuthConfig = (state) => {
  const cacheKey = String(state || '').trim();
  if (!cacheKey) return null;

  const cached = oauthStateConfigCache.get(cacheKey);
  if (!cached) return null;
  if (Number(cached.expiresAt || 0) < Date.now()) {
    oauthStateConfigCache.delete(cacheKey);
    return null;
  }

  return cached.metaConfig || null;
};

const deleteCachedOAuthConfig = (state) => {
  const cacheKey = String(state || '').trim();
  if (!cacheKey) return;
  oauthStateConfigCache.delete(cacheKey);
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
  const metaConfig = resolveMetaOAuthConfig(await getMetaConfigForUser({ authHeader }));
  if (!metaConfig.appId || !metaConfig.appSecret) {
    const error = new Error(
      'Meta App ID and Meta App Secret are missing. Configure them in admin credentials for this company admin, or set META_APP_ID and META_APP_SECRET in backend environment variables.'
    );
    error.status = 400;
    throw error;
  }

  const state = buildSignedState({
    userId: req.user.id,
    credentialOwnerUserId: metaConfig.credentialOwnerUserId || req.user.id,
    origin: req.body?.origin || process.env.FRONTEND_URL || ''
  });

  setCachedOAuthConfig(state, metaConfig);

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
      authUrl: await buildFacebookAuthUrl(req),
      backendOrigin: getBackendOrigin(req)
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
      backendOrigin: getBackendOrigin(req),
      message: 'Facebook OAuth URL generated successfully.'
    });
  } catch (error) {
    res.status(error.status || 500).json({ success: false, error: error.message });
  }
});

router.get('/oauth/callback', async (req, res) => {
  const { code, state, error: authError, error_message: authErrorMessage } = req.query;

  const renderCallbackPage = ({ message, payload, targetOrigin = '' }) => `
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
            .detail { background:#eff6ff; color:#1e3a8a; border-radius:10px; padding:12px; font-size:14px; word-break:break-word; }
            button { border:0; border-radius:10px; background:#2563eb; color:#fff; padding:10px 16px; font-weight:600; cursor:pointer; }
          </style>
        </head>
        <body>
          <div class="card">
            <h1>${message}</h1>
            ${payload?.error ? `<p class="detail">${escapeHtml(payload.error)}</p>` : ''}
            <p>You can close this window and return to the app if it does not close automatically.</p>
            <button onclick="window.close()">Close Window</button>
          </div>
          <script>
            (function () {
              try {
                var targetOrigin = ${JSON.stringify(targetOrigin)};
                if (window.opener && !window.opener.closed && targetOrigin) {
                  window.opener.postMessage(${JSON.stringify(payload)}, targetOrigin);
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
    const credentialLookupUserId = decoded.credentialOwnerUserId || decoded.userId;
    const metaConfig = resolveMetaOAuthConfig(
      getCachedOAuthConfig(state) || (await getMetaConfigByUserId(credentialLookupUserId))
    );
    if (!metaConfig.appId || !metaConfig.appSecret) {
      throw new Error(
        'Meta app credentials are not configured. Save Meta App ID and Meta App Secret for the company admin in the admin backend, or set META_APP_ID and META_APP_SECRET in backend env, then try reconnecting again.'
      );
    }

    const expectedSignature = signStatePayload(payload);
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
    deleteCachedOAuthConfig(state);

    const setup = await metaAdsService.getSetupBundle({ userId: decoded.userId });
    const targetOrigin = isSafeFrontendOrigin(decoded.origin)
      ? normalizeOrigin(decoded.origin)
      : (isSafeFrontendOrigin(process.env.FRONTEND_URL) ? normalizeOrigin(process.env.FRONTEND_URL) : '');

    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Meta account connected',
          payload: { type: 'meta_oauth_success', setup, backendOrigin: getBackendOrigin(req) },
          targetOrigin
        })
      );
  } catch (error) {
    deleteCachedOAuthConfig(state);
    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Meta connection failed',
          payload: {
            type: 'meta_oauth_error',
            error: String(error.message || 'Meta OAuth failed.'),
            backendOrigin: getBackendOrigin(req)
          }
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

router.get('/leads/:leadId/preview', auth, async (req, res) => {
  try {
    const leadData = await fetchMetaLead({ userId: req.user.id, leadId: req.params.leadId });
    const mapping = {
      phoneFieldKeys: req.query.phoneFieldKeys ? String(req.query.phoneFieldKeys).split(',') : DEFAULT_PHONE_KEYS,
      nameFieldKeys: req.query.nameFieldKeys ? String(req.query.nameFieldKeys).split(',') : DEFAULT_NAME_KEYS,
      emailFieldKeys: req.query.emailFieldKeys ? String(req.query.emailFieldKeys).split(',') : DEFAULT_EMAIL_KEYS,
      consentFieldKeys: req.query.consentFieldKeys ? String(req.query.consentFieldKeys).split(',') : DEFAULT_CONSENT_KEYS,
      consentApprovedValues: req.query.consentApprovedValues
        ? String(req.query.consentApprovedValues).split(',')
        : DEFAULT_APPROVED_VALUES
    };

    res.json({
      success: true,
      data: {
        lead: leadData,
        resolved: buildResolvedLeadPayload(leadData, mapping),
        mapping
      }
    });
  } catch (error) {
    res.status(error.status || 500).json({
      success: false,
      error: error.message,
      details: error.details || null
    });
  }
});

router.post('/leads/sync-consent', auth, async (req, res) => {
  try {
    const leadId = String(req.body?.leadId || '').trim();
    if (!leadId) {
      return res.status(400).json({ success: false, error: 'leadId is required.' });
    }

    const mapping = {
      phoneFieldKeys: Array.isArray(req.body?.mapping?.phoneFieldKeys)
        ? req.body.mapping.phoneFieldKeys
        : DEFAULT_PHONE_KEYS,
      nameFieldKeys: Array.isArray(req.body?.mapping?.nameFieldKeys)
        ? req.body.mapping.nameFieldKeys
        : DEFAULT_NAME_KEYS,
      emailFieldKeys: Array.isArray(req.body?.mapping?.emailFieldKeys)
        ? req.body.mapping.emailFieldKeys
        : DEFAULT_EMAIL_KEYS,
      consentFieldKeys: Array.isArray(req.body?.mapping?.consentFieldKeys)
        ? req.body.mapping.consentFieldKeys
        : DEFAULT_CONSENT_KEYS,
      consentApprovedValues: Array.isArray(req.body?.mapping?.consentApprovedValues)
        ? req.body.mapping.consentApprovedValues
        : DEFAULT_APPROVED_VALUES,
      consentText: req.body?.mapping?.consentText,
      scope: req.body?.mapping?.scope || 'marketing'
    };

    const result = await syncMetaLeadConsent({
      userId: req.user.id,
      leadId,
      companyId: req.companyId || req.body?.companyId || null,
      mapping,
      capturedBy: req.user?.email || req.user?.name || req.user?.id || 'meta_lead_sync'
    });

    res.json({
      success: true,
      data: {
        contact: result.contact,
        lead: result.leadData,
        resolved: result.resolvedLead
      }
    });
  } catch (error) {
    res.status(error.status || 500).json({
      success: false,
      error: error.message,
      details: error.details || null
    });
  }
});

module.exports = router;
