const { getWhatsAppCredentialsForUser } = require('../services/userWhatsAppCredentialsService');

const createWhatsAppCredentialsMiddleware = ({ forceRefresh = false } = {}) => async (req, res, next) => {
  try {
    const credentials = await getWhatsAppCredentialsForUser({
      authHeader: req.headers.authorization || '',
      userId: req.user?.id || null,
      forceRefresh
    });

    if (!credentials) {
      return res.status(400).json({
        success: false,
        error: 'WhatsApp credentials are not configured for this user. Please set twilioId, whatsappId, whatsappToken, and whatsappBusiness for this user in admin backend.',
        userId: req.user?.id || null
      });
    }

    const requestCompanyId = String(req.companyId || req.user?.companyId || '').trim();
    const credentialCompanyId = String(credentials.companyId || '').trim();
    if (forceRefresh && requestCompanyId && credentialCompanyId && requestCompanyId !== credentialCompanyId) {
      console.error('[WHATSAPP_CREDENTIAL_SELECTION]', JSON.stringify({
        outcome: 'failure', reason: 'company_mismatch',
        userId: String(req.user?.id || ''),
        requestCompanyId,
        credentialCompanyId,
        credentialOwnerUserId: String(credentials.credentialOwnerUserId || ''),
        credentialSource: credentials.credentialSource || 'unknown'
      }));
      return res.status(409).json({
        success: false,
        error: 'WhatsApp credentials belong to a different company. Verify the connected client account before syncing templates.'
      });
    }

    req.whatsappCredentials = credentials;
    console.info('[WHATSAPP_CREDENTIAL_SELECTION]', JSON.stringify({
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || req.user?.companyId || ''),
      credentialOwnerUserId: String(credentials.credentialOwnerUserId || req.user?.id || ''),
      credentialSource: credentials.credentialSource || 'unknown',
      forceRefresh,
      wabaId: credentials.businessAccountId,
      phoneNumberId: credentials.phoneNumberId
    }));
    return next();
  } catch (error) {
    const statusCode = Number(error?.statusCode) || 502;
    console.error('[WHATSAPP_CREDENTIAL_SELECTION]', JSON.stringify({
      outcome: 'failure',
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || req.user?.companyId || ''),
      forceRefresh,
      statusCode,
      error: String(error?.message || 'Credential lookup failed')
    }));
    return res.status(statusCode).json({
      success: false,
      error: 'Failed to fetch WhatsApp credentials from admin backend',
      details: error.message
    });
  }
};

const requireWhatsAppCredentials = createWhatsAppCredentialsMiddleware();
requireWhatsAppCredentials.forceRefresh = createWhatsAppCredentialsMiddleware({ forceRefresh: true });

module.exports = requireWhatsAppCredentials;
