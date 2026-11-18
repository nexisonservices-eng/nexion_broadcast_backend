const { getWhatsAppCredentialsForUser } = require('../services/userWhatsAppCredentialsService');

const createWhatsAppCredentialsMiddleware = ({ forceRefresh = false, templateWrite = false } = {}) => async (req, res, next) => {
  const requestCompanyId = String(req.companyId || req.user?.companyId || '').trim();
  try {
    const credentials = await getWhatsAppCredentialsForUser({
      authHeader: req.headers.authorization || '',
      userId: req.user?.id || null,
      forceRefresh,
      ...(templateWrite ? { templateCompanyId: requestCompanyId } : {})
    });

    if (!credentials) {
      return res.status(400).json({
        success: false,
        error: 'WhatsApp credentials are not configured for this user. Please set twilioId, whatsappId, whatsappToken, and whatsappBusiness for this user in admin backend.',
        userId: req.user?.id || null
      });
    }

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
    const message = templateWrite && statusCode !== 400 && statusCode !== 409
      ? 'Failed to fetch WhatsApp credentials from admin backend'
      : String(error?.message || 'Credential lookup failed');
    console.error('[WHATSAPP_CREDENTIAL_SELECTION]', JSON.stringify({
      outcome: 'failure',
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || req.user?.companyId || ''),
      forceRefresh,
      statusCode,
      error: message
    }));
    return res.status(statusCode).json({
      success: false,
      error: templateWrite ? message : 'Failed to fetch WhatsApp credentials from admin backend',
      ...(templateWrite ? {} : { details: error.message })
    });
  }
};

const requireWhatsAppCredentials = createWhatsAppCredentialsMiddleware();
requireWhatsAppCredentials.forceRefresh = createWhatsAppCredentialsMiddleware({ forceRefresh: true });
requireWhatsAppCredentials.forTemplateWrite = createWhatsAppCredentialsMiddleware({ forceRefresh: true, templateWrite: true });

module.exports = requireWhatsAppCredentials;
