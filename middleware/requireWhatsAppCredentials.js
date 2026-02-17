const { getWhatsAppCredentialsForUser } = require('../services/userWhatsAppCredentialsService');

module.exports = async (req, res, next) => {
  try {
    const credentials = await getWhatsAppCredentialsForUser({
      authHeader: req.headers.authorization || '',
      userId: req.user?.id || null
    });

    if (!credentials) {
      return res.status(400).json({
        success: false,
        error: 'WhatsApp credentials are not configured for this user. Please set twilioId, whatsappId, whatsappToken, and whatsappBusiness for this user in admin backend.',
        userId: req.user?.id || null
      });
    }

    req.whatsappCredentials = credentials;
    next();
  } catch (error) {
    const statusCode = Number(error?.statusCode) || 502;
    return res.status(statusCode).json({
      success: false,
      error: 'Failed to fetch WhatsApp credentials from admin backend',
      details: error.message
    });
  }
};
