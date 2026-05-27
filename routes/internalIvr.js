const express = require('express');
const router = express.Router();

const whatsappService = require('../services/whatsappService');
const {
  getWhatsAppCredentialsByUserId
} = require('../services/userWhatsAppCredentialsService');

const trimOrNull = (value) => {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
};

const requireInternalApiKey = (req, res, next) => {
  const expected = trimOrNull(process.env.WHATSAPP_BACKEND_INTERNAL_API_KEY || process.env.INTERNAL_API_KEY || process.env.ADMIN_INTERNAL_API_KEY);
  const provided = trimOrNull(req.headers['x-internal-api-key']);
  if (!expected) {
    return res.status(503).json({
      success: false,
      error: 'Internal booking notification endpoint is not configured'
    });
  }
  if (!provided || provided !== expected) {
    return res.status(401).json({
      success: false,
      error: 'Unauthorized internal request'
    });
  }
  return next();
};

router.post('/notify', requireInternalApiKey, async (req, res) => {
  try {
    const {
      userId = '',
      recipient = '',
      messageType = 'text',
      templateName = '',
      language = 'en_US',
      variables = [],
      text = '',
      components = [],
      templateComponents = []
    } = req.body || {};

    if (!userId || !recipient) {
      return res.status(400).json({
        success: false,
        error: 'userId and recipient are required'
      });
    }

    const credentials = await getWhatsAppCredentialsByUserId(userId);
    if (!credentials) {
      return res.status(404).json({
        success: false,
        error: 'WhatsApp credentials not found for the requested user'
      });
    }

    const normalizedMessageType = String(messageType || 'text').trim().toLowerCase();
    let result;
    if (normalizedMessageType === 'template' || templateName) {
      result = await whatsappService.sendTemplateMessage(
        recipient,
        templateName,
        language || 'en_US',
        Array.isArray(variables) ? variables : [],
        credentials,
        true,
        Array.isArray(templateComponents) && templateComponents.length > 0 ? templateComponents : components
      );
    } else {
      result = await whatsappService.sendTextMessage(
        recipient,
        text,
        credentials
      );
    }

    if (!result.success) {
      return res.status(400).json({
        success: false,
        error: result.error || 'Failed to send WhatsApp message'
      });
    }

    return res.json({
      success: true,
      data: result.data || null
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Internal IVR notification failed'
    });
  }
});

module.exports = router;
