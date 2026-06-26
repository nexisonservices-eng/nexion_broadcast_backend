const express = require('express');
const router = express.Router();

const whatsappService = require('../services/whatsappService');
const {
  getWhatsAppCredentialsByUserId,
  getAdminBackendConfig
} = require('../services/userWhatsAppCredentialsService');

const trimOrNull = (value) => {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
};

const normalizeErrorMessage = (value, fallback = 'Failed to send WhatsApp message') => {
  if (!value) return fallback;
  if (typeof value === 'string') return value;
  if (value instanceof Error) return value.message || fallback;
  if (typeof value === 'object') {
    const nested =
      value.message ||
      value.error_user_msg ||
      value.error_user_title ||
      value.details ||
      value.error ||
      value.title ||
      value.description;
    if (nested && nested !== value) return normalizeErrorMessage(nested, fallback);
    try {
      return JSON.stringify(value);
    } catch {
      return fallback;
    }
  }
  return String(value || fallback);
};

const requireInternalApiKey = (req, res, next) => {
  const expectedKeys = [
    process.env.ADMIN_INTERNAL_API_KEY,
    process.env.INTERNAL_API_KEY,
    process.env.WHATSAPP_BACKEND_INTERNAL_API_KEY
  ]
    .map((value) => trimOrNull(value))
    .filter(Boolean);
  const provided = trimOrNull(req.headers['x-internal-api-key']);
  if (expectedKeys.length === 0) {
    return res.status(503).json({
      success: false,
      error: 'Internal booking notification endpoint is not configured'
    });
  }
  if (!provided || !expectedKeys.includes(provided)) {
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

    const adminBackendConfig = getAdminBackendConfig();
    if (!adminBackendConfig.configured) {
      return res.status(503).json({
        success: false,
        error: `Admin backend credentials lookup is not configured (${adminBackendConfig.missing.join(', ')})`
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
    if (normalizedMessageType !== 'template') {
      return res.status(400).json({
        success: false,
        error: 'IVR WhatsApp notifications are template-only'
      });
    }

    const normalizedTemplateName = String(templateName || '').trim();
    if (!normalizedTemplateName) {
      return res.status(400).json({
        success: false,
        error: 'templateName is required for IVR WhatsApp notifications'
      });
    }

    const result = await whatsappService.sendTemplateMessage(
      recipient,
      normalizedTemplateName,
      language || 'en_US',
      Array.isArray(variables) ? variables : [],
      credentials,
      true,
      Array.isArray(templateComponents) && templateComponents.length > 0 ? templateComponents : components
    );

    if (!result.success) {
      return res.status(400).json({
        success: false,
        error: normalizeErrorMessage(result.error)
      });
    }

    return res.json({
      success: true,
      data: result.data || null,
      providerMessageId: result.providerMessageId || result.data?.messages?.[0]?.id || ''
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Internal IVR notification failed'
    });
  }
});

module.exports = router;
