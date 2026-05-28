const axios = require('axios');

const trimSlashes = (value = '') => String(value || '').replace(/^\/+|\/+$/g, '');

const buildStatusError = (statusData = {}) => {
  const error = Array.isArray(statusData.errors) ? statusData.errors[0] || {} : {};
  return [
    error.message,
    error.error_data?.message,
    error.error_data?.details,
    error.title,
    error.code ? `code ${error.code}` : '',
    error.error_data?.code ? `code ${error.error_data.code}` : ''
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean)
    .join(' • ');
};

const getBridgeConfig = () => {
  const baseUrl = String(
    process.env.IVR_BACKEND_INTERNAL_URL ||
    process.env.VOICE_BACKEND_INTERNAL_URL ||
    'http://localhost:5000'
  ).trim();
  const apiKey = String(
    process.env.WHATSAPP_BACKEND_INTERNAL_API_KEY ||
    process.env.INTERNAL_API_KEY ||
    process.env.ADMIN_INTERNAL_API_KEY ||
    ''
  ).trim();
  const statusPath = String(
    process.env.IVR_BACKEND_INTERNAL_STATUS_PATH ||
    '/internal/ivr/notification-status'
  ).trim();

  return {
    enabled: Boolean(baseUrl && apiKey),
    url: `${baseUrl.replace(/\/+$/, '')}/${trimSlashes(statusPath)}`,
    apiKey
  };
};

const forwardIvrNotificationStatus = async (statusData = {}) => {
  const providerMessageId = String(statusData.id || '').trim();
  const status = String(statusData.status || '').trim().toLowerCase();
  if (!providerMessageId || !status) {
    return { success: false, skipped: true, error: 'Missing provider message id or status' };
  }

  const config = getBridgeConfig();
  if (!config.enabled) {
    return { success: false, skipped: true, error: 'IVR status bridge is not configured' };
  }

  try {
    const response = await axios.post(
      config.url,
      {
        providerMessageId,
        status,
        errorMessage: buildStatusError(statusData),
        raw: statusData
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-internal-api-key': config.apiKey
        },
        timeout: 10000
      }
    );

    return response.data || { success: true };
  } catch (error) {
    const statusCode = error?.response?.status;
    const responseError = error?.response?.data?.error || error?.response?.data?.message || error?.message;
    console.warn(`IVR notification status bridge failed: ${statusCode ? `${statusCode} ` : ''}${responseError}`);
    return { success: false, error: responseError || 'Failed to forward IVR notification status' };
  }
};

module.exports = {
  forwardIvrNotificationStatus
};
