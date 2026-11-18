const WHATSAPP_GRAPH_BASE_URL = 'https://graph.facebook.com';
const defaultGraphVersion = 'v20.0';

module.exports = {
  WHATSAPP_GRAPH_BASE_URL,
  WHATSAPP_API_URL: `${WHATSAPP_GRAPH_BASE_URL}/${defaultGraphVersion}`,
  WHATSAPP_TEMPLATE_CREATE_GRAPH_VERSION:
    (process.env.WHATSAPP_TEMPLATE_CREATE_GRAPH_VERSION || '').trim() || defaultGraphVersion,
  WEBHOOK_VERIFY_TOKEN: process.env.WEBHOOK_VERIFY_TOKEN || 'your_verify_token_here'
};
