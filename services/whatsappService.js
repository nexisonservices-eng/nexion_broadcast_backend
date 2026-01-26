const axios = require('axios');
const whatsappConfig = require('../config/whatsapp');

class WhatsAppService {
  constructor() {
    this.apiUrl = whatsappConfig.WHATSAPP_API_URL;
    this.wabaId = whatsappConfig.WHATSAPP_BUSINESS_ACCOUNT_ID;
    this.phoneNumberId = whatsappConfig.WHATSAPP_PHONE_NUMBER_ID;
    this.accessToken = whatsappConfig.WHATSAPP_ACCESS_TOKEN;
  }

  getHeaders() {
    return {
      'Authorization': `Bearer ${this.accessToken}`,
      'Content-Type': 'application/json'
    };
  }

  async sendTextMessage(to, text) {
    try {
      // WhatsApp API requires phone numbers in E.164 format with + prefix and no spaces/special chars
      let normalizedPhone = to;
      
      // Remove any non-digit characters except +
      normalizedPhone = normalizedPhone.replace(/[^\d+]/g, '');
      
      // Ensure it starts with +
      if (!normalizedPhone.startsWith('+')) {
        normalizedPhone = '+' + normalizedPhone;
      }
      
      console.log(`📞 Normalized phone number: ${to} -> ${normalizedPhone}`);
      
      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        {
          messaging_product: 'whatsapp',
          to: normalizedPhone,
          type: 'text',
          text: { body: text }
        },
        { headers: this.getHeaders() }
      );
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error sending text message:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }

  async sendTemplateMessage(to, templateName, language = 'en_US', variables = []) {
    try {
      // WhatsApp API requires phone numbers in E.164 format with + prefix and no spaces/special chars
      let normalizedPhone = to;
      
      // Remove any non-digit characters except +
      normalizedPhone = normalizedPhone.replace(/[^\d+]/g, '');
      
      // Ensure it starts with +
      if (!normalizedPhone.startsWith('+')) {
        normalizedPhone = '+' + normalizedPhone;
      }
      
      console.log(`📞 Normalized phone number: ${to} -> ${normalizedPhone}`);
      
      const components = [];
      
      if (variables.length > 0) {
        components.push({
          type: 'BODY',
          parameters: variables.map(varValue => ({
            type: 'text',
            text: String(varValue)
          }))
        });
        console.log(`🔍 Sending ${variables.length} variables:`, variables);
      } else {
        console.log(`🔍 No variables provided, sending empty components`);
      }

      const payload = {
        messaging_product: 'whatsapp',
        to: normalizedPhone,
        type: 'template',
        template: {
          name: templateName,
          language: { code: language },
          components: components
        }
      };

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );
      
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error sending template message:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }

  async sendMediaMessage(to, mediaType, mediaUrl, caption = '') {
    try {
      const payload = {
        messaging_product: 'whatsapp',
        to: to,
        type: mediaType,
        [mediaType]: {
          link: mediaUrl,
          caption: caption
        }
      };

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );
      
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error sending media message:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }

  async markMessageAsRead(messageId) {
    try {
      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        {
          messaging_product: 'whatsapp',
          status: 'read',
          message_id: messageId
        },
        { headers: this.getHeaders() }
      );
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error marking message as read:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }

  async getTemplateList() {
    try {
      if (!this.wabaId) {
        return {
          success: false,
          error: 'WHATSAPP_BUSINESS_ACCOUNT_ID (WABA ID) is not configured'
        };
      }
      console.log(`Fetching templates from: ${this.apiUrl}/${this.wabaId}/message_templates`);
      const response = await axios.get(
        `${this.apiUrl}/${this.wabaId}/message_templates`,
        { headers: this.getHeaders() }
      );
      console.log('Raw Meta API Response:', JSON.stringify(response.data, null, 2));
      console.log('Templates found:', response.data.data?.length || 0);
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error fetching templates:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }
}

module.exports = new WhatsAppService();
