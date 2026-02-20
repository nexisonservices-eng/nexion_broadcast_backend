const axios = require('axios');

class WhatsAppService {
  constructor() {
    this.apiUrl = 'https://graph.facebook.com/v20.0';
    this.mockMode = process.env.WHATSAPP_MOCK_MODE === 'true';
  }

  // Initialize service with user-specific credentials
  initialize(credentials) {
    this.wabaId = credentials.businessAccountId;
    this.phoneNumberId = credentials.phoneNumberId;
    this.accessToken = credentials.accessToken;
    this.webhookVerifyToken = credentials.webhookVerifyToken;
  }

  getHeaders() {
    if (!this.accessToken) {
      throw new Error('WhatsApp service not initialized with credentials');
    }
    return {
      'Authorization': `Bearer ${this.accessToken}`,
      'Content-Type': 'application/json'
    };
  }

  async sendTextMessage(to, text, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      console.log(`🧪 MOCK MODE: Simulating text message to ${to}: "${text}"`);
      return { 
        success: true, 
        data: { 
          messageId: 'mock_' + Date.now(),
          status: 'sent'
        } 
      };
    }

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

  async sendTemplateMessage(to, templateName, language = 'en_US', variables = [], credentials = null, retryOnNotFound = true) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      console.log(`🧪 MOCK MODE: Simulating template message to ${to}`);
      console.log(`   Template: ${templateName}`);
      console.log(`   Language: ${language}`);
      console.log(`   Variables:`, variables);
      return { 
        success: true, 
        data: { 
          messageId: 'mock_template_' + Date.now(),
          status: 'sent',
          template: templateName
        } 
      };
    }

    try {
      const normalizedTemplateName = String(templateName || '').trim();
      if (!normalizedTemplateName) {
        return { success: false, error: 'Template name is required' };
      }

      console.log(`🔍 DEBUG: sendTemplateMessage called with:`);
      console.log(`   to: ${to}`);
      console.log(`   templateName: "${normalizedTemplateName}"`);
      console.log(`   language: ${language}`);
      console.log(`   variables:`, variables);
      
      // WhatsApp API requires phone numbers in E.164 format with + prefix and no spaces/special chars
      let normalizedPhone = to;
      
      // Remove any non-digit characters except +
      normalizedPhone = normalizedPhone.replace(/[^\d+]/g, '');
      
      // Ensure it starts with +
      if (!normalizedPhone.startsWith('+')) {
        normalizedPhone = '+' + normalizedPhone;
      }
      
      console.log(`📞 Normalized phone number: ${to} -> ${normalizedPhone}`);
      console.log(`🔍 Template details: name=${normalizedTemplateName}, language=${language}, variables=${variables.length}`);
      
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
          name: normalizedTemplateName,
          language: { code: language }
        }
      };

      if (components.length > 0) {
        payload.template.components = components;
      }

      console.log(`📤 Sending template payload:`, JSON.stringify(payload, null, 2));

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );
      
      console.log(`✅ Template sent successfully to ${normalizedPhone}:`, response.data);
      return { success: true, data: response.data };
    } catch (error) {
      const phoneNumberForLog = to || 'unknown';
      console.error(`❌ Error sending template "${templateName}" to ${phoneNumberForLog}:`, {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        message: error.message
      });
      
      // Log specific WhatsApp Business API errors
      if (error.response?.data) {
        const whatsappError = error.response.data;
        if (whatsappError.error) {
          console.error(`🔍 WhatsApp API Error Code: ${whatsappError.error.code}`);
          console.error(`🔍 WhatsApp API Error Message: ${whatsappError.error.message}`);
          console.error(`🔍 WhatsApp API Error Title: ${whatsappError.error.title}`);
          
          // Check for common template restrictions
          if (whatsappError.error.code === 131051) {
            console.error(`🚫 Template restriction: Template not approved for this number`);
          } else if (whatsappError.error.code === 131047) {
            console.error(`🚫 Template restriction: Number not in approved conversation window`);
          }
        }
      }
      
      const metaMessage =
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        '';

      const shouldRetryOnLanguage =
        /language/i.test(metaMessage) &&
        (/translation/i.test(metaMessage) || /does not exist/i.test(metaMessage));

      if (retryOnNotFound && /template .* not found/i.test(metaMessage)) {
        try {
          const listResult = await this.getTemplateList(credentials || null);
          if (listResult.success) {
            const templates = listResult.data?.data || [];
            const normalizedRequested = normalizedTemplateName.toLowerCase();
            const exactMatch = templates.find(
              (t) => String(t.name || '').trim().toLowerCase() === normalizedRequested
            );
            if (exactMatch && exactMatch.name && exactMatch.name !== normalizedTemplateName) {
              console.log(`🔁 Retrying template send with Meta-canonical name: ${exactMatch.name}`);
              return this.sendTemplateMessage(
                to,
                exactMatch.name,
                language,
                variables,
                credentials,
                false
              );
            }
          }
        } catch (retryError) {
          console.error('Template retry resolution failed:', retryError.message);
        }
      }

      if (retryOnNotFound && shouldRetryOnLanguage) {
        try {
          const listResult = await this.getTemplateList(credentials || null);
          if (listResult.success) {
            const templates = listResult.data?.data || [];
            const sameName = templates.filter(
              (t) =>
                String(t.name || '').trim().toLowerCase() ===
                String(normalizedTemplateName || '').trim().toLowerCase()
            );
            const candidate = sameName.find(
              (t) =>
                String(t.language || '').trim().toLowerCase() !==
                String(language || '').trim().toLowerCase()
            );
            if (candidate?.language) {
              console.log(
                `🔁 Retrying template send with Meta-available language: ${candidate.language}`
              );
              return this.sendTemplateMessage(
                to,
                normalizedTemplateName,
                candidate.language,
                variables,
                credentials,
                false
              );
            }
          }
        } catch (retryError) {
          console.error('Template language retry resolution failed:', retryError.message);
        }
      }

      let normalizedError = metaMessage;
      if (!normalizedError) {
        const rawError = error.response?.data || error.message;
        if (typeof rawError === 'string') {
          normalizedError = rawError;
        } else {
          try {
            normalizedError = JSON.stringify(rawError);
          } catch (stringifyError) {
            normalizedError = 'Unknown WhatsApp API error';
          }
        }
      }

      return {
        success: false,
        error: normalizedError
      };
    }
  }

  async sendMediaMessage(to, mediaType, mediaUrl, caption = '', credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
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

  async markMessageAsRead(messageId, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
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

  async createTemplate(templateData, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      console.log(`🧪 MOCK MODE: Simulating template creation:`, templateData);
      return { 
        success: true, 
        data: { 
          id: 'mock_template_' + Date.now(),
          name: templateData.name,
          status: 'PENDING'
        } 
      };
    }

    try {
      console.log(`📝 Creating template: ${templateData.name}`);
      
      // Meta API format for template creation
      const metaTemplateData = {
        name: templateData.name,
        category: templateData.category,
        language: templateData.language,
        components: templateData.components || []
      };

      const response = await axios.post(
        `${this.apiUrl}/${this.wabaId}/message_templates`,
        metaTemplateData,
        { headers: this.getHeaders() }
      );
      
      console.log('✅ Template created successfully:', response.data);
      return { 
        success: true, 
        data: response.data 
      };
    } catch (error) {
      console.error('❌ Failed to create template:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data?.error?.message || error.message 
      };
    }
  }

  async getTemplateList(credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
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
        {
          headers: this.getHeaders(),
          params: {
            fields: 'id,name,language,status,category,components',
            limit: 200
          }
        }
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

  async deleteTemplateByName(templateName, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }

    try {
      const normalizedName = String(templateName || '').trim();
      if (!normalizedName) {
        return { success: false, error: 'Template name is required' };
      }

      const response = await axios.delete(
        `${this.apiUrl}/${this.wabaId}/message_templates`,
        {
          headers: this.getHeaders(),
          params: { name: normalizedName }
        }
      );

      return { success: true, data: response.data };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.error?.message || error.message
      };
    }
  }

}

module.exports = new WhatsAppService();
