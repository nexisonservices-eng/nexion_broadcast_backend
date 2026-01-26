// Mock WhatsApp Service for testing when API token is expired
class WhatsAppServiceMock {
  constructor() {
    this.apiUrl = 'https://graph.facebook.com/v20.0';
    this.wabaId = process.env.WHATSAPP_BUSINESS_ACCOUNT_ID;
    this.phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
    this.accessToken = process.env.WHATSAPP_ACCESS_TOKEN;
  }

  async sendTextMessage(to, text) {
    console.log(`📤 [MOCK] Sending text message to ${to}: "${text}"`);
    
    // Simulate API behavior - accept more phone number formats
    if (!to || (typeof to === 'string' && to.trim().length < 10)) {
      return { 
        success: false, 
        error: { message: 'Invalid phone number format' }
      };
    }

    // Accept various phone number formats including Indian numbers
    const cleanPhone = to.replace(/[\s\-\(\)]/g, '');
    if (!cleanPhone.match(/^\+?\d{10,15}$/)) {
      console.log(`❌ Phone format check failed for: ${to} -> ${cleanPhone}`);
      return { 
        success: false, 
        error: { message: 'Invalid phone number format' }
      };
    }

    // Simulate successful send
    console.log(`✅ Phone format accepted: ${cleanPhone}`);
    return { 
      success: true, 
      data: {
        messages: [{
          id: `mock_msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          message_status: 'accepted'
        }]
      }
    };
  }

  async sendTemplateMessage(to, templateName, language = 'en_US', variables = []) {
    console.log(`📤 [MOCK] Sending template message to ${to}: "${templateName}"`);
    
    if (!to || !to.startsWith('+')) {
      return { 
        success: false, 
        error: { message: 'Invalid phone number format' }
      };
    }

    return { 
      success: true, 
      data: {
        messages: [{
          id: `mock_template_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          message_status: 'accepted'
        }]
      }
    };
  }

  async sendMediaMessage(to, mediaType, mediaUrl, caption = '') {
    console.log(`📤 [MOCK] Sending media message to ${to}: ${mediaType}`);
    
    return { 
      success: true, 
      data: {
        messages: [{
          id: `mock_media_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          message_status: 'accepted'
        }]
      }
    };
  }
}

module.exports = WhatsAppServiceMock;
