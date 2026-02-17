const express = require('express');
const axios = require('axios');
const csv = require('csv-parser');
const { Readable } = require('stream');
const whatsappService = require('../services/whatsappService');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const Broadcast = require('../models/Broadcast');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');

const router = express.Router();
router.use(auth);
console.log('[BULK_ROUTE_VERSION] bulk_direct_meta_v2 loaded');

const WHATSAPP_API_URL = 'https://graph.facebook.com/v20.0';

async function sendTemplateDirectViaMeta({ phone, templateName, language, variables, credentials }) {
  const sanitizedCredentials = {
    accessToken: String(credentials?.accessToken || '').trim(),
    phoneNumberId: String(credentials?.phoneNumberId || '').trim()
  };
  if (!sanitizedCredentials.accessToken || !sanitizedCredentials.phoneNumberId) {
    return { success: false, error: 'Invalid WhatsApp credentials for template send' };
  }

  const normalizedPhone = String(phone || '').replace(/[^\d+]/g, '').startsWith('+')
    ? String(phone || '').replace(/[^\d+]/g, '')
    : `+${String(phone || '').replace(/[^\d+]/g, '')}`;

  const payload = {
    messaging_product: 'whatsapp',
    to: normalizedPhone,
    type: 'template',
    template: {
      name: String(templateName || '').trim(),
      language: { code: language || 'en_US' }
    }
  };

  if (Array.isArray(variables) && variables.length > 0) {
    payload.template.components = [
      {
        type: 'BODY',
        parameters: variables.map((value) => ({ type: 'text', text: String(value) }))
      }
    ];
  }

  try {
    const response = await axios.post(
      `${WHATSAPP_API_URL}/${sanitizedCredentials.phoneNumberId}/messages`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${sanitizedCredentials.accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    return { success: true, data: response.data };
  } catch (error) {
    return {
      success: false,
      error:
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        error.response?.data ||
        error.message
    };
  }
}

async function resolveTemplatePreviewTextFromMeta({ templateName, language, credentials }) {
  try {
    const listResult = await whatsappService.getTemplateList(credentials || null);
    if (!listResult?.success) return null;

    const templates = listResult?.data?.data || [];
    const requested = String(templateName || '').trim().toLowerCase();
    const requestedLanguage = String(language || '').trim().toLowerCase();

    const matchedTemplate =
      templates.find((t) => String(t.name || '').trim().toLowerCase() === requested && String(t.language || '').trim().toLowerCase() === requestedLanguage) ||
      templates.find((t) => String(t.name || '').trim().toLowerCase() === requested);

    if (!matchedTemplate || !Array.isArray(matchedTemplate.components)) {
      return null;
    }

    const bodyComponent = matchedTemplate.components.find((component) => String(component.type || '').toUpperCase() === 'BODY');
    const bodyText =
      bodyComponent?.text ||
      bodyComponent?.body_text ||
      (Array.isArray(bodyComponent?.example?.body_text) ? bodyComponent.example.body_text[0] : '');
    if (!bodyText) return null;

    return String(bodyText);
  } catch (error) {
    console.error('Failed to resolve template preview text from Meta:', error.message);
    return null;
  }
}

// Process template variables - supports unlimited named variables - matching Python reference
function processTemplateVariables(templateContent, variables, rowData = {}) {
  let processedContent = templateContent;
  
  console.log(`🔧 Processing template: "${templateContent}" with rowData:`, rowData);
  
  // Support named variables from CSV columns like {name}, {email}, {city}, etc.
  if (rowData && typeof rowData === 'object') {
    Object.keys(rowData).forEach(columnName => {
      if (columnName !== 'phone') { // Don't replace phone column
        const placeholder = new RegExp(`\\{${columnName}\\}`, 'g');
        const beforeReplace = processedContent;
        processedContent = processedContent.replace(placeholder, rowData[columnName] || '');
        if (beforeReplace !== processedContent) {
          console.log(`✅ Replaced {${columnName}} with "${rowData[columnName] || ''}"`);
        }
      }
    });
  }
  
  // Support both {{1}} and {var1} formats for backward compatibility - matching Python reference
  variables.forEach((varValue, index) => {
    // Support {{1}} format
    const placeholder1 = new RegExp(`\\{\\{${index + 1}\\}\\}`, 'g');
    const beforeReplace1 = processedContent;
    processedContent = processedContent.replace(placeholder1, varValue || '');
    if (beforeReplace1 !== processedContent) {
      console.log(`✅ Replaced {{${index + 1}}} with "${varValue || ''}"`);
    }
    
    // Support {var1} format
    const placeholder2 = new RegExp(`\\{var${index + 1}\\}`, 'g');
    const beforeReplace2 = processedContent;
    processedContent = processedContent.replace(placeholder2, varValue || '');
    if (beforeReplace2 !== processedContent) {
      console.log(`✅ Replaced {var${index + 1}} with "${varValue || ''}"`);
    }
  });
  
  console.log(`📝 Final processed message: "${processedContent}"`);
  return processedContent;
}

// Rate limiting function
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Parse CSV data with enhanced processing - supports unlimited columns and proper CSV parsing
function parseCSV(csvData, hasHeaders) {
  return new Promise((resolve, reject) => {
    const results = [];
    
    // Use proper CSV parsing to handle quoted values
    const parseLine = (line) => {
      const result = [];
      let current = '';
      let inQuotes = false;
      
      for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
          result.push(current.trim());
          current = '';
        } else {
          current += char;
        }
      }
      
      // Add the last value (important for the last column)
      result.push(current.trim());
      return result;
    };
    
    const lines = csvData.split('\n').filter(line => line.trim());
    
    if (hasHeaders && lines.length > 0) {
      const headers = parseLine(lines[0]);
      const dataLines = lines.slice(1);
      
      // First column should be phone, all other columns are available as variables
      const phoneColumn = headers[0]; // First column is phone
      
      dataLines.forEach(line => {
        const values = parseLine(line);
        const row = {};
        
        // Create row object with all column data
        headers.forEach((header, index) => {
          row[header] = values[index] || '';
        });
        
        // Extract phone number from first column
        const phone = row[phoneColumn] || '';
        if (phone) {
          // Extract variables from all columns except phone (for backward compatibility)
          const variables = [];
          headers.slice(1).forEach(header => {
            if (row[header]) {
              variables.push(row[header]);
            }
          });
          
          results.push({
            phone: phone.trim(),
            variables: variables,
            data: row // Keep full row data for named variable replacement
          });
        }
      });
    } else {
      // CSV with only phone numbers, no headers
      lines.forEach(line => {
        const values = parseLine(line);
        const phone = values[0] ? values[0].trim() : '';
        if (phone) {
          results.push({
            phone: phone,
            variables: [],
            data: { phone: phone }
          });
        }
      });
    }
    
    resolve(results);
  });
}

// Upload CSV endpoint - handle CSV as base64 string
router.post('/upload', async (req, res) => {
  try {
    const { csvData } = req.body;
    
    if (!csvData) {
      return res.status(400).json({
        success: false,
        message: 'CSV data is required'
      });
    }

    // Decode base64 CSV data
    const csvText = Buffer.from(csvData, 'base64').toString('utf8');
    
    // Check if CSV has headers - enhanced detection
    const lines = csvText.split('\n').filter(line => line.trim());
    const firstRow = lines[0] || '';
    
    // Smart header detection: check if first row contains letters and not just numbers/phone chars
    const hasHeaders = /[a-zA-Z]/.test(firstRow) && !/^[\d+\-\s()]+$/.test(firstRow);
    
    // Parse CSV
    const recipients = await parseCSV(csvText, hasHeaders);
    
    if (recipients.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No valid recipients found in CSV'
      });
    }

    res.json({
      success: true,
      recipients: recipients,
      count: recipients.length,
      hasHeaders,
      // Include full CSV data for debugging/display
      csvData: recipients.map(r => ({
        phone: r.phone,
        variables: r.variables,
        fullData: r.data // All CSV columns including non-var ones
      }))
    });
  } catch (error) {
    console.error('CSV upload error:', error);
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

// Bulk send endpoint with rate limiting and enhanced processing
router.post('/send', requireWhatsAppCredentials, async (req, res) => {
  try {
    console.log('📥 Received bulk send request:', req.body);
    const { message_type, template_name, language, custom_message, broadcast_name, recipients, messageType, customMessage, templateName, templateContent } = req.body;
    
    // Support both camelCase and snake_case parameter names
    const msgType = message_type || messageType || (templateName ? 'template' : 'text');
    const rawTemplateName = template_name || templateName;
    const finalTemplateName = rawTemplateName
      ? String(rawTemplateName).trim().toLowerCase()
      : rawTemplateName;
    const customMsg = custom_message || customMessage;
    
    console.log('📋 Message type:', msgType);
    console.log('📋 Custom message:', customMsg);
    console.log('📋 Recipients count:', recipients?.length || 0);
    
    // Support both direct recipients data and CSV file upload
    let parsedRecipients = [];
    
    if (recipients && Array.isArray(recipients)) {
      // Use provided recipients data (from frontend)
      parsedRecipients = recipients;
      console.log('📊 Using provided recipients data, first one:', recipients[0]);
    } else if (req.files && req.files.csv_file) {
      // Legacy CSV file upload support
      const csvFile = req.files.csv_file;
      const csvData = csvFile.data.toString('utf8');
      
      // Check if CSV has headers - enhanced detection
      const lines = csvData.split('\n').filter(line => line.trim());
      const firstRow = lines[0] || '';
      
      // Smart header detection: check if first row contains letters and not just numbers/phone chars
      const hasHeaders = /[a-zA-Z]/.test(firstRow) && !/^[\d+\-\s()]+$/.test(firstRow);
      
      // Parse CSV with enhanced processing
      parsedRecipients = await parseCSV(csvData, hasHeaders);
    } else {
      return res.status(400).json({
        success: false,
        message: 'Recipients data or CSV file is required'
      });
    }
    
    if (parsedRecipients.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No valid recipients found'
      });
    }

    const results = [];
    let successful = 0;
    let failed = 0;
    let templatePreviewText = String(templateContent || '').trim() || null;

    if (msgType === 'template' && finalTemplateName) {
      if (!templatePreviewText) {
        templatePreviewText = await resolveTemplatePreviewTextFromMeta({
          templateName: finalTemplateName,
          language: language || 'en_US',
          credentials: req.whatsappCredentials
        });
      }
    }

    // Create broadcast record (persist resolved template body for history/inbox context)
    const broadcast = await Broadcast.create({
      name: broadcast_name || `Bulk Send - ${new Date().toISOString()}`,
      message: customMsg || '',
      templateName: finalTemplateName || null,
      templateContent: templatePreviewText || '',
      language: language || 'en_US',
      recipients: parsedRecipients.map(r => ({ phone: r.phone, variables: r.variables || [] })),
      status: 'sending',
      startedAt: new Date(),
      createdBy: req.user.username || req.user.email || req.user.id,
      createdByEmail: req.user.email,
      createdById: req.user.id,
      messageType: msgType // Add message type to broadcast record
    });

    console.log(`🚀 Starting bulk send to ${parsedRecipients.length} recipients`);
    console.log(`📋 First recipient data sample:`, parsedRecipients[0]);

    for (let i = 0; i < parsedRecipients.length; i++) {
      const recipient = parsedRecipients[i];
      const phone = recipient.phone;
      // Extract variables from multiple sources to ensure we get them
      let variables = recipient.variables || [];
      const rowData = recipient.data || recipient.fullData || {};
      
      // If variables array is empty but rowData has var1, var2, etc., extract them
      if (variables.length === 0 && rowData) {
        Object.keys(rowData).forEach(key => {
          if (key.startsWith('var') && rowData[key]) {
            const varNum = key.replace('var', '');
            variables[parseInt(varNum) - 1] = rowData[key];
          }
        });
        // Filter out empty values and reindex
        variables = variables.filter(v => v !== undefined && v !== null && v !== '');
      }
      
      console.log(`📊 Processing recipient ${phone}:`, { rowData, variables });
      
      let result;
      let messageTextForInbox = '';

      try {
        console.log(`📤 About to send message to ${phone} with type: ${msgType}`);

        if (msgType === 'template' && finalTemplateName) {
          console.log(`Sending template directly via Meta API: ${finalTemplateName} -> ${phone}`);
          messageTextForInbox = templatePreviewText
            ? processTemplateVariables(templatePreviewText, variables, rowData)
            : `Template: ${finalTemplateName}`;
          result = await sendTemplateDirectViaMeta({
            phone,
            templateName: finalTemplateName,
            language: language || 'en_US',
            variables,
            credentials: req.whatsappCredentials
          });
          console.log(`Template send result for ${phone}:`, result);
        } else if (customMsg) {
          // Replace variables in custom message using enhanced processing with named variables
          messageTextForInbox = processTemplateVariables(customMsg, variables, rowData);
          console.log(`📤 Sending processed custom message: "${messageTextForInbox}"`);
          result = await whatsappService.sendTextMessage(phone, messageTextForInbox, req.whatsappCredentials);
          console.log(`📤 WhatsApp service result:`, result);
        } else {
          console.log(`❌ Missing template_name or custom_message. Template: ${finalTemplateName}, Custom: ${customMsg}`);
          results.push({
            phone,
            success: false,
            error: 'Either template_name or custom_message is required'
          });
          failed++;
          continue;
        }

        if (result.success) {
          successful++;
          broadcast.stats.sent++;

          // Create/Update contact + conversation + message so Team Inbox shows it
          let contact = await Contact.findOne({ userId: req.user.id, phone });
          if (!contact) {
            contact = await Contact.create({ userId: req.user.id, phone, name: '', lastContact: new Date() });
          }

          let conversation = await Conversation.findOne({
            userId: req.user.id,
            contactPhone: phone,
            status: { $in: ['active', 'pending'] }
          });
          if (!conversation) {
            conversation = await Conversation.create({
              userId: req.user.id,
              contactId: contact._id,
              contactPhone: phone,
              contactName: contact.name,
              lastMessageTime: new Date(),
              lastMessage: messageTextForInbox,
              lastMessageFrom: 'agent',
              unreadCount: 0
            });
          } else {
            conversation.lastMessageTime = new Date();
            conversation.lastMessage = messageTextForInbox;
            conversation.lastMessageFrom = 'agent';
            await conversation.save();
          }

          const whatsappMessageId = result.data?.messages?.[0]?.id;
          console.log(`🔍 DEBUG: About to create message with text: "${messageTextForInbox}"`);
          console.log(`🔍 DEBUG: messageTextForInbox type: ${typeof messageTextForInbox}`);
          console.log(`🔍 DEBUG: messageTextForInbox length: ${messageTextForInbox.length}`);
          
          const savedMessage = await Message.create({
            userId: req.user.id,
            conversationId: conversation._id,
            sender: 'agent',
            text: messageTextForInbox,
            status: 'sent',
            whatsappMessageId,
            broadcastId: broadcast._id
          });
          
          console.log(`🔍 DEBUG: Created message with ID: ${savedMessage._id} and text: "${savedMessage.text}"`);

          // Realtime push to all team members
          const sendToUser = req.app?.locals?.sendToUser;
          if (typeof sendToUser === 'function') {
            sendToUser(String(req.user.id), {
              type: 'message_sent',
              conversation: conversation.toObject(),
              message: savedMessage.toObject()
            });
          }
        } else {
          failed++;
          broadcast.stats.failed++;
        }

        results.push({
          phone,
          success: result.success,
          response: result.data || result.error
        });

        // Rate limiting - wait 1 second between messages to avoid WhatsApp API limits
        if (i < parsedRecipients.length - 1) { // Don't delay after the last message
          console.log(`⏳ Rate limiting: waiting 1 second before next message...`);
          await delay(1000);
        }
      } catch (error) {
        console.error(`❌ Error processing recipient ${phone}:`, error);
        failed++;
        broadcast.stats.failed++;
        results.push({
          phone,
          success: false,
          error: error.message
        });
        
        // Check if messageTextForInbox was set properly
        console.log(`🔍 DEBUG Error case - messageTextForInbox: "${messageTextForInbox}"`);
      }
    }

    broadcast.status = 'completed';
    broadcast.completedAt = new Date();
    await broadcast.save();

    console.log(`✅ Bulk send completed: ${successful} successful, ${failed} failed`);

    res.json({
      success: true,
      engine: 'bulk_direct_meta_v2',
      broadcastId: broadcast._id,
      total_sent: parsedRecipients.length,
      successful,
      failed,
      results
    });
  } catch (error) {
    console.error('Bulk send error:', error);
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

module.exports = router;

