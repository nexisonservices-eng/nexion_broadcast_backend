const express = require('express');
const csv = require('csv-parser');
const { Readable } = require('stream');
const whatsappService = require('../services/whatsappService');
const Template = require('../models/Template');
const Broadcast = require('../models/Broadcast');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');

const router = express.Router();

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
router.post('/send', async (req, res) => {
  try {
    console.log('📥 Received bulk send request:', req.body);
    const { message_type, template_name, language, custom_message, broadcast_name, recipients, messageType, customMessage, templateName } = req.body;
    
    // Support both camelCase and snake_case parameter names
    const msgType = message_type || messageType || (templateName ? 'template' : 'text');
    const finalTemplateName = template_name || templateName;
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

    // Create broadcast record
    const broadcast = await Broadcast.create({
      name: broadcast_name || `Bulk Send - ${new Date().toISOString()}`,
      message: customMsg || '',
      templateName: finalTemplateName || null,
      language: language || 'en_US',
      recipients: parsedRecipients.map(r => ({ phone: r.phone, variables: r.variables || [] })),
      status: 'sending',
      startedAt: new Date(),
      createdBy: req.body.createdBy || 'system',
      messageType: msgType // Add message type to broadcast record
    });

    const results = [];
    let successful = 0;
    let failed = 0;

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
          console.log(`🔍 DEBUG: Processing template ${finalTemplateName} for phone ${phone}`);
          
          // Fetch template from database to get actual content
          console.log(`🔍 Looking for template: ${finalTemplateName} (removing isActive filter for debugging)`);
          const template = await Template.findOne({ name: finalTemplateName });
          console.log(`🔍 Found template:`, template ? {
            name: template.name,
            category: template.category,
            status: template.status,
            isActive: template.isActive
          } : 'NOT FOUND');
          
          if (!template) {
            console.log(`❌ Template ${finalTemplateName} not found in database`);
            results.push({
              phone,
              success: false,
              error: `Template '${finalTemplateName}' not found`
            });
            failed++;
            continue;
          }
          
          // Check if template is active
          if (!template.isActive) {
            console.log(`❌ Template ${finalTemplateName} is not active (status: ${template.status})`);
            results.push({
              phone,
              success: false,
              error: `Template '${finalTemplateName}' is not active. Status: ${template.status}`
            });
            failed++;
            continue;
          }

          console.log(`✅ Template found: ${template.name}`);

          // Process template content for inbox display with named variables
          let templateContent = template.content.body || '';
          if (template.content.header && template.content.header.text) {
            templateContent = template.content.header.text + '\n' + templateContent;
          }
          if (template.content.footer) {
            templateContent = templateContent + '\n' + template.content.footer;
          }
          
          // Process the template content with named variables for both WhatsApp and inbox
          const processedTemplateContent = processTemplateVariables(templateContent, variables, rowData);
          messageTextForInbox = processedTemplateContent;
          
          console.log(`🔍 DEBUG: Template processing results:`);
          console.log(`   Raw template content: "${templateContent}"`);
          console.log(`   Variables:`, variables);
          console.log(`   RowData:`, rowData);
          console.log(`   Processed content: "${messageTextForInbox}"`);
          console.log(`   Processed content type: ${typeof messageTextForInbox}`);
          console.log(`   Processed content length: ${messageTextForInbox.length}`);
          
          // For template messages, we still need to send via WhatsApp template API
          // but we'll use the processed content for the inbox display
          // Check if template content has placeholders to determine if we need to send variables
          const hasPlaceholders = /\{\{\d+\}\}/.test(templateContent) || /\{var\d+\}/.test(templateContent);
          const templateVariables = hasPlaceholders ? variables : [];
          
          console.log(`🔍 Template "${finalTemplateName}" has placeholders: ${hasPlaceholders}, sending ${templateVariables.length} variables:`, templateVariables);
          
          result = await whatsappService.sendTemplateMessage(
            phone,
            finalTemplateName,
            language || 'en_US',
            templateVariables
          );
          
          console.log(`📤 WhatsApp API result for ${phone}:`, result);
        } else if (customMsg) {
          // Replace variables in custom message using enhanced processing with named variables
          messageTextForInbox = processTemplateVariables(customMsg, variables, rowData);
          console.log(`📤 Sending processed custom message: "${messageTextForInbox}"`);
          result = await whatsappService.sendTextMessage(phone, messageTextForInbox);
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
          let contact = await Contact.findOne({ phone });
          if (!contact) {
            contact = await Contact.create({ phone, name: '', lastContact: new Date() });
          }

          let conversation = await Conversation.findOne({ contactPhone: phone, status: { $in: ['active', 'pending'] } });
          if (!conversation) {
            conversation = await Conversation.create({
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
            conversationId: conversation._id,
            sender: 'agent',
            text: messageTextForInbox,
            status: 'sent',
            whatsappMessageId
          });
          
          console.log(`🔍 DEBUG: Created message with ID: ${savedMessage._id} and text: "${savedMessage.text}"`);

          // Realtime push to all team members
          const broadcaster = req.app?.locals?.broadcast;
          if (typeof broadcaster === 'function') {
            broadcaster({
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
