const Broadcast = require('../models/Broadcast');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const whatsappService = require('./whatsappService');
const { getWhatsAppCredentialsForUser } = require('./userWhatsAppCredentialsService');
const axios = require('axios');

const ADMIN_USAGE_ENDPOINT =
  process.env.ADMIN_USAGE_ENDPOINT || '/internal/usage/record';
const ADMIN_API_BASE_URLS = [
  process.env.ADMIN_API_BASE_URL,
  process.env.ADMIN_BACKEND_URL,
  'http://localhost:8000',
  'http://localhost:5000'
]
  .map((url) => (url || '').trim())
  .filter(Boolean)
  .filter((url, index, arr) => arr.indexOf(url) === index);
const ADMIN_INTERNAL_API_KEY = process.env.ADMIN_INTERNAL_API_KEY || '';

class BroadcastService {
  async reportUsage(companyId, count = 1) {
    if (!companyId || !ADMIN_INTERNAL_API_KEY) return;
    for (const baseUrl of ADMIN_API_BASE_URLS) {
      try {
        await axios.post(
          `${baseUrl}${ADMIN_USAGE_ENDPOINT}`,
          {
            companyId,
            usageType: 'whatsapp_message',
            count
          },
          {
            headers: {
              'x-internal-api-key': ADMIN_INTERNAL_API_KEY
            },
            timeout: 10000
          }
        );
        return;
      } catch (error) {
        continue;
      }
    }
  }
  normalizePhoneNumber(phone) {
    return String(phone || '').replace(/\D/g, '');
  }

  getStatusScore(status) {
    const normalized = String(status || '').toLowerCase();
    if (normalized === 'read') return 4;
    if (normalized === 'delivered') return 3;
    if (normalized === 'sent') return 2;
    if (normalized === 'failed') return 1;
    return 0;
  }

  async buildRecipientStatusDetails(broadcast) {
    const recipients = Array.isArray(broadcast?.recipients) ? broadcast.recipients : [];
    const detailsByPhone = new Map();
    recipients.forEach((recipient) => {
      const rawPhone = recipient?.phone || recipient;
      const normalizedPhone = this.normalizePhoneNumber(rawPhone);
      if (!normalizedPhone) return;
      detailsByPhone.set(normalizedPhone, {
        phone: rawPhone,
        name: recipient?.name || '',
        sent: false,
        delivered: false,
        read: false,
        failed: false,
        replied: false,
        replyCount: 0,
        status: 'pending',
        lastSentAt: null,
        lastStatusAt: null,
        lastReplyAt: null,
        lastReplyText: ''
      });
    });

    const startTime = new Date(broadcast.startedAt || broadcast.createdAt || Date.now());
    const completedAt = new Date(broadcast.completedAt || startTime);
    const endTime = new Date(completedAt);
    endTime.setDate(endTime.getDate() + 1);

    let outboundMessages = await Message.find({
      userId: broadcast.createdById,
      companyId: broadcast.companyId,
      sender: 'agent',
      broadcastId: broadcast._id
    })
      .select('conversationId status timestamp text')
      .sort({ timestamp: 1 })
      .lean();

    // Legacy fallback: old records may not have broadcastId tagged.
    // In that case, find agent messages in recipient conversations during campaign window.
    if (outboundMessages.length === 0 && detailsByPhone.size > 0) {
      const recipientPhoneVariants = Array.from(detailsByPhone.keys());
      const recipientPhoneRegex = recipientPhoneVariants.map((phone) => new RegExp(`${phone}$`));
      const legacyConversations = await Conversation.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        $or: [
          { contactPhone: { $in: recipientPhoneVariants } },
          ...recipientPhoneRegex.map((phoneRegex) => ({ contactPhone: phoneRegex }))
        ]
      })
        .select('_id contactPhone contactName')
        .lean();

      const legacyConversationIds = legacyConversations.map((item) => item._id);
      if (legacyConversationIds.length > 0) {
        outboundMessages = await Message.find({
          userId: broadcast.createdById,
          companyId: broadcast.companyId,
          sender: 'agent',
          conversationId: { $in: legacyConversationIds },
          timestamp: { $gte: startTime, $lte: endTime }
        })
          .select('conversationId status timestamp text')
          .sort({ timestamp: 1 })
          .lean();
      }
    }

    const conversationIds = Array.from(
      new Set(outboundMessages.map((message) => String(message.conversationId || '')).filter(Boolean))
    );

    const conversations = conversationIds.length
      ? await Conversation.find({ _id: { $in: conversationIds }, companyId: broadcast.companyId })
          .select('_id contactPhone contactName')
          .lean()
      : [];

    const conversationPhoneMap = new Map();
    conversations.forEach((conversation) => {
      conversationPhoneMap.set(String(conversation._id), {
        normalizedPhone: this.normalizePhoneNumber(conversation.contactPhone),
        rawPhone: conversation.contactPhone,
        contactName: conversation.contactName || ''
      });
    });

    outboundMessages.forEach((message) => {
      const conversationEntry = conversationPhoneMap.get(String(message.conversationId || ''));
      const normalizedPhone = conversationEntry?.normalizedPhone;
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: conversationEntry?.rawPhone || normalizedPhone,
          name: conversationEntry?.contactName || '',
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: 'pending',
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: ''
        });
      }

      const detail = detailsByPhone.get(normalizedPhone);
      const status = String(message.status || 'sent').toLowerCase();
      const messageTime = message.timestamp ? new Date(message.timestamp) : null;

      detail.sent = true;
      if (!detail.lastSentAt || (messageTime && messageTime > new Date(detail.lastSentAt))) {
        detail.lastSentAt = messageTime;
      }

      if (!detail.name && conversationEntry?.contactName) {
        detail.name = conversationEntry.contactName;
      }
      if (!detail.phone && conversationEntry?.rawPhone) {
        detail.phone = conversationEntry.rawPhone;
      }

      if (status === 'delivered' || status === 'read') detail.delivered = true;
      if (status === 'read') detail.read = true;
      if (status === 'failed') detail.failed = true;

      const currentScore = this.getStatusScore(detail.status);
      const nextScore = this.getStatusScore(status);
      if (nextScore >= currentScore) {
        detail.status = status;
      }

      if (!detail.lastStatusAt || (messageTime && messageTime > new Date(detail.lastStatusAt))) {
        detail.lastStatusAt = messageTime;
      }
    });

    const firstSentTimeByConversation = new Map();
    outboundMessages.forEach((message) => {
      const key = String(message.conversationId || '');
      if (!key || !message.timestamp) return;
      const current = firstSentTimeByConversation.get(key);
      const candidate = new Date(message.timestamp);
      if (!current || candidate < current) {
        firstSentTimeByConversation.set(key, candidate);
      }
    });

    const incomingMessages = conversationIds.length
      ? await Message.find({
          userId: broadcast.createdById,
          companyId: broadcast.companyId,
          sender: 'contact',
          conversationId: { $in: conversationIds },
          timestamp: { $gte: startTime }
        })
          .select('conversationId text timestamp')
          .sort({ timestamp: 1 })
          .lean()
      : [];

    incomingMessages.forEach((message) => {
      const conversationId = String(message.conversationId || '');
      const conversationEntry = conversationPhoneMap.get(conversationId);
      const normalizedPhone = conversationEntry?.normalizedPhone;
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: conversationEntry?.rawPhone || normalizedPhone,
          name: conversationEntry?.contactName || '',
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: 'pending',
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: ''
        });
      }

      const firstSentTime = firstSentTimeByConversation.get(conversationId);
      const replyTime = message.timestamp ? new Date(message.timestamp) : null;
      if (firstSentTime && replyTime && replyTime < firstSentTime) return;

      const detail = detailsByPhone.get(normalizedPhone);
      detail.replied = true;
      detail.replyCount += 1;
      detail.lastReplyAt = replyTime || detail.lastReplyAt;
      detail.lastReplyText = String(message.text || '').trim();
    });

    // Final fallback: populate missing names from contacts table.
    const missingNamePhones = Array.from(detailsByPhone.entries())
      .filter(([, detail]) => !String(detail?.name || '').trim())
      .map(([normalizedPhone]) => normalizedPhone)
      .filter(Boolean);

    if (missingNamePhones.length > 0) {
      const contactPhoneRegex = missingNamePhones.map((phone) => new RegExp(`${phone}$`));
      const contacts = await Contact.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        $or: [
          { phone: { $in: missingNamePhones } },
          ...contactPhoneRegex.map((phoneRegex) => ({ phone: phoneRegex }))
        ]
      })
        .select('phone name')
        .lean();

      const contactNameByPhone = new Map();
      contacts.forEach((contact) => {
        const normalized = this.normalizePhoneNumber(contact?.phone);
        const name = String(contact?.name || '').trim();
        if (normalized && name && !contactNameByPhone.has(normalized)) {
          contactNameByPhone.set(normalized, name);
        }
      });

      missingNamePhones.forEach((normalizedPhone) => {
        const detail = detailsByPhone.get(normalizedPhone);
        if (!detail) return;
        const fallbackName = contactNameByPhone.get(normalizedPhone);
        if (fallbackName) {
          detail.name = fallbackName;
        }
      });
    }

    return Array.from(detailsByPhone.values()).map((detail) => ({
      ...detail,
      lastSentAt: detail.lastSentAt || null,
      lastStatusAt: detail.lastStatusAt || null,
      lastReplyAt: detail.lastReplyAt || null
    }));
  }

  async resolveCredentialsForBroadcast(broadcast, credentials = null) {
    if (credentials) return credentials;

    const authHeader = String(broadcast?.authHeaderSnapshot || '').trim();
    if (authHeader.startsWith('Bearer ')) {
      try {
        const fetched = await getWhatsAppCredentialsForUser({
          authHeader,
          userId: String(broadcast?.createdById || '')
        });
        if (fetched) {
          return fetched;
        }
      } catch (error) {
        console.error(`Failed to fetch admin credentials for scheduled broadcast ${broadcast?._id}:`, error.message);
      }
    }

    const snapshot = broadcast?.credentialsSnapshot || null;
    const accessToken = String(snapshot?.accessToken || snapshot?.whatsappToken || '').trim();
    const businessAccountId = String(snapshot?.businessAccountId || snapshot?.whatsappBusiness || '').trim();
    const phoneNumberId = String(snapshot?.phoneNumberId || snapshot?.whatsappId || '').trim();

    if (accessToken && businessAccountId && phoneNumberId) {
      return {
        accessToken,
        businessAccountId,
        phoneNumberId,
        whatsappToken: accessToken,
        whatsappBusiness: businessAccountId,
        whatsappId: phoneNumberId,
        twilioId: snapshot?.twilioId || null
      };
    }

    const envAccessToken = String(process.env.WHATSAPP_ACCESS_TOKEN || '').trim();
    const envBusinessAccountId = String(process.env.WHATSAPP_BUSINESS_ACCOUNT_ID || '').trim();
    const envPhoneNumberId = String(process.env.WHATSAPP_PHONE_NUMBER_ID || '').trim();

    if (envAccessToken && envBusinessAccountId && envPhoneNumberId) {
      return {
        accessToken: envAccessToken,
        businessAccountId: envBusinessAccountId,
        phoneNumberId: envPhoneNumberId,
        whatsappToken: envAccessToken,
        whatsappBusiness: envBusinessAccountId,
        whatsappId: envPhoneNumberId,
        twilioId: null
      };
    }

    return null;
  }

  async resolveTemplatePreviewTextFromMeta(templateName, language, credentials) {
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

  // Process template variables - matching Python reference format
  processTemplateVariables(templateContent, variables) {
    let processedContent = templateContent;
    
    // Support both {{1}} and {var1} formats like Python reference
    variables.forEach((varValue, index) => {
      // Replace {{1}} format
      const placeholder1 = new RegExp(`\\{\\{${index + 1}\\}\\}`, 'g');
      processedContent = processedContent.replace(placeholder1, varValue);
      
      // Replace {var1} format  
      const placeholder2 = new RegExp(`\\{var${index + 1}\\}`, 'g');
      processedContent = processedContent.replace(placeholder2, varValue);
    });
    
    return processedContent;
  }
  async createBroadcast(broadcastData) {
    try {
      // If scheduledAt is provided, set status to 'scheduled'
      if (broadcastData.scheduledAt) {
        broadcastData.status = 'scheduled';
        // Handle timezone properly - datetime-local comes without timezone info
        // Parse it as local time and preserve it exactly
        const scheduledDate = new Date(broadcastData.scheduledAt);
        
        // Check if the parsed date is valid
        if (isNaN(scheduledDate.getTime())) {
          return { success: false, error: 'Invalid scheduled time format' };
        }
        
        console.log('📅 Original scheduledAt input:', broadcastData.scheduledAt);
        console.log('📅 Parsed Date object:', scheduledDate);
        console.log('📅 Date string (local):', scheduledDate.toString());
        console.log('📅 Date string (UTC):', scheduledDate.toUTCString());
        console.log('🔧 Timezone offset minutes:', scheduledDate.getTimezoneOffset());
        
        // Store the date as-is without timezone manipulation
        // MongoDB will store it in UTC and the comparison will work correctly
        broadcastData.scheduledAt = scheduledDate;
      }
      
      const broadcast = await Broadcast.create(broadcastData);
      console.log('✅ Created broadcast with scheduledAt:', broadcast.scheduledAt);
      return { success: true, data: broadcast };
    } catch (error) {
      console.error('❌ Error creating broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async sendBroadcast(broadcastId, broadcaster, credentials = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      const resolvedCredentials = await this.resolveCredentialsForBroadcast(broadcast, credentials);

      if (!resolvedCredentials) {
        return { success: false, error: 'WhatsApp credentials are not configured for this user' };
      }

      console.log('🔍 Broadcast data being processed:', {
        _id: broadcast._id,
        name: broadcast.name,
        messageType: broadcast.messageType,
        templateName: broadcast.templateName,
        message: broadcast.message,
        language: broadcast.language,
        recipientsCount: broadcast.recipients?.length || 0
      });

      broadcast.status = 'sending';
      broadcast.startedAt = new Date();
      await broadcast.save();

      const results = [];
      let successful = 0;
      let failed = 0;
      let usageBatchCount = 0;
      const usageBatchSize = Number(process.env.BROADCAST_USAGE_BATCH || 50);
      let templatePreviewText = broadcast.templateContent || null;

      // If templateContent is not stored, try to resolve from Meta
      if (!templatePreviewText && broadcast.templateName) {
        templatePreviewText = await this.resolveTemplatePreviewTextFromMeta(
          broadcast.templateName,
          broadcast.language || 'en_US',
          resolvedCredentials
        );
      }

      for (const recipient of broadcast.recipients) {
        try {
          let result;
          const phoneNumber = recipient.phone || recipient;
          
          let messageTextForInbox = broadcast.message;
          if (broadcast.templateName) {
            const normalizedTemplateName = String(broadcast.templateName || '').trim();
            if (!normalizedTemplateName) {
              results.push({
                phone: phoneNumber,
                success: false,
                error: 'Template name is required'
              });
              failed++;
              broadcast.stats.failed++;
              continue;
            }

            result = await whatsappService.sendTemplateMessage(
              phoneNumber,
              normalizedTemplateName,
              broadcast.language || 'en_US',
              recipient.variables || broadcast.variables || [],
              resolvedCredentials
            );
            
            console.log(`📤 Template send result for ${phoneNumber}:`, {
              success: result.success,
              templateName: normalizedTemplateName,
              language: broadcast.language || 'en_US',
              variables: recipient.variables || broadcast.variables || [],
              error: result.error
            });
            
            messageTextForInbox = templatePreviewText
              ? this.processTemplateVariables(templatePreviewText, recipient.variables || broadcast.variables || [])
              : `Template: ${normalizedTemplateName}`;
          } else if (broadcast.message) {
            // Process custom message with variable replacement
            const processedMessage = this.processTemplateVariables(broadcast.message, recipient.variables || broadcast.variables || []);
            result = await whatsappService.sendTextMessage(phoneNumber, processedMessage, resolvedCredentials);
            messageTextForInbox = processedMessage;
          } else {
            results.push({
              phone: phoneNumber,
              success: false,
              error: 'No message or template specified'
            });
            failed++;
            broadcast.stats.failed++;
            continue;
          }

          if (result.success) {
            successful++;
            broadcast.stats.sent++;
            
            // Create or update conversation + create message so Team Inbox shows it
            const { conversation, message } = await this.updateConversation(
              phoneNumber,
              messageTextForInbox,
              result.data,
              broadcast._id,
              broadcast.createdById,
              broadcast.companyId
            );

            if (typeof broadcaster === 'function' && conversation && message) {
              broadcaster({
                type: 'message_sent',
                conversation: conversation.toObject(),
                message: message.toObject()
              });
            }
            usageBatchCount += 1;
            if (usageBatchCount >= usageBatchSize) {
              await this.reportUsage(broadcast.companyId, usageBatchCount);
              usageBatchCount = 0;
            }
          } else {
            failed++;
            broadcast.stats.failed++;
            console.error(`❌ Failed to send to ${phoneNumber}:`, result.error);
          }

          results.push({
            phone: phoneNumber,
            success: result.success,
            response: result.data || result.error
          });

          // Rate limiting - wait 1 second between messages
          await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (error) {
          failed++;
          broadcast.stats.failed++;
          results.push({
            phone: recipient.phone || recipient,
            success: false,
            error: error.message
          });
        }
      }

      broadcast.status = 'completed';
      broadcast.completedAt = new Date();
      await broadcast.save();

      if (usageBatchCount > 0) {
        await this.reportUsage(broadcast.companyId, usageBatchCount);
      }

      // Note: Don't sync stats immediately after completion
      // Stats will be updated in real-time via message status updates
      // This prevents premature 100% read rates for first broadcasts

      return {
        success: true,
        data: {
          engine: 'broadcast_direct_meta_v2',
          broadcast,
          results,
          stats: {
            total: broadcast.recipients.length,
            successful,
            failed
          }
        }
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async updateConversation(phone, message, whatsappResponse, broadcastId, userId, companyId) {
    try {
      let contact = await Contact.findOne({ userId, companyId, phone });
      if (!contact) {
        contact = await Contact.create({
          userId,
          companyId,
          phone,
          name: '',
          sourceType: 'incoming_message'
        });
      } else if (broadcastId && contact.sourceType !== 'incoming_message') {
        // If this contact is being used in broadcast message flow, mark source as message-origin.
        contact.sourceType = 'incoming_message';
        await contact.save();
      }

      let conversation = await Conversation.findOne({
        userId,
        companyId,
        contactPhone: phone,
        status: { $in: ['active', 'pending'] }
      });
      if (!conversation) {
        conversation = await Conversation.create({
          userId,
          companyId,
          contactId: contact._id,
          contactPhone: phone,
          contactName: contact.name,
          lastMessage: message,
          lastMessageTime: new Date(),
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent'
        });
      } else {
        conversation.lastMessage = message;
        conversation.lastMessageTime = new Date();
        conversation.lastMessageMediaType = '';
        conversation.lastMessageAttachmentName = '';
        conversation.lastMessageAttachmentPages = null;
        conversation.lastMessageFrom = 'agent';
        await conversation.save();
      }

      const whatsappMessageId = whatsappResponse?.messages?.[0]?.id;
      const savedMessage = await Message.create({
        userId,
        companyId,
        conversationId: conversation._id,
        sender: 'agent',
        text: message,
        whatsappMessageId,
        status: 'sent',
        ...(broadcastId ? { broadcastId } : {})
      });

      return { conversation, message: savedMessage };
    } catch (error) {
      console.error('Error updating conversation:', error);
      return { conversation: null, message: null };
    }
  }

  async getBroadcasts(filters = {}) {
    try {
      // Keep list endpoint lightweight for fast overview updates.
      const broadcasts = await Broadcast.find(filters)
        .select('name status scheduledAt startedAt completedAt createdAt updatedAt recipientCount stats messageType templateName language')
        .sort({ createdAt: -1 })
        .lean();
      return { success: true, data: broadcasts };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getBroadcastById(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }
      const recipientDetails = await this.buildRecipientStatusDetails(broadcast);
      const data = broadcast.toObject ? broadcast.toObject() : broadcast;
      data.recipientDetails = recipientDetails;
      return { success: true, data };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  // Sync broadcast stats from actual messages in team inbox
  async syncBroadcastStats(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      // Find all messages sent from this broadcast
      const startTime = new Date(broadcast.startedAt || broadcast.createdAt);
      const endTime = new Date(broadcast.completedAt || Date.now());

      const broadcastIdQuery = {
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        sender: 'agent',
        broadcastId: broadcast._id,
        timestamp: { $gte: startTime, $lte: endTime }
      };

      let messages = await Message.find(broadcastIdQuery);

      // Backward compatibility for older records without broadcastId
      if (messages.length === 0) {
        const recipientPhones = (broadcast.recipients || [])
          .map(r => r.phone || r)
          .filter(Boolean);

        const conversations = recipientPhones.length
          ? await Conversation.find({ userId: broadcast.createdById, contactPhone: { $in: recipientPhones } })
          : [];

        const conversationIds = conversations.map(c => c._id);

        if (conversationIds.length > 0) {
          let convoQuery = {
            userId: broadcast.createdById,
            sender: 'agent',
            conversationId: { $in: conversationIds },
            timestamp: { $gte: startTime, $lte: endTime }
          };

          messages = await Message.find(convoQuery);

          if (messages.length > 0) {
            await Message.updateMany(
              { _id: { $in: messages.map(m => m._id) }, broadcastId: { $exists: false } },
              { $set: { broadcastId: broadcast._id } }
            );
          }
        }

        // Intentionally avoid broad text-based legacy fallback queries:
        // they can cross-match unrelated broadcasts and cause stat drops/fluctuations.
      }

      console.log(`?? Found ${messages.length} messages for broadcast "${broadcast.name}"`);

      // Count statuses with proper validation
      const stats = {
        sent: messages.length,
        // In WhatsApp, "read" implies message was delivered.
        delivered: messages.filter(msg => msg.status === 'delivered' || msg.status === 'read').length,
        read: messages.filter(msg => msg.status === 'read').length,
        failed: messages.filter(msg => msg.status === 'failed').length,
        replied: 0 // Will be calculated below
      };

      // Debug: Log message statuses to identify issues
      console.log('🔍 DEBUG: Message statuses found:', {
        totalMessages: messages.length,
        statusBreakdown: {
          sent: messages.filter(msg => msg.status === 'sent').length,
          delivered: messages.filter(msg => msg.status === 'delivered' || msg.status === 'read').length,
          read: messages.filter(msg => msg.status === 'read').length,
          failed: messages.filter(msg => msg.status === 'failed').length,
          other: messages.filter(msg => !['sent', 'delivered', 'read', 'failed'].includes(msg.status)).length
        },
        messageDetails: messages.map(m => ({
          id: m._id,
          whatsappId: m.whatsappMessageId,
          status: m.status,
          timestamp: m.timestamp
        }))
      });


      // Count unique contacts who replied to this broadcast
      const conversationIds = Array.from(new Set(messages.map(m => String(m.conversationId))));
      const replyStartTime = messages.length > 0
        ? new Date(Math.min(...messages.map(m => new Date(m.timestamp).getTime())))
        : startTime;

      const replyMessages = await Message.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        conversationId: { $in: conversationIds },
        sender: 'contact',
        timestamp: { $gte: replyStartTime }
      });

      // Count unique conversations that have at least one reply
      const uniqueRepliedConversations = new Set(replyMessages.map(msg => msg.conversationId.toString()));
      stats.replied = uniqueRepliedConversations.size;

      console.log('📊 Message status breakdown:', {
        total: messages.length,
        statuses: messages.map(m => ({ id: m._id, status: m.status })),
        calculatedStats: stats
      });

      // Only update broadcast stats if they're different
      const currentStats = broadcast.stats || {};
      const statsChanged = 
        currentStats.sent !== stats.sent ||
        currentStats.delivered !== stats.delivered ||
        currentStats.read !== stats.read ||
        currentStats.failed !== stats.failed ||
        currentStats.replied !== stats.replied;

      if (statsChanged) {
        // Update only the stats field without touching other fields
        await Broadcast.updateOne(
          { _id: broadcastId },
          { $set: { stats: stats, updatedAt: new Date() } }
        );
        
        // Get updated broadcast with virtual fields
        const updatedBroadcast = await Broadcast.findById(broadcastId);
        const repliedPercentage = updatedBroadcast.repliedPercentage;
        const repliedPercentageOfTotal = updatedBroadcast.repliedPercentageOfTotal;
        const readPercentage = updatedBroadcast.readPercentage;
        const readPercentageOfTotal = updatedBroadcast.readPercentageOfTotal;
        const deliveryRate = updatedBroadcast.deliveryRate;
        
        console.log(`📊 Updated stats for broadcast ${broadcast.name}:`, stats);
        console.log(`📊 Delivery rate: ${deliveryRate}%, Read rate: ${readPercentage}% of sent, ${readPercentageOfTotal}% of total recipients`);
        console.log(`📊 Replied percentage: ${repliedPercentage}% of sent, ${repliedPercentageOfTotal}% of total recipients`);
      } else {
        console.log(`📊 No stat changes for broadcast ${broadcast.name}`);
      }

      return { success: true, data: { broadcast, stats, messagesFound: messages.length } };
    } catch (error) {
      console.error('Error syncing broadcast stats:', error);
      return { success: false, error: error.message };
    }
  }

  // Check for scheduled broadcasts that need to be sent
  async checkScheduledBroadcasts() {
    try {
      const now = new Date();
      console.log('🔍 Checking for scheduled broadcasts at:', now.toString());
      console.log('🔍 Current time (UTC):', now.toUTCString());
      console.log('🔍 Current time (ISO):', now.toISOString());
      console.log('🔍 Current time (local):', now.toLocaleString());
      console.log('🔍 Current timestamp (ms):', now.getTime());
      
      // First, let's see all scheduled broadcasts
      const allScheduled = await Broadcast.find({ status: 'scheduled' }).maxTimeMS(5000);
      console.log(`📋 Total scheduled broadcasts in database: ${allScheduled.length}`);
      
      allScheduled.forEach((broadcast, index) => {
        const scheduledTime = new Date(broadcast.scheduledAt);
        console.log(`📋 Scheduled ${index + 1}: ${broadcast.name}`);
        console.log(`   Scheduled time (stored): ${broadcast.scheduledAt}`);
        console.log(`   Scheduled time (parsed): ${scheduledTime.toString()}`);
        console.log(`   Scheduled time (UTC): ${scheduledTime.toUTCString()}`);
        console.log(`   Scheduled timestamp (ms): ${scheduledTime.getTime()}`);
        console.log(`   Time difference (ms): ${scheduledTime.getTime() - now.getTime()}`);
        console.log(`   Should send: ${scheduledTime <= now}`);
        console.log('---');
      });
      
      // Use direct Date comparison - MongoDB stores dates in UTC
      const scheduledBroadcasts = await Broadcast.find({
        status: 'scheduled',
        scheduledAt: { $lte: now }
      }).maxTimeMS(5000);

      console.log(`📋 Found ${scheduledBroadcasts.length} scheduled broadcasts ready to send`);

      for (const broadcast of scheduledBroadcasts) {
        console.log(`⏰ Processing scheduled broadcast: ${broadcast.name}`);
        console.log(`📅 Scheduled time: ${broadcast.scheduledAt}`);
        console.log(`📅 Current time: ${now}`);
        
        try {
          const result = await this.sendBroadcast(broadcast._id);
          if (result?.success) {
            console.log(`✅ Successfully sent scheduled broadcast: ${broadcast.name}`);
          } else {
            console.error(`❌ Scheduled broadcast failed: ${broadcast.name}: ${result?.error || 'Unknown error'}`);
          }
        } catch (error) {
          console.error(`❌ Failed to send scheduled broadcast ${broadcast.name}:`, error);
        }
      }
      
      if (scheduledBroadcasts.length === 0) {
        console.log('💤 No scheduled broadcasts ready to send');
      }
    } catch (error) {
      console.error('❌ Error checking scheduled broadcasts:', error);
    }
  }

  async pauseBroadcast(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (broadcast.status !== 'scheduled') {
        return { success: false, error: 'Only scheduled broadcasts can be paused' };
      }

      broadcast.status = 'paused';
      await broadcast.save();

      return { success: true, data: broadcast };
    } catch (error) {
      console.error('Error pausing broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async resumeBroadcast(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (broadcast.status !== 'paused') {
        return { success: false, error: 'Only paused broadcasts can be resumed' };
      }

      const now = new Date();
      if (broadcast.scheduledAt && broadcast.scheduledAt < now) {
        broadcast.scheduledAt = now;
      }

      broadcast.status = 'scheduled';
      await broadcast.save();

      return { success: true, data: broadcast };
    } catch (error) {
      console.error('Error resuming broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async cancelScheduledBroadcast(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (!['scheduled', 'paused'].includes(broadcast.status)) {
        return { success: false, error: 'Only scheduled or paused broadcasts can be cancelled' };
      }

      broadcast.status = 'cancelled';
      await broadcast.save();

      return { success: true, data: broadcast };
    } catch (error) {
      console.error('Error cancelling broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async deleteBroadcast(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      // Delete the broadcast from database
      await Broadcast.findByIdAndDelete(broadcastId);
      
      return { success: true, message: 'Broadcast deleted successfully' };
    } catch (error) {
      console.error('Error deleting broadcast:', error);
      return { success: false, error: error.message };
    }
  }
}

module.exports = new BroadcastService();
