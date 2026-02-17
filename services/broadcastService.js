const Broadcast = require('../models/Broadcast');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const whatsappService = require('./whatsappService');

class BroadcastService {
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

      const resolvedCredentials = credentials;

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
              broadcast.createdById
            );

            if (typeof broadcaster === 'function' && conversation && message) {
              broadcaster({
                type: 'message_sent',
                conversation: conversation.toObject(),
                message: message.toObject()
              });
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

  async updateConversation(phone, message, whatsappResponse, broadcastId, userId) {
    try {
      let contact = await Contact.findOne({ userId, phone });
      if (!contact) {
        contact = await Contact.create({ userId, phone, name: '' });
      }

      let conversation = await Conversation.findOne({ userId, contactPhone: phone, status: { $in: ['active', 'pending'] } });
      if (!conversation) {
        conversation = await Conversation.create({
          userId,
          contactId: contact._id,
          contactPhone: phone,
          contactName: contact.name,
          lastMessage: message,
          lastMessageTime: new Date(),
          lastMessageFrom: 'agent'
        });
      } else {
        conversation.lastMessage = message;
        conversation.lastMessageTime = new Date();
        conversation.lastMessageFrom = 'agent';
        await conversation.save();
      }

      const whatsappMessageId = whatsappResponse?.messages?.[0]?.id;
      const savedMessage = await Message.create({
        userId,
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
      const broadcasts = await Broadcast.find(filters)
        .sort({ createdAt: -1 });
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
      return { success: true, data: broadcast };
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
          await this.sendBroadcast(broadcast._id);
          console.log(`✅ Successfully sent scheduled broadcast: ${broadcast.name}`);
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
