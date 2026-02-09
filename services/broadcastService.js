const Broadcast = require('../models/Broadcast');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const Template = require('../models/Template');
const whatsappService = require('./whatsappService');

class BroadcastService {
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

  async sendBroadcast(broadcastId, broadcaster) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
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

      for (const recipient of broadcast.recipients) {
        try {
          let result;
          const phoneNumber = recipient.phone || recipient;
          
          let messageTextForInbox = broadcast.message;
          if (broadcast.templateName) {
            // Fetch template from database - use simple query first
            console.log(`🔍 Looking for template: "${broadcast.templateName}"`);
            
            let template = await Template.findOne({ name: broadcast.templateName });
            
            // If not found, try case-insensitive search
            if (!template) {
              console.log(`🔍 Template not found with exact match, trying case-insensitive search`);
              template = await Template.findOne({ 
                name: { $regex: new RegExp(`^${broadcast.templateName}$`, 'i') }
              });
            }
            
            // If still not found, list all available templates for debugging
            if (!template) {
              const allTemplates = await Template.find({});
              console.log(`🔍 Available templates in database:`);
              allTemplates.forEach(t => {
                console.log(`   - name: "${t.name}", status: "${t.status}", isActive: ${t.isActive}`);
              });
            }
            
            console.log(`🔍 Found template:`, template ? {
              _id: template._id,
              name: template.name,
              templateName: template.templateName,
              category: template.category,
              status: template.status,
              isActive: template.isActive
            } : 'NOT FOUND');
            
            if (!template) {
              console.log(`❌ Template ${broadcast.templateName} not found in database`);
              results.push({
                phone: phoneNumber,
                success: false,
                error: `Template '${broadcast.templateName}' not found`
              });
              failed++;
              broadcast.stats.failed++;
              continue;
            }
            
            // Check if template is active - more flexible validation
            const activeStatuses = ['APPROVED', 'approved', 'ACTIVE', 'active', true];
            const isActive = activeStatuses.includes(template.status) || template.isActive || activeStatuses.includes(template.isActive);
            
            if (!isActive) {
              console.log(`❌ Template ${broadcast.templateName} is not active (status: ${template.status}, isActive: ${template.isActive})`);
              results.push({
                phone: phoneNumber,
                success: false,
                error: `Template '${broadcast.templateName}' is not active. Status: ${template.status}`
              });
              failed++;
              broadcast.stats.failed++;
              continue;
            }

            result = await whatsappService.sendTemplateMessage(
              phoneNumber,
              broadcast.templateName,
              broadcast.language || 'en_US',
              recipient.variables || broadcast.variables || []
            );
            
            console.log(`📤 Template send result for ${phoneNumber}:`, {
              success: result.success,
              templateName: broadcast.templateName,
              language: broadcast.language || 'en_US',
              variables: recipient.variables || broadcast.variables || [],
              error: result.error
            });
            
            // Process template content for inbox display - more flexible content extraction
            let templateContent = '';
            
            // Handle different template content structures
            if (template.content && template.content.body) {
              templateContent = template.content.body;
              if (template.content.header && template.content.header.text) {
                templateContent = template.content.header.text + '\n' + templateContent;
              }
              if (template.content.footer) {
                templateContent = templateContent + '\n' + template.content.footer;
              }
            } else if (template.components) {
              // Extract text from components structure
              const bodyComponent = template.components.find(comp => comp.type === 'BODY');
              if (bodyComponent && bodyComponent.text) {
                templateContent = bodyComponent.text;
              }
              
              const headerComponent = template.components.find(comp => comp.type === 'HEADER');
              if (headerComponent && headerComponent.text) {
                templateContent = headerComponent.text + '\n' + templateContent;
              }
              
              const footerComponent = template.components.find(comp => comp.type === 'FOOTER');
              if (footerComponent && footerComponent.text) {
                templateContent = templateContent + '\n' + footerComponent.text;
              }
            } else if (template.text) {
              // Fallback to simple text field
              templateContent = template.text;
            } else if (template.message) {
              // Another fallback
              templateContent = template.message;
            } else {
              // Last resort - use template name
              templateContent = `Template: ${template.name || broadcast.templateName}`;
            }
            
            messageTextForInbox = this.processTemplateVariables(templateContent, recipient.variables || broadcast.variables || []);
          } else if (broadcast.message) {
            // Process custom message with variable replacement
            const processedMessage = this.processTemplateVariables(broadcast.message, recipient.variables || broadcast.variables || []);
            result = await whatsappService.sendTextMessage(phoneNumber, processedMessage);
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
            const { conversation, message } = await this.updateConversation(phoneNumber, messageTextForInbox, result.data, broadcast._id);

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

  async updateConversation(phone, message, whatsappResponse, broadcastId) {
    try {
      let contact = await Contact.findOne({ phone });
      if (!contact) {
        contact = await Contact.create({ phone, name: '' });
      }

      let conversation = await Conversation.findOne({ contactPhone: phone, status: { $in: ['active', 'pending'] } });
      if (!conversation) {
        conversation = await Conversation.create({
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
          ? await Conversation.find({ contactPhone: { $in: recipientPhones } })
          : [];

        const conversationIds = conversations.map(c => c._id);

        if (conversationIds.length > 0) {
          let convoQuery = {
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

        // Last-resort fallback for legacy data
        if (messages.length === 0) {
          let messageQuery = {
            sender: 'agent',
            timestamp: { $gte: startTime, $lte: endTime }
          };

          if (broadcast.message) {
            messageQuery.$or = [
              { text: broadcast.message },
              { text: { $regex: broadcast.message.substring(0, 20), $options: 'i' } }
            ];
          } else if (broadcast.templateName) {
            messageQuery.$or = [
              { text: { $regex: broadcast.templateName, $options: 'i' } }
            ];
          }

          console.log('?? Message query for broadcast sync (legacy fallback):', messageQuery);
          messages = await Message.find(messageQuery);
        }
      }

      console.log(`?? Found ${messages.length} messages for broadcast "${broadcast.name}"`);

      // Count statuses with proper validation
      const stats = {
        sent: messages.length,
        delivered: messages.filter(msg => msg.status === 'delivered').length,
        read: messages.filter(msg => msg.status === 'read').length,
        failed: messages.filter(msg => msg.status === 'failed').length,
        replied: 0 // Will be calculated below
      };

      // Debug: Log message statuses to identify issues
      console.log('🔍 DEBUG: Message statuses found:', {
        totalMessages: messages.length,
        statusBreakdown: {
          sent: messages.filter(msg => msg.status === 'sent').length,
          delivered: messages.filter(msg => msg.status === 'delivered').length,
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
