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
        // Ensure scheduledAt is a Date object
        broadcastData.scheduledAt = new Date(broadcastData.scheduledAt);
      }
      
      const broadcast = await Broadcast.create(broadcastData);
      return { success: true, data: broadcast };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async sendBroadcast(broadcastId, broadcaster) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

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
            // Fetch template from database to get actual content
            const template = await Template.findOne({ name: broadcast.templateName, isActive: true });
            if (!template) {
              results.push({
                phone: phoneNumber,
                success: false,
                error: `Template '${broadcast.templateName}' not found or inactive`
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
            
            // Process template content for inbox display
            let templateContent = template.content.body || '';
            if (template.content.header && template.content.header.text) {
              templateContent = template.content.header.text + '\n' + templateContent;
            }
            if (template.content.footer) {
              templateContent = templateContent + '\n' + template.content.footer;
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
            const { conversation, message } = await this.updateConversation(phoneNumber, messageTextForInbox, result.data);

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

      // Sync stats from actual messages after completion
      await this.syncBroadcastStats(broadcastId);

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

  async updateConversation(phone, message, whatsappResponse) {
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
        status: 'sent'
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
      // We can identify them by the message content and timestamp range
      const startTime = new Date(broadcast.startedAt || broadcast.createdAt);
      const endTime = new Date(broadcast.completedAt || Date.now());
      
      // Build message query - more flexible matching
      let messageQuery = {
        sender: 'agent',
        timestamp: { $gte: startTime, $lte: endTime }
      };

      // Add content matching if available
      if (broadcast.message) {
        // For custom messages, try exact match first, then partial match
        messageQuery.$or = [
          { text: broadcast.message },
          { text: { $regex: broadcast.message.substring(0, 20), $options: 'i' } }
        ];
      } else if (broadcast.templateName) {
        // For template messages, look for template content
        messageQuery.$or = [
          { text: { $regex: broadcast.templateName, $options: 'i' } },
          { templateName: broadcast.templateName },
          { text: { $regex: 'Welcome', $options: 'i' } } // Common template pattern
        ];
      }

      const messages = await Message.find(messageQuery);

      // Count statuses
      const stats = {
        sent: messages.length,
        delivered: messages.filter(msg => msg.status === 'delivered').length,
        read: messages.filter(msg => msg.status === 'read').length,
        failed: messages.filter(msg => msg.status === 'failed').length
      };

      // Only update broadcast stats if they're different
      const currentStats = broadcast.stats || {};
      const statsChanged = 
        currentStats.sent !== stats.sent ||
        currentStats.delivered !== stats.delivered ||
        currentStats.read !== stats.read ||
        currentStats.failed !== stats.failed;

      if (statsChanged) {
        // Update only the stats field without touching other fields
        await Broadcast.updateOne(
          { _id: broadcastId },
          { $set: { stats: stats, updatedAt: new Date() } }
        );
        console.log(`📊 Updated stats for broadcast ${broadcast.name}:`, stats);
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
  async checkScheduledBroadcasts(broadcaster) {
    try {
      const now = new Date();
      const scheduledBroadcasts = await Broadcast.find({
        status: 'scheduled',
        scheduledAt: { $lte: now }
      });

      for (const broadcast of scheduledBroadcasts) {
        console.log(`Processing scheduled broadcast: ${broadcast.name}`);
        await this.sendBroadcast(broadcast._id, broadcaster);
      }
    } catch (error) {
      console.error('Error checking scheduled broadcasts:', error);
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
