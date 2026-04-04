const cron = require('node-cron');

const startAppScheduler = ({
  app,
  mongoose,
  broadcastService,
  missedCallAutomationService,
  templateController,
  metaAdsService
}) => {
  console.log('Starting broadcast scheduler...');

  cron.schedule('* * * * *', async () => {
    try {
      console.log('Checking for scheduled broadcasts...');

      if (mongoose.connection.readyState !== 1) {
        console.log('Database not connected, skipping scheduler run');
        return;
      }

      await broadcastService.checkScheduledBroadcasts();
      await missedCallAutomationService.processPendingMissedCalls({ app });
    } catch (error) {
      console.error('Scheduler error:', error.message);
    }
  });

  cron.schedule('0 */6 * * *', async () => {
    try {
      console.log('Starting automatic template sync from Meta...');

      if (mongoose.connection.readyState !== 1) {
        console.log('Database not connected, skipping template sync');
        return;
      }

      const syncUserId = process.env.TEMPLATE_SYNC_USER_ID || process.env.DEFAULT_USER_ID || null;
      const mockReq = {
        syncUserId: syncUserId || undefined,
        whatsappCredentials: {
          businessAccountId: process.env.WHATSAPP_BUSINESS_ACCOUNT_ID,
          phoneNumberId: process.env.WHATSAPP_PHONE_NUMBER_ID,
          accessToken: process.env.WHATSAPP_ACCESS_TOKEN,
          webhookVerifyToken: process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN
        }
      };
      const mockRes = {
        status: (code) => ({
          json: (data) => {
            if (code === 200) {
              console.log('Automatic template sync completed:', data.message);
            } else {
              console.error('Automatic template sync failed:', data.error);
            }
          }
        }),
        json: (data) => {
          console.log('Automatic template sync completed:', data.message);
        }
      };

      if (!mockReq.syncUserId) {
        console.warn('TEMPLATE_SYNC_USER_ID not set; skipping automatic template sync');
        return;
      }

      if (
        !mockReq.whatsappCredentials.businessAccountId ||
        !mockReq.whatsappCredentials.phoneNumberId ||
        !mockReq.whatsappCredentials.accessToken
      ) {
        console.warn('WhatsApp credentials missing in env; skipping automatic template sync');
        return;
      }

      await templateController.syncWhatsAppTemplates(mockReq, mockRes);
    } catch (error) {
      console.error('Automatic template sync error:', error.message);
    }
  });

  console.log('Broadcast scheduler started - checking every minute');

  cron.schedule('*/5 * * * *', async () => {
    try {
      if (mongoose.connection.readyState !== 1) {
        console.log('Skipping Meta Ads sync: database not connected');
        return;
      }

      const result = await metaAdsService.syncAllCrudCampaignAnalytics();
      if (result.synced || result.warnings.length) {
        console.log(`Meta Ads sync completed. Synced: ${result.synced}, warnings: ${result.warnings.length}`);
      }
    } catch (error) {
      console.error('Meta Ads sync error:', error.message);
    }
  });

  cron.schedule('*/30 * * * *', async () => {
    try {
      if (mongoose.connection.readyState !== 1) {
        console.log('Skipping campaign performance sync: database not connected');
        return;
      }

      console.log('Syncing campaign performance metrics...');
      const Campaign = require('../models/campaign');

      const result = await Campaign.updateMany(
        {},
        { $set: { lastSynced: new Date() } }
      );

      console.log(`Campaign performance sync completed: ${result.modifiedCount} campaigns updated`);
    } catch (error) {
      console.error('Campaign performance sync error:', error.message);
    }
  });

  console.log('Template auto-sync disabled: run per user via authenticated API.');
};

module.exports = {
  startAppScheduler
};
