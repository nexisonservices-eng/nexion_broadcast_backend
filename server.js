// server.js - Main backend server
require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const WebSocket = require('ws');

// Database connection
const connectDB = require('./config/database');
const mongoose = require('mongoose');
connectDB();

// Services
const broadcastService = require('./services/broadcastService');
const missedCallAutomationService = require('./services/missedCallAutomationService');
const templateController = require('./controllers/templateController');
const metaAdsService = require('./services/metaAdsService');
const whatsappService = require('./services/whatsappService');

// Models
const Contact = require('./models/Contact');
const Template = require('./models/Template');
const Conversation = require('./models/Conversation');
const Message = require('./models/Message');
const Broadcast = require('./models/Broadcast');
const MetaAdsTransaction = require('./models/MetaAdsTransaction');
const MetaAdsWallet = require('./models/MetaAdsWallet');
const LeadScoringConfig = require('./models/LeadScoringConfig');
const GoogleCalendarConnection = require('./models/GoogleCalendarConnection');
const Campaign = require('./models/campaign');
const { ContactDocument } = require('./models/ContactDocument');

// Middleware / config / helpers
const auth = require('./middleware/auth');
const requireWhatsAppCredentials = require('./middleware/requireWhatsAppCredentials');
const requirePlanFeature = require('./middleware/planGuard');
const whatsappConfig = require('./config/whatsapp');
const { validateMetaAdsEnv } = require('./config/metaAdsConfig');
const {
  resolveUserIdByPhoneNumberId,
  getWhatsAppCredentialsForUser,
  getWhatsAppCredentialsByUserId
} = require('./services/userWhatsAppCredentialsService');
const {
  getLeadScoringSettings,
  updateLeadScoringSettings,
  applyReadScoreForMessage,
  applyIncomingMessageScore
} = require('./services/leadScoringService');
const { isDebugLoggingEnabled, validateSecurityEnv } = require('./utils/securityConfig');

// Routes
const bulkRoutes = require('./routes/bulk');
const templateRoutes = require('./routes/templates');
const broadcastRoutes = require('./routes/broadcasts');
const conversationRoutes = require('./routes/conversations');
const messageRoutes = require('./routes/messages');
const contactRoutes = require('./routes/contacts');
const missedCallRoutes = require('./routes/missedCalls');
const metaAdsRoutes = require('./routes/metaAds');
const insightsRoutes = require('./routes/insights');
const whatsappWorkflowRoutes = require('./routes/whatsappWorkflows');
const crmRoutes = require('./routes/crm');
const googleCalendarRoutes = require('./routes/googleCalendar');
const campaignRoutes = require('./routes/campaignroutes');
const { registerWhatsAppWebhookRoutes } = require('./routes/whatsappWebhookRoutes');
const { registerLegacyCoreRoutes } = require('./routes/legacyCoreRoutes');
const { createWebSocketHub } = require('./realtime/websocketHub');
const { startAppScheduler } = require('./jobs/appScheduler');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const ENABLE_DEBUG_LOGS = isDebugLoggingEnabled();
const securityEnvValidation = validateSecurityEnv();

if (securityEnvValidation.warnings.length) {
  console.warn('Security configuration warnings:', securityEnvValidation.warnings.join(' '));
}
if (securityEnvValidation.errors.length) {
  console.error('Security configuration errors:', securityEnvValidation.errors.join(' '));
  process.exit(1);
}

// Middleware
const allowedOrigins = [
  process.env.FRONTEND_URL,
  'https://www.technovahub.in',
  'https://technovahub.in',
  'https://technovahub.in/nexion',
  'https://technovo-automation-afplwwbfj-technovas-projects-37226de2.vercel.app',
  'https://technovo-automation-m9n8fz6sl-technovas-projects-37226de2.vercel.app',
  'https://technovo-automation.vercel.app',
  'https://localhost:5173',
  'https://127.0.0.1:5173',
  'https://localhost:5174',
  'https://127.0.0.1:5174',
  'https://localhost:5175',
  'https://127.0.0.1:5175',
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  'http://127.0.0.1:53918',
  'http://localhost:53918',
  'http://localhost:60932',
  'http://localhost:5174',
  'http://localhost:5175',
  'http://127.0.0.1:5174',
  'http://127.0.0.1:5175'
].filter(Boolean);

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin) return callback(null, true);

    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }

    if (origin.includes('.vercel.app')) {
      console.log(`CORS allowed for Vercel deployment: ${origin}`);
      return callback(null, true);
    }

    console.log(`CORS blocked: ${origin}`);
    return callback(new Error(`CORS blocked: ${origin}`));
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'ngrok-skip-browser-warning'],
  preflightContinue: false,
  optionsSuccessStatus: 204
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    }
  })
);
app.use(express.urlencoded({ extended: true }));

// Backward-compatible redirect for older Meta OAuth callback URLs.
app.get('/auth/meta/callback', (req, res) => {
  const query = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
  res.redirect(`/api/meta-ads/oauth/callback${query}`);
});

const metaEnvValidation = validateMetaAdsEnv();
if (metaEnvValidation.warnings.length) {
  console.warn('Meta Ads configuration warnings:', metaEnvValidation.warnings.join(' '));
}

// Core API routes
app.use('/api/bulk', bulkRoutes);
app.use('/api/templates', templateRoutes);
app.use('/api/broadcasts', broadcastRoutes);
app.use('/api/conversations', conversationRoutes);
app.use('/api/messages', messageRoutes);
app.use('/api/contacts', contactRoutes);
app.use('/api/missedcalls', missedCallRoutes);
app.use('/api/meta-ads', metaAdsRoutes);
app.use('/api/insights', insightsRoutes);
app.use('/api/whatsapp/workflows', whatsappWorkflowRoutes);
app.use('/api/crm', crmRoutes);
app.use('/api/google-calendar', googleCalendarRoutes);
app.use('/api/campaigns', campaignRoutes);

// WebSocket hub
const websocketHub = createWebSocketHub({ wss });
app.locals.broadcast = websocketHub.broadcast;
app.locals.sendToUser = websocketHub.sendToUser;

// WhatsApp webhook endpoints
registerWhatsAppWebhookRoutes(app, {
  whatsappConfig,
  ENABLE_DEBUG_LOGS,
  resolveUserIdByPhoneNumberId,
  getWhatsAppCredentialsByUserId,
  applyIncomingMessageScore,
  applyReadScoreForMessage,
  Contact,
  Conversation,
  Message,
  Broadcast,
  emitRealtimeEvent: websocketHub.emitRealtimeEvent
});

// Legacy app-level API endpoints preserved for compatibility
registerLegacyCoreRoutes(app, {
  auth,
  requirePlanFeature,
  requireWhatsAppCredentials,
  whatsappService,
  getLeadScoringSettings,
  updateLeadScoringSettings,
  Contact,
  Conversation,
  Message,
  Broadcast,
  broadcastService,
  getWhatsAppCredentialsForUser,
  mongoose,
  ENABLE_DEBUG_LOGS,
  emitRealtimeEvent: websocketHub.emitRealtimeEvent
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  console.log('WebSocket server ready');
  console.log(`Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:5173'}`);
  console.log(`Campaign Management API: http://localhost:${PORT}/api/campaigns`);

  try {
    await Promise.all([
      Contact.syncIndexes(),
      Template.syncIndexes(),
      MetaAdsWallet.syncIndexes(),
      MetaAdsTransaction.syncIndexes(),
      LeadScoringConfig.syncIndexes(),
      GoogleCalendarConnection.syncIndexes(),
      Campaign.syncIndexes(),
      ContactDocument.syncIndexes()
    ]);
    console.log('MongoDB indexes synced for all models including Campaigns.');
  } catch (indexError) {
    console.error('Failed to sync MongoDB indexes:', indexError.message);
  }

  startAppScheduler({
    app,
    mongoose,
    broadcastService,
    missedCallAutomationService,
    templateController,
    metaAdsService
  });

  console.log('Skipping global template sync on startup: credentials are user-scoped and fetched per request.');
});
