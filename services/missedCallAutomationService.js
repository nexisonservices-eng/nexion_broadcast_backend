const MissedCall = require('../models/MissedCall');
const Template = require('../models/Template');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const whatsappService = require('./whatsappService');
const { getWhatsAppCredentialsByUserId } = require('./userWhatsAppCredentialsService');

const DEFAULT_RETRY_DELAY_MINUTES = 2;

const normalizeDelayMinutes = (value, fallback = 5) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return parsed;
};

const toIntInRange = (value, fallback, min, max) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const intVal = Math.trunc(num);
  if (intVal < min || intVal > max) return fallback;
  return intVal;
};

const getTimeZoneParts = (date, timeZone) => {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });

  const parts = formatter.formatToParts(date);
  const getPart = (type) => Number(parts.find((p) => p.type === type)?.value || 0);
  return {
    year: getPart('year'),
    month: getPart('month'),
    day: getPart('day'),
    hour: getPart('hour'),
    minute: getPart('minute'),
    second: getPart('second')
  };
};

const zonedLocalToUtcDate = ({ year, month, day, hour, minute, second = 0 }, timeZone) => {
  const utcGuess = Date.UTC(year, month - 1, day, hour, minute, second);
  const guessDate = new Date(utcGuess);
  const asTz = getTimeZoneParts(guessDate, timeZone);
  const asTzUtcMillis = Date.UTC(
    asTz.year,
    asTz.month - 1,
    asTz.day,
    asTz.hour,
    asTz.minute,
    asTz.second
  );
  const offsetMs = asTzUtcMillis - guessDate.getTime();
  return new Date(utcGuess - offsetMs);
};

const computeNextRunAt = ({
  calledAt = new Date(),
  delayMinutes = 5,
  mode = 'immediate',
  nightHour = 21,
  nightMinute = 0,
  timezone = 'Asia/Kolkata'
}) => {
  const base = new Date(calledAt);
  const safeDelay = normalizeDelayMinutes(delayMinutes, 5);
  const triggerAt = new Date(base.getTime() + safeDelay * 60 * 1000);

  if (String(mode || '').toLowerCase() !== 'night_batch') {
    return triggerAt;
  }

  const safeHour = toIntInRange(nightHour, 21, 0, 23);
  const safeMinute = toIntInRange(nightMinute, 0, 0, 59);
  const safeTimezone = String(timezone || '').trim() || 'Asia/Kolkata';

  try {
    const triggerLocal = getTimeZoneParts(triggerAt, safeTimezone);
    let candidate = zonedLocalToUtcDate(
      {
        year: triggerLocal.year,
        month: triggerLocal.month,
        day: triggerLocal.day,
        hour: safeHour,
        minute: safeMinute,
        second: 0
      },
      safeTimezone
    );

    if (candidate.getTime() <= triggerAt.getTime()) {
      const nextDayUtc = new Date(Date.UTC(triggerLocal.year, triggerLocal.month - 1, triggerLocal.day + 1));
      const nextLocal = getTimeZoneParts(nextDayUtc, safeTimezone);
      candidate = zonedLocalToUtcDate(
        {
          year: nextLocal.year,
          month: nextLocal.month,
          day: nextLocal.day,
          hour: safeHour,
          minute: safeMinute,
          second: 0
        },
        safeTimezone
      );
    }

    return candidate;
  } catch (error) {
    return triggerAt;
  }
};

const normalizeTemplateVariables = (value) => {
  if (!Array.isArray(value)) return [];
  return value
    .map((item, idx) => ({
      index: Number.isFinite(Number(item?.index)) ? Number(item.index) : idx + 1,
      source: String(item?.source || item?.sourceType || 'callerName').trim() || 'callerName',
      value: String(item?.value || item?.staticValue || '').trim()
    }))
    .sort((a, b) => a.index - b.index);
};

const resolveVariableValue = (mapping, missedCall) => {
  const source = String(mapping?.source || 'callerName').trim();
  const staticValue = String(mapping?.value || '').trim();
  const calledAt = new Date(missedCall.calledAt || missedCall.createdAt || Date.now());

  switch (source) {
    case 'callerPhone':
      return String(missedCall.fromNumber || '').trim();
    case 'businessPhone':
      return String(missedCall.toNumber || '').trim();
    case 'callDate':
      return Number.isNaN(calledAt.getTime())
        ? ''
        : calledAt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
    case 'callTime':
      return Number.isNaN(calledAt.getTime())
        ? ''
        : calledAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    case 'static':
      return staticValue;
    case 'callerName':
    default:
      return String(missedCall.callerName || missedCall.fromNumber || '').trim();
  }
};

const extractTemplateIndexes = (text) => {
  if (!text) return [];
  const indexes = new Set();
  const regex = /\{\{(\d+)\}\}/g;
  let match;
  while ((match = regex.exec(String(text))) !== null) {
    const idx = Number(match[1]);
    if (Number.isFinite(idx) && idx > 0) indexes.add(idx);
  }
  return Array.from(indexes).sort((a, b) => a - b);
};

const normalizeTemplateSelection = (templateName, templateLanguage) => {
  let name = String(templateName || '').trim();
  let language = String(templateLanguage || '').trim();

  if (name.includes('||')) {
    const parts = name.split('||');
    name = String(parts[0] || '').trim();
    if (!language) {
      language = String(parts[1] || '').trim();
    }
  }

  const match = name.match(/^(.*)\(([^)]+)\)\s*$/);
  if (match) {
    const possibleName = String(match[1] || '').trim();
    const possibleLanguage = String(match[2] || '').trim();
    if (possibleName && possibleLanguage && !language) {
      name = possibleName;
      language = possibleLanguage;
    }
  }

  return {
    templateName: name,
    templateLanguage: language || 'en_US'
  };
};

const buildTemplateVariableEntries = (missedCall, credentials) => {
  const fromAutomation = normalizeTemplateVariables(missedCall?.automation?.templateVariables);
  const fromCredentials = normalizeTemplateVariables(credentials?.missedCallTemplateVariables);
  const mappings = fromAutomation.length > 0 ? fromAutomation : fromCredentials;
  if (mappings.length === 0) return [];

  return mappings.map((mapping) => ({
    index: Number(mapping.index),
    value: resolveVariableValue(mapping, missedCall)
  }));
};

const resolveTemplateBodyText = async ({ missedCall, templateName, templateLanguage, credentials }) => {
  const localTemplate = await Template.findOne({
    userId: missedCall.userId,
    name: templateName,
    isActive: true
  })
    .select({ 'content.body': 1, language: 1 })
    .lean();

  if (localTemplate?.content?.body) {
    return String(localTemplate.content.body);
  }

  const metaResult = await whatsappService.getTemplateList(credentials || null);
  if (!metaResult?.success) return '';

  const all = Array.isArray(metaResult.data?.data) ? metaResult.data.data : [];
  const found = all.find((tpl) => {
    const sameName = String(tpl?.name || '').trim() === String(templateName || '').trim();
    const sameLanguage = String(tpl?.language || '').trim() === String(templateLanguage || '').trim();
    return sameName && sameLanguage;
  }) || all.find((tpl) => String(tpl?.name || '').trim() === String(templateName || '').trim());

  if (!found || !Array.isArray(found.components)) return '';
  const body = found.components.find((c) => String(c?.type || '').toUpperCase() === 'BODY');
  return String(body?.text || '');
};

const buildTemplateVariables = async (
  missedCall,
  credentials,
  templateName,
  templateLanguage,
  resolvedBodyText = ''
) => {
  const entries = buildTemplateVariableEntries(missedCall, credentials);
  const bodyText = resolvedBodyText || await resolveTemplateBodyText({
    missedCall,
    templateName,
    templateLanguage,
    credentials
  });
  const requiredIndexes = extractTemplateIndexes(bodyText);

  if (requiredIndexes.length === 0) {
    return entries.map((e) => e.value);
  }

  const valuesByIndex = new Map(entries.map((e) => [Number(e.index), String(e.value || '')]));
  return requiredIndexes.map((idx) => {
    if (valuesByIndex.has(idx)) return valuesByIndex.get(idx);
    return resolveVariableValue({ source: 'callerName', value: '' }, missedCall);
  });
};

const renderTemplateBody = (bodyText, variables) => {
  let rendered = String(bodyText || '');
  if (!rendered) return '';

  const safeVariables = Array.isArray(variables) ? variables : [];
  safeVariables.forEach((value, index) => {
    const placeholder = new RegExp(`\\{\\{\\s*${index + 1}\\s*\\}\\}`, 'g');
    rendered = rendered.replace(placeholder, String(value || ''));
  });

  return rendered;
};

const buildAutomationInboxMessage = ({ templateName, bodyText, variables }) => {
  const renderedBody = renderTemplateBody(bodyText, variables).trim();
  if (renderedBody) return renderedBody;
  return `Template: ${String(templateName || '').trim() || 'missed_call_template'}`;
};

async function updateConversationForMissedCallAutomation({
  userId,
  phone,
  callerName,
  messageText,
  whatsappMessageId
}) {
  let contact = await Contact.findOne({ userId, phone });
  if (!contact) {
    contact = await Contact.create({
      userId,
      phone,
      name: String(callerName || phone || '').trim(),
      sourceType: 'incoming_call',
      lastContact: new Date()
    });
  } else {
    contact.lastContact = new Date();
    if (!contact.name && callerName) {
      contact.name = String(callerName).trim();
    }
    await contact.save();
  }

  let conversation = await Conversation.findOne({
    userId,
    contactPhone: phone,
    status: { $in: ['active', 'pending'] }
  });

  if (!conversation) {
    conversation = await Conversation.create({
      userId,
      contactId: contact._id,
      contactPhone: phone,
      contactName: contact.name,
      lastMessage: messageText,
      lastMessageTime: new Date(),
      lastMessageMediaType: '',
      lastMessageAttachmentName: '',
      lastMessageAttachmentPages: null,
      lastMessageFrom: 'agent'
    });
  } else {
    conversation.contactName = conversation.contactName || contact.name;
    conversation.lastMessage = messageText;
    conversation.lastMessageTime = new Date();
    conversation.lastMessageMediaType = '';
    conversation.lastMessageAttachmentName = '';
    conversation.lastMessageAttachmentPages = null;
    conversation.lastMessageFrom = 'agent';
    await conversation.save();
  }

  const message = await Message.create({
    userId,
    conversationId: conversation._id,
    sender: 'agent',
    text: messageText,
    whatsappMessageId: String(whatsappMessageId || '').trim() || undefined,
    status: 'sent'
  });

  return { conversation, message };
}

async function processSingleMissedCall(missedCall, sendToUser) {
  const now = new Date();

  try {
    const credentials = await getWhatsAppCredentialsByUserId(String(missedCall.userId));
    if (!credentials) {
      throw new Error('WhatsApp credentials not found for missed call owner');
    }

    const rawTemplateName =
      (missedCall.automation?.templateName || credentials.missedCallTemplateName || 'hello_world').trim();
    const rawTemplateLanguage =
      (missedCall.automation?.templateLanguage || credentials.missedCallTemplateLanguage || 'en_US').trim();
    const { templateName, templateLanguage } = normalizeTemplateSelection(
      rawTemplateName,
      rawTemplateLanguage
    );
    if (!templateName) {
      throw new Error('Missed call automation template name is empty');
    }
    const bodyText = await resolveTemplateBodyText({
      missedCall,
      templateName,
      templateLanguage,
      credentials
    });
    const templateVariables = await buildTemplateVariables(
      missedCall,
      credentials,
      templateName,
      templateLanguage,
      bodyText
    );

    const result = await whatsappService.sendTemplateMessage(
      missedCall.fromNumber,
      templateName,
      templateLanguage,
      templateVariables,
      credentials
    );

    if (!result?.success) {
      const reason = typeof result?.error === 'string' ? result.error : JSON.stringify(result?.error || {});
      throw new Error(reason || 'Failed to send missed call automation template');
    }

    const messageId = result?.data?.messages?.[0]?.id || '';
    const messageTextForInbox = buildAutomationInboxMessage({
      templateName,
      bodyText,
      variables: templateVariables
    });

    let conversation = null;
    let message = null;
    try {
      const saved = await updateConversationForMissedCallAutomation({
        userId: missedCall.userId,
        phone: missedCall.fromNumber,
        callerName: missedCall.callerName,
        messageText: messageTextForInbox,
        whatsappMessageId: messageId
      });
      conversation = saved.conversation;
      message = saved.message;
    } catch (inboxError) {
      console.error('Failed to persist missed call automation message in inbox:', inboxError.message);
    }

    const updated = await MissedCall.findByIdAndUpdate(
      missedCall._id,
      {
        $set: {
          'automation.status': 'sent',
          'automation.sentAt': now,
          'automation.messageId': messageId,
          'automation.lastError': ''
        },
        $inc: {
          'automation.attempts': 1
        }
      },
      { new: true }
    ).lean();

    if (typeof sendToUser === 'function') {
      if (conversation && message) {
        const payload = {
          conversation: conversation.toObject(),
          message: message.toObject()
        };

        // Team Inbox listens to `new_message` for realtime append/update.
        sendToUser(String(missedCall.userId), {
          type: 'new_message',
          ...payload
        });

        // Keep legacy event for existing listeners that still use message_sent.
        sendToUser(String(missedCall.userId), {
          type: 'message_sent',
          ...payload
        });
      }

      sendToUser(String(missedCall.userId), {
        type: 'missed_call_automation_sent',
        missedCallId: String(missedCall._id),
        call: updated
      });
    }

    return { success: true };
  } catch (error) {
    const retryDelayMinutes = normalizeDelayMinutes(
      missedCall.automation?.delayMinutes,
      DEFAULT_RETRY_DELAY_MINUTES
    );
    const nextRunAt = computeNextRunAt({
      calledAt: new Date(),
      delayMinutes: retryDelayMinutes,
      mode: missedCall.automation?.mode || 'immediate',
      nightHour: missedCall.automation?.nightHour ?? 21,
      nightMinute: missedCall.automation?.nightMinute ?? 0,
      timezone: missedCall.automation?.timezone || 'Asia/Kolkata'
    });

    const updated = await MissedCall.findByIdAndUpdate(
      missedCall._id,
      {
        $set: {
          'automation.status': 'failed',
          'automation.lastError': error.message,
          'automation.nextRunAt': nextRunAt
        },
        $inc: {
          'automation.attempts': 1
        }
      },
      { new: true }
    ).lean();

    if (typeof sendToUser === 'function') {
      sendToUser(String(missedCall.userId), {
        type: 'missed_call_automation_failed',
        missedCallId: String(missedCall._id),
        error: error.message,
        call: updated
      });
    }

    return { success: false, error: error.message };
  }
}

async function processPendingMissedCalls({ limit = 50, app = null } = {}) {
  const now = new Date();
  const sendToUser = app?.locals?.sendToUser;

  const dueCalls = await MissedCall.find({
    status: 'missed',
    'automation.enabled': true,
    'automation.status': { $in: ['pending', 'failed'] },
    'automation.nextRunAt': { $lte: now }
  })
    .sort({ 'automation.nextRunAt': 1, calledAt: 1 })
    .limit(Math.max(1, Number(limit) || 50))
    .lean();

  let processed = 0;
  let sent = 0;
  let failed = 0;

  for (const row of dueCalls) {
    const locked = await MissedCall.findOneAndUpdate(
      {
        _id: row._id,
        'automation.status': { $in: ['pending', 'failed'] }
      },
      {
        $set: {
          'automation.status': 'processing'
        }
      },
      { new: true }
    ).lean();

    if (!locked) continue;

    processed += 1;
    const result = await processSingleMissedCall(locked, sendToUser);
    if (result.success) sent += 1;
    else failed += 1;
  }

  return { processed, sent, failed };
}

module.exports = {
  processPendingMissedCalls
};
