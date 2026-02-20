const express = require('express');
const auth = require('../middleware/auth');
const MissedCall = require('../models/MissedCall');
const Contact = require('../models/Contact');
const {
  resolveUserIdByPhoneNumberId,
  resolveUserIdByRegisteredPhone,
  getWhatsAppCredentialsByUserId,
  updateUserCredentialsByUserId
} = require('../services/userWhatsAppCredentialsService');

const router = express.Router();

const normalizePhone = (value) => {
  if (!value) return '';
  return String(value).replace(/[^\d+]/g, '').trim();
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
    name,
    language: language || 'en_US'
  };
};

const isMissedStatus = (status) => {
  const normalized = String(status || '').toLowerCase().trim();
  if (!normalized) return true;
  return [
    'missed',
    'no-answer',
    'no answer',
    'not_answered',
    'busy',
    'failed',
    'cancelled',
    'canceled'
  ].includes(normalized);
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
  const safeDelay = Number.isFinite(Number(delayMinutes)) && Number(delayMinutes) >= 0
    ? Number(delayMinutes)
    : 5;
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

router.post('/webhook', async (req, res) => {
  try {
    const body = req.body || {};
    const fromNumber = normalizePhone(
      body.From || body.from || body.caller || body.phone || body.phoneNumber
    );
    const toNumber = normalizePhone(
      body.To || body.to || body.called || body.recipient || body.businessPhone || body.phoneNumberId
    );
    const rawStatus = body.CallStatus || body.status || body.callStatus || 'missed';
    const direction = String(body.Direction || body.direction || 'inbound').toLowerCase() === 'outbound'
      ? 'outbound'
      : 'inbound';

    const lookupPhoneNumberId = body.phone_number_id || body.phoneNumberId || '';
    let userId = null;

    if (lookupPhoneNumberId) {
      userId = await resolveUserIdByPhoneNumberId(String(lookupPhoneNumberId));
    }
    if (!userId && toNumber) {
      userId = await resolveUserIdByRegisteredPhone(toNumber);
    }

    if (!userId) {
      return res.status(202).json({
        success: false,
        skipped: true,
        message: 'No mapped admin user found for this missed call webhook payload'
      });
    }

    if (!fromNumber) {
      return res.status(400).json({ success: false, error: 'Caller phone number is required in webhook payload' });
    }

    const calledAt = body.timestamp ? new Date(body.timestamp) : new Date();
    const isMissed = isMissedStatus(rawStatus);

    const credentials = await getWhatsAppCredentialsByUserId(String(userId));
    const delayMinutesRaw = credentials?.missedCallDelayMinutes;
    const delayMinutes = Number.isFinite(Number(delayMinutesRaw)) && Number(delayMinutesRaw) >= 0
      ? Number(delayMinutesRaw)
      : 5;
    const automationEnabled = credentials?.missedCallAutomationEnabled !== false;
    const templateName = String(credentials?.missedCallTemplateName || '').trim() || 'hello_world';
    const templateLanguage = String(credentials?.missedCallTemplateLanguage || 'en_US').trim() || 'en_US';
    const templateVariables = normalizeTemplateVariables(credentials?.missedCallTemplateVariables);
    const automationMode =
      String(credentials?.missedCallAutomationMode || 'immediate').toLowerCase() === 'night_batch'
        ? 'night_batch'
        : 'immediate';
    const nightHour = toIntInRange(credentials?.missedCallNightHour, 21, 0, 23);
    const nightMinute = toIntInRange(credentials?.missedCallNightMinute, 0, 0, 59);
    const timezone = String(credentials?.missedCallTimezone || 'Asia/Kolkata').trim() || 'Asia/Kolkata';
    const nextRunAt = isMissed && automationEnabled
      ? computeNextRunAt({
          calledAt,
          delayMinutes,
          mode: automationMode,
          nightHour,
          nightMinute,
          timezone
        })
      : null;

    const missedCall = await MissedCall.create({
      userId,
      fromNumber,
      toNumber,
      callerName: String(body.callerName || body.name || '').trim(),
      direction,
      status: isMissed ? 'missed' : 'resolved',
      provider: String(body.provider || 'webhook').trim() || 'webhook',
      calledAt,
      payload: body,
      automation: {
        enabled: automationEnabled,
        status: isMissed
          ? (automationEnabled ? 'pending' : 'disabled')
          : 'disabled',
        templateName,
        templateLanguage,
        templateVariables,
        delayMinutes,
        mode: automationMode,
        nightHour,
        nightMinute,
        timezone,
        nextRunAt,
        attempts: 0
      }
    });

    if (isMissed) {
      const existingContact = await Contact.findOne({ userId, phone: fromNumber });
      if (!existingContact) {
        await Contact.create({
          userId,
          phone: fromNumber,
          name: missedCall.callerName || fromNumber,
          sourceType: 'incoming_call',
          lastContact: new Date()
        });
      } else {
        existingContact.lastContact = new Date();
        await existingContact.save();
      }
    }

    const sendToUser = req.app.locals.sendToUser;
    if (typeof sendToUser === 'function') {
      sendToUser(String(userId), {
        type: 'missed_call_new',
        call: missedCall.toObject()
      });
    }

    return res.status(201).json({ success: true, data: missedCall });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.use(auth);
router.get('/settings', async (req, res) => {
  try {
    const creds = await getWhatsAppCredentialsByUserId(String(req.user.id));
    return res.json({
      success: true,
      data: {
        missedCallAutomationEnabled: creds?.missedCallAutomationEnabled !== false,
        missedCallDelayMinutes:
          Number.isFinite(Number(creds?.missedCallDelayMinutes))
            ? Number(creds.missedCallDelayMinutes)
            : 5,
        missedCallTemplateName: String(creds?.missedCallTemplateName || '').trim(),
        missedCallTemplateLanguage: String(creds?.missedCallTemplateLanguage || 'en_US').trim() || 'en_US',
        missedCallTemplateVariables: normalizeTemplateVariables(creds?.missedCallTemplateVariables),
        missedCallAutomationMode:
          String(creds?.missedCallAutomationMode || 'immediate').toLowerCase() === 'night_batch'
            ? 'night_batch'
            : 'immediate',
        missedCallNightHour: toIntInRange(creds?.missedCallNightHour, 21, 0, 23),
        missedCallNightMinute: toIntInRange(creds?.missedCallNightMinute, 0, 0, 59),
        missedCallTimezone: String(creds?.missedCallTimezone || 'Asia/Kolkata').trim() || 'Asia/Kolkata',
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});
router.put('/settings', async (req, res) => {
  try {
    const body = req.body || {};
    const payload = {};
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallAutomationEnabled')) {
      payload.missedCallAutomationEnabled = Boolean(body.missedCallAutomationEnabled);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallDelayMinutes')) {
      const delay = Number(body.missedCallDelayMinutes);
      if (!Number.isFinite(delay) || delay < 0 || delay > 1440) {
        return res.status(400).json({ success: false, error: 'missedCallDelayMinutes must be between 0 and 1440' });
      }
      payload.missedCallDelayMinutes = delay;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallTemplateName')) {
      const normalized = normalizeTemplateSelection(
        body.missedCallTemplateName,
        body.missedCallTemplateLanguage
      );
      payload.missedCallTemplateName = normalized.name;
      if (!Object.prototype.hasOwnProperty.call(body, 'missedCallTemplateLanguage')) {
        payload.missedCallTemplateLanguage = normalized.language;
      }
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallTemplateLanguage')) {
      payload.missedCallTemplateLanguage = String(body.missedCallTemplateLanguage || '').trim() || 'en_US';
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallTemplateVariables')) {
      payload.missedCallTemplateVariables = normalizeTemplateVariables(body.missedCallTemplateVariables);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallAutomationMode')) {
      payload.missedCallAutomationMode =
        String(body.missedCallAutomationMode || '').toLowerCase() === 'night_batch'
          ? 'night_batch'
          : 'immediate';
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallNightHour')) {
      const hour = Number(body.missedCallNightHour);
      if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
        return res.status(400).json({ success: false, error: 'missedCallNightHour must be between 0 and 23' });
      }
      payload.missedCallNightHour = Math.trunc(hour);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallNightMinute')) {
      const minute = Number(body.missedCallNightMinute);
      if (!Number.isFinite(minute) || minute < 0 || minute > 59) {
        return res.status(400).json({ success: false, error: 'missedCallNightMinute must be between 0 and 59' });
      }
      payload.missedCallNightMinute = Math.trunc(minute);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'missedCallTimezone')) {
      payload.missedCallTimezone = String(body.missedCallTimezone || '').trim() || 'Asia/Kolkata';
    }
    if (Object.keys(payload).length === 0) {
      return res.status(400).json({ success: false, error: 'No settings fields provided' });
    }
    const updated = await updateUserCredentialsByUserId(String(req.user.id), payload);
    if (!updated) {
      return res.status(502).json({ success: false, error: 'Failed to update settings in admin backend' });
    }
    return res.json({
      success: true,
      data: {
        missedCallAutomationEnabled: updated.missedCallAutomationEnabled !== false,
        missedCallDelayMinutes:
          Number.isFinite(Number(updated.missedCallDelayMinutes))
            ? Number(updated.missedCallDelayMinutes)
            : 5,
        missedCallTemplateName: String(updated.missedCallTemplateName || '').trim(),
        missedCallTemplateLanguage: String(updated.missedCallTemplateLanguage || 'en_US').trim() || 'en_US',
        missedCallTemplateVariables: normalizeTemplateVariables(updated.missedCallTemplateVariables),
        missedCallAutomationMode:
          String(updated.missedCallAutomationMode || 'immediate').toLowerCase() === 'night_batch'
            ? 'night_batch'
            : 'immediate',
        missedCallNightHour: toIntInRange(updated.missedCallNightHour, 21, 0, 23),
        missedCallNightMinute: toIntInRange(updated.missedCallNightMinute, 0, 0, 59),
        missedCallTimezone: String(updated.missedCallTimezone || 'Asia/Kolkata').trim() || 'Asia/Kolkata',
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/run-now', async (req, res) => {
  try {
    const row = await MissedCall.findOneAndUpdate(
      { _id: req.params.id, userId: req.user.id },
      {
        $set: {
          status: 'missed',
          'automation.enabled': true,
          'automation.status': 'pending',
          'automation.nextRunAt': new Date(),
          'automation.lastError': ''
        }
      },
      { new: true }
    );

    if (!row) {
      return res.status(404).json({ success: false, error: 'Missed call record not found' });
    }

    return res.json({ success: true, data: row });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/', async (req, res) => {
  try {
    const { status = 'all', search = '', startDate, endDate, limit = 100 } = req.query;
    const filters = { userId: req.user.id };

    if (status && status !== 'all') {
      filters.status = status;
    }

    if (startDate || endDate) {
      filters.calledAt = {};
      if (startDate) filters.calledAt.$gte = new Date(startDate);
      if (endDate) {
        const end = new Date(endDate);
        end.setHours(23, 59, 59, 999);
        filters.calledAt.$lte = end;
      }
    }

    if (search) {
      filters.$or = [
        { fromNumber: { $regex: search, $options: 'i' } },
        { toNumber: { $regex: search, $options: 'i' } },
        { callerName: { $regex: search, $options: 'i' } }
      ];
    }

    const rows = await MissedCall.find(filters)
      .sort({ calledAt: -1 })
      .limit(Math.min(Number(limit) || 100, 500))
      .lean();

    return res.json({ success: true, data: rows });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.put('/:id/resolve', async (req, res) => {
  try {
    const row = await MissedCall.findOneAndUpdate(
      { _id: req.params.id, userId: req.user.id },
      {
        $set: {
          status: 'resolved',
          resolvedAt: new Date(),
          notes: typeof req.body?.notes === 'string' ? req.body.notes : undefined
        }
      },
      { new: true }
    );

    if (!row) {
      return res.status(404).json({ success: false, error: 'Missed call record not found' });
    }

    return res.json({ success: true, data: row });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;

