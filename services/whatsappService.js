const axios = require('axios');
const FormData = require('form-data');
const { isDebugLoggingEnabled } = require('../utils/securityConfig');
const META_REQUEST_TIMEOUT_MS = Number(process.env.WHATSAPP_META_REQUEST_TIMEOUT_MS || 12000);

const debugLog = (...args) => {
  if (isDebugLoggingEnabled()) {
    console.log(...args);
  }
};

const truncateMediaDebugValue = (value, max = 120) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw.length > max ? `${raw.slice(0, max - 3)}...` : raw;
};

const emitMediaDebugLog = ({
  stage,
  level = 'info',
  requestId,
  conversationId,
  to,
  mediaType,
  file,
  cloudinary,
  meta,
  whatsapp,
  error,
  message,
  extra = {}
} = {}) => {
  const payload = {
    ts: new Date().toISOString(),
    event: 'media_pipeline',
    stage: truncateMediaDebugValue(stage || 'unknown', 80),
    requestId: truncateMediaDebugValue(requestId || '', 80) || null,
    message: truncateMediaDebugValue(message || '', 240) || null,
    conversationId: truncateMediaDebugValue(conversationId || '', 80) || null,
    to: truncateMediaDebugValue(to || '', 40) || null,
    mediaType: truncateMediaDebugValue(mediaType || '', 32) || null,
    file: file
      ? {
          name: truncateMediaDebugValue(file.originalname || file.name || '', 120),
          mimeType: truncateMediaDebugValue(file.mimetype || file.type || '', 80),
          sizeBytes: Number(file.size || file.buffer?.length || 0) || 0
        }
      : null,
    cloudinary: cloudinary
      ? {
          publicId: truncateMediaDebugValue(cloudinary.publicId || cloudinary.public_id || '', 180),
          resourceType: truncateMediaDebugValue(cloudinary.resourceType || cloudinary.resource_type || '', 24),
          format: truncateMediaDebugValue(cloudinary.format || '', 24),
          bytes: Number(cloudinary.bytes || 0) || 0
        }
      : null,
    meta: meta
      ? {
          mediaId: truncateMediaDebugValue(meta.id || meta.mediaId || '', 80),
          messageId: truncateMediaDebugValue(meta?.messages?.[0]?.id || meta.messageId || '', 80)
        }
      : null,
    whatsapp: whatsapp
      ? {
          messageId: truncateMediaDebugValue(whatsapp?.messages?.[0]?.id || whatsapp?.messageId || '', 80),
          status: truncateMediaDebugValue(whatsapp?.status || '', 24)
        }
      : null,
    error: error
      ? {
          message: truncateMediaDebugValue(
            error?.message || error?.response?.data?.error?.message || '',
            240
          ),
          status: Number(error?.status || error?.response?.status || 0) || null,
          code: truncateMediaDebugValue(
            error?.code || error?.response?.data?.error?.code || '',
            48
          ),
          upstreamMessage: truncateMediaDebugValue(
            error?.response?.data?.error?.message ||
              error?.response?.data?.error?.error_user_msg ||
              error?.response?.data?.message ||
              '',
            240
          )
        }
      : null,
    ...extra
  };

  const serialized = JSON.stringify(payload);
  if (level === 'error') {
    console.error(`[MEDIA_PIPELINE] ${serialized}`);
    return payload;
  }
  if (level === 'warn') {
    console.warn(`[MEDIA_PIPELINE] ${serialized}`);
    return payload;
  }
  console.info(`[MEDIA_PIPELINE] ${serialized}`);
  return payload;
};

const normalizeTemplateLookupValue = (value = '') => String(value || '').trim().toLowerCase();
const normalizeTemplateCompactKey = (value = '') =>
  normalizeTemplateLookupValue(value).replace(/[^a-z0-9]/g, '');

const findTemplateCandidate = (templates = [], requestedName = '') => {
  if (!Array.isArray(templates) || templates.length === 0) return null;

  const requested = normalizeTemplateLookupValue(requestedName);
  if (!requested) return null;

  const requestedUnderscore = requested.replace(/[\s-]+/g, '_');
  const requestedCompact = normalizeTemplateCompactKey(requested);

  return (
    templates.find((template) => {
      const templateName = normalizeTemplateLookupValue(template?.name || '');
      return templateName === requested || templateName === requestedUnderscore;
    }) ||
    templates.find((template) => {
      const templateName = normalizeTemplateCompactKey(template?.name || '');
      return templateName && templateName === requestedCompact;
    }) ||
    null
  );
};

const extractTemplateVariableCount = (text = '') => {
  const matches = [...String(text || '').matchAll(/\{\{(\d+)\}\}/g)];
  if (matches.length === 0) return 0;
  return matches.reduce((maxValue, match) => {
    const numericValue = Number(match?.[1] || 0);
    return Number.isFinite(numericValue) ? Math.max(maxValue, numericValue) : maxValue;
  }, 0);
};

const getHeaderRequirements = (template = {}) => {
  const headerComponent = Array.isArray(template?.components)
    ? template.components.find(
        (component) => String(component?.type || '').trim().toUpperCase() === 'HEADER'
      )
    : null;

  if (!headerComponent) {
    return { requiresMedia: false, mediaType: null, variableCount: 0 };
  }

  const headerFormat = String(headerComponent?.format || '').trim().toUpperCase();
  if (['IMAGE', 'VIDEO', 'DOCUMENT'].includes(headerFormat)) {
    return {
      requiresMedia: true,
      mediaType: headerFormat.toLowerCase(),
      variableCount: 0
    };
  }

  return {
    requiresMedia: false,
    mediaType: null,
    variableCount: extractTemplateVariableCount(headerComponent?.text || '')
  };
};

const normalizeTemplateComponents = (components = []) => {
  if (!Array.isArray(components)) return [];

  return components
    .map((component) => {
      const type = String(component?.type || '').trim().toUpperCase();
      if (!type) return null;

      const parameters = Array.isArray(component?.parameters)
        ? component.parameters
            .map((parameter) => {
              const parameterType = String(parameter?.type || '').trim().toLowerCase();
              if (!parameterType) return null;

              if (parameterType === 'text') {
                return {
                  type: 'text',
                  text: String(parameter?.text || '').trim()
                };
              }

              if (['image', 'video', 'document'].includes(parameterType)) {
                const mediaValue = parameter?.[parameterType] || {};
                const mediaId = String(mediaValue?.id || '').trim();
                const mediaLink = String(mediaValue?.link || '').trim();

                if (!mediaId && !mediaLink) return null;

                return {
                  type: parameterType,
                  [parameterType]: {
                    ...(mediaId ? { id: mediaId } : {}),
                    ...(mediaLink ? { link: mediaLink } : {})
                  }
                };
              }

              if (parameterType === 'payload') {
                return {
                  type: 'payload',
                  payload: String(parameter?.payload || '').trim()
                };
              }

              return null;
            })
            .filter(Boolean)
        : [];

      if (parameters.length === 0) return null;

      return {
        type,
        parameters
      };
    })
    .filter(Boolean);
};

const countTextParameters = (components = [], type = 'BODY') =>
  components.reduce((count, component) => {
    if (String(component?.type || '').toUpperCase() !== String(type || '').toUpperCase()) return count;
    const parameters = Array.isArray(component?.parameters) ? component.parameters : [];
    return (
      count +
      parameters.filter((parameter) => String(parameter?.type || '').toLowerCase() === 'text').length
    );
  }, 0);

const hasMediaHeaderParameter = (components = []) =>
  components.some((component) => {
    if (String(component?.type || '').toUpperCase() !== 'HEADER') return false;
    const parameters = Array.isArray(component?.parameters) ? component.parameters : [];
    return parameters.some((parameter) =>
      ['image', 'video', 'document'].includes(String(parameter?.type || '').toLowerCase())
    );
  });

const normalizeWhatsAppPhone = (value = '') => {
  let normalizedPhone = String(value || '').replace(/[^\d+]/g, '');
  if (!normalizedPhone.startsWith('+')) {
    normalizedPhone = '+' + normalizedPhone;
  }
  return normalizedPhone;
};

class WhatsAppService {
  constructor() {
    this.apiUrl = 'https://graph.facebook.com/v20.0';
    this.mockMode = process.env.WHATSAPP_MOCK_MODE === 'true';
  }

  // Initialize service with user-specific credentials
  initialize(credentials) {
    this.wabaId = credentials.businessAccountId;
    this.phoneNumberId = credentials.phoneNumberId;
    this.accessToken = credentials.accessToken;
    this.webhookVerifyToken = credentials.webhookVerifyToken;
  }

  getHeaders() {
    if (!this.accessToken) {
      throw new Error('WhatsApp service not initialized with credentials');
    }
    return {
      'Authorization': `Bearer ${this.accessToken}`,
      'Content-Type': 'application/json'
    };
  }

  async sendTextMessage(to, text, credentials = null, options = {}) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      debugLog(`ðŸ§ª MOCK MODE: Simulating text message to ${to}: "${text}"`);
      return { 
        success: true, 
        data: { 
          messageId: 'mock_' + Date.now(),
          status: 'sent'
        } 
      };
    }

    try {
      // WhatsApp API requires phone numbers in E.164 format with + prefix and no spaces/special chars
      const normalizedPhone = normalizeWhatsAppPhone(to);
      
      debugLog(`ðŸ“ž Normalized phone number: ${to} -> ${normalizedPhone}`);
      
      const replyContextMessageId = String(options?.whatsappContextMessageId || '').trim();
      const payload = {
        messaging_product: 'whatsapp',
        to: normalizedPhone,
        type: 'text',
        text: { body: text }
      };

      if (replyContextMessageId) {
        payload.context = {
          message_id: replyContextMessageId
        };
      }

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error sending text message:', error.response?.data || error.message);
      const normalizedError =
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        error.message ||
        'Failed to send text message';
      return { 
        success: false, 
        error: normalizedError
      };
    }
  }

  async sendTemplateMessage(
    to,
    templateName,
    language = 'en_US',
    variables = [],
    credentials = null,
    retryOnNotFound = true,
    templateComponents = null
  ) {
    if (credentials) {
      this.initialize(credentials);
    }

    if (this.mockMode) {
      debugLog('MOCK MODE: Simulating template message send');
      debugLog('   to:', to);
      debugLog('   template:', templateName);
      debugLog('   language:', language);
      return {
        success: true,
        data: {
          messageId: 'mock_template_' + Date.now(),
          status: 'sent',
          template: templateName
        }
      };
    }

    const normalizedTemplateName = String(templateName || '').trim();
    const normalizedProvidedComponents = normalizeTemplateComponents(templateComponents || []);

    try {
      if (!normalizedTemplateName) {
        return { success: false, error: 'Template name is required' };
      }

      const normalizedPhone = normalizeWhatsAppPhone(to);

      let components = [];
      if (normalizedProvidedComponents.length > 0) {
        components = normalizedProvidedComponents;
      } else if (variables.length > 0) {
        components = [
          {
            type: 'BODY',
            parameters: variables.map((value) => ({
              type: 'text',
              text: String(value ?? '').trim()
            }))
          }
        ];
      }

      const payload = {
        messaging_product: 'whatsapp',
        to: normalizedPhone,
        type: 'template',
        template: {
          name: normalizedTemplateName,
          language: { code: language }
        }
      };

      if (components.length > 0) {
        payload.template.components = components;
      }

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );

      return { success: true, data: response.data };
    } catch (error) {
      const metaMessage =
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        '';
      const metaCode = Number(error.response?.data?.error?.code || 0);

      if (retryOnNotFound && /template .* not found/i.test(metaMessage)) {
        try {
          const listResult = await this.getTemplateList(credentials || null);
          if (listResult.success) {
            const templates = listResult.data?.data || [];
            const candidate = findTemplateCandidate(templates, normalizedTemplateName);
            if (candidate?.name && candidate.name !== normalizedTemplateName) {
              return this.sendTemplateMessage(
                to,
                candidate.name,
                language,
                variables,
                credentials,
                false,
                normalizedProvidedComponents
              );
            }
          }
        } catch (retryError) {
          console.error('Template retry resolution failed:', retryError.message);
        }
      }

      const shouldRetryOnLanguage =
        /language/i.test(metaMessage) &&
        (/translation/i.test(metaMessage) || /does not exist/i.test(metaMessage));

      if (retryOnNotFound && shouldRetryOnLanguage) {
        try {
          const listResult = await this.getTemplateList(credentials || null);
          if (listResult.success) {
            const templates = listResult.data?.data || [];
            const sameNameCandidates = templates.filter((template) =>
              Boolean(findTemplateCandidate([template], normalizedTemplateName))
            );
            const candidate = sameNameCandidates.find(
              (template) =>
                String(template?.language || '').trim().toLowerCase() !==
                String(language || '').trim().toLowerCase()
            );
            if (candidate?.language) {
              return this.sendTemplateMessage(
                to,
                normalizedTemplateName,
                candidate.language,
                variables,
                credentials,
                false,
                normalizedProvidedComponents
              );
            }
          }
        } catch (retryError) {
          console.error('Template language retry resolution failed:', retryError.message);
        }
      }

      const shouldInspectTemplateShape =
        retryOnNotFound &&
        (metaCode === 132000 ||
          /header/i.test(metaMessage) ||
          /components?/i.test(metaMessage) ||
          /parameter/i.test(metaMessage) ||
          /media/i.test(metaMessage) ||
          /format/i.test(metaMessage));

      if (shouldInspectTemplateShape) {
        try {
          const listResult = await this.getTemplateList(credentials || null);
          if (listResult.success) {
            const templates = listResult.data?.data || [];
            const candidate = findTemplateCandidate(templates, normalizedTemplateName);

            if (candidate) {
              const headerRequirements = getHeaderRequirements(candidate);
              const bodyComponent = Array.isArray(candidate?.components)
                ? candidate.components.find(
                    (component) => String(component?.type || '').trim().toUpperCase() === 'BODY'
                  )
                : null;

              const requiredBodyVariables = extractTemplateVariableCount(bodyComponent?.text || '');
              const providedHeaderMedia = hasMediaHeaderParameter(normalizedProvidedComponents);
              const providedHeaderTextCount = countTextParameters(normalizedProvidedComponents, 'HEADER');
              const providedBodyTextCount =
                countTextParameters(normalizedProvidedComponents, 'BODY') || variables.length;

              if (headerRequirements.requiresMedia && !providedHeaderMedia) {
                return {
                  success: false,
                  error: `Template "${candidate.name}" requires a ${headerRequirements.mediaType} header media URL. Add it in Send Template modal and try again.`
                };
              }

              if (
                headerRequirements.variableCount > 0 &&
                providedHeaderTextCount < headerRequirements.variableCount
              ) {
                return {
                  success: false,
                  error: `Template "${candidate.name}" requires ${headerRequirements.variableCount} header variable(s), but only ${providedHeaderTextCount} were provided.`
                };
              }

              if (requiredBodyVariables > 0 && providedBodyTextCount < requiredBodyVariables) {
                return {
                  success: false,
                  error: `Template "${candidate.name}" requires ${requiredBodyVariables} body variable(s), but only ${providedBodyTextCount} were provided.`
                };
              }
            }
          }
        } catch (shapeError) {
          console.error('Template requirement inspection failed:', shapeError.message);
        }
      }

      let normalizedError = metaMessage;
      if (!normalizedError) {
        const rawError = error.response?.data || error.message;
        if (typeof rawError === 'string') {
          normalizedError = rawError;
        } else {
          try {
            normalizedError = JSON.stringify(rawError);
          } catch (_stringifyError) {
            normalizedError = 'Unknown WhatsApp API error';
          }
        }
      }

      return {
        success: false,
        error: normalizedError
      };
    }
  }

  async sendMediaMessage(to, mediaType, mediaUrl, caption = '', credentials = null, options = {}) {
    if (credentials) {
      this.initialize(credentials);
    }
    let normalizedPhone = '';
    let normalizedMediaType = '';
    try {
      normalizedPhone = normalizeWhatsAppPhone(to);
      normalizedMediaType = String(mediaType || '').trim().toLowerCase();
      const normalizedCaption = String(caption || '').trim();
      const normalizedFileName = String(options?.fileName || '').trim();
      const normalizedMediaId = String(options?.mediaId || '').trim();
      const allowLinkFallback = Boolean(options?.allowLinkFallback);
      const normalizedMediaUrl = String(mediaUrl || '').trim();
      const debugContext = options?.debugContext || {};
      emitMediaDebugLog({
        stage: 'whatsapp_send_attempt',
        requestId: debugContext.requestId,
        conversationId: debugContext.conversationId,
        to: normalizedPhone,
        mediaType: normalizedMediaType,
        file: debugContext.file,
        cloudinary: debugContext.cloudinary,
        meta: debugContext.meta,
        message: 'Sending WhatsApp media message'
      });

      if (!['image', 'audio', 'document'].includes(normalizedMediaType)) {
        emitMediaDebugLog({
          stage: 'whatsapp_send_validation_failed',
          level: 'warn',
          requestId: debugContext.requestId,
          conversationId: debugContext.conversationId,
          to: normalizedPhone,
          mediaType: normalizedMediaType,
          file: debugContext.file,
          message: 'Outbound media type is disabled'
        });
        return {
          success: false,
          error: 'This website only supports sending images, audio, and documents.'
        };
      }

      if (!normalizedMediaId && !allowLinkFallback) {
        emitMediaDebugLog({
          stage: 'whatsapp_send_validation_failed',
          level: 'warn',
          requestId: debugContext.requestId,
          conversationId: debugContext.conversationId,
          to: normalizedPhone,
          mediaType: normalizedMediaType,
          file: debugContext.file,
          message: 'WhatsApp media_id missing and link fallback disabled'
        });
        return {
          success: false,
          error: 'A WhatsApp media_id is required for outbound media sending.'
        };
      }

      const payload = {
        messaging_product: 'whatsapp',
        to: normalizedPhone,
        type: normalizedMediaType,
        [normalizedMediaType]: normalizedMediaId
          ? { id: normalizedMediaId }
          : { link: normalizedMediaUrl }
      };

      if (normalizedCaption && normalizedMediaType !== 'audio' && normalizedMediaType !== 'sticker') {
        payload[normalizedMediaType].caption = normalizedCaption;
      }

      if (normalizedMediaType === 'document' && normalizedFileName) {
        payload.document.filename = normalizedFileName;
      }

      const replyContextMessageId = String(options?.whatsappContextMessageId || '').trim();
      if (replyContextMessageId) {
        payload.context = {
          message_id: replyContextMessageId
        };
      }

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );

      emitMediaDebugLog({
        stage: 'whatsapp_send_success',
        requestId: debugContext.requestId,
        conversationId: debugContext.conversationId,
        to: normalizedPhone,
        mediaType: normalizedMediaType,
        file: debugContext.file,
        meta: debugContext.meta,
        whatsapp: response?.data,
        message: 'WhatsApp media message sent'
      });
      
      return { success: true, data: response.data };
    } catch (error) {
      const debugContext = options?.debugContext || {};
      emitMediaDebugLog({
        stage: 'whatsapp_send_failed',
        level: 'error',
        requestId: debugContext.requestId,
        conversationId: debugContext.conversationId,
        to: normalizedPhone,
        mediaType: normalizedMediaType,
        file: debugContext.file,
        cloudinary: debugContext.cloudinary,
        meta: debugContext.meta,
        error,
        message: 'Failed to send WhatsApp media message'
      });
      console.error('Error sending media message:', error.response?.data || error.message);
      const normalizedError =
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        error.message ||
        'Failed to send media message';
      return { 
        success: false, 
        error: normalizedError
      };
    }
  }

  async uploadMediaAsset(file, credentials = null, options = {}) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      return {
        success: true,
        data: {
          id: `mock_media_upload_${Date.now()}`
        }
      };
    }

    try {
      if (!file?.buffer) {
        return {
          success: false,
          error: 'Attachment file buffer is required for WhatsApp media upload'
        };
      }

      const debugContext = options?.debugContext || {};
      emitMediaDebugLog({
        stage: 'whatsapp_media_upload_attempt',
        requestId: debugContext.requestId,
        conversationId: debugContext.conversationId,
        to: debugContext.to,
        mediaType: debugContext.mediaType,
        file,
        cloudinary: debugContext.cloudinary,
        message: 'Uploading media to WhatsApp media endpoint'
      });

      const form = new FormData();
      form.append('messaging_product', 'whatsapp');
      const normalizedMimeType = String(file?.mimetype || '')
        .trim()
        .toLowerCase()
        .split(';')[0]
        .trim();
      form.append('file', file.buffer, {
        filename: String(file?.originalname || 'attachment').trim() || 'attachment',
        contentType: normalizedMimeType || 'application/octet-stream',
        knownLength: Number(file?.size || file?.buffer?.length || 0)
      });

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/media`,
        form,
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
            ...form.getHeaders()
          },
          maxBodyLength: Infinity
        }
      );

      emitMediaDebugLog({
        stage: 'whatsapp_media_upload_success',
        requestId: debugContext.requestId,
        conversationId: debugContext.conversationId,
        to: debugContext.to,
        mediaType: debugContext.mediaType,
        file,
        meta: response?.data,
        message: 'Media uploaded to WhatsApp'
      });

      return { success: true, data: response.data };
    } catch (error) {
      const debugContext = options?.debugContext || {};
      emitMediaDebugLog({
        stage: 'whatsapp_media_upload_failed',
        level: 'error',
        requestId: debugContext.requestId,
        conversationId: debugContext.conversationId,
        to: debugContext.to,
        mediaType: debugContext.mediaType,
        file,
        error,
        message: 'Failed to upload media to WhatsApp'
      });
      console.error('Error uploading media asset:', {
        response: error.response?.data || null,
        message: error.message,
        status: Number(error?.response?.status || 0) || null,
        code: String(error?.code || '').trim() || null
      });
      const normalizedError =
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        error.message ||
        'Failed to upload media asset';
      const errorCode =
        String(error.response?.data?.error?.code || error.response?.data?.error?.error_subcode || '')
          .trim() || '';
      const errorDetails =
        String(
          error.response?.data?.error?.error_data?.details ||
            error.response?.data?.error?.error_user_title ||
            error.response?.data?.error?.fbtrace_id ||
            ''
        ).trim() || '';
      return {
        success: false,
        error: normalizedError,
        errorCode: errorCode || null,
        errorDetails: errorDetails || null,
        response: error.response?.data || null
      };
    }
  }

  async sendReactionMessage(to, targetMessageId, emoji = '', credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      return {
        success: true,
        data: {
          messageId: 'mock_reaction_' + Date.now(),
          status: 'sent'
        }
      };
    }

    try {
      const normalizedPhone = normalizeWhatsAppPhone(to);

      const normalizedTargetMessageId = String(targetMessageId || '').trim();
      const normalizedEmoji = String(emoji || '').trim();
      if (!normalizedTargetMessageId) {
        return {
          success: false,
          error: 'A target WhatsApp message ID is required to send a reaction'
        };
      }

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        {
          messaging_product: 'whatsapp',
          to: normalizedPhone,
          type: 'reaction',
          reaction: {
            message_id: normalizedTargetMessageId,
            emoji: normalizedEmoji
          }
        },
        { headers: this.getHeaders() }
      );

      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error sending reaction message:', error.response?.data || error.message);
      const normalizedError =
        error.response?.data?.error?.message ||
        error.response?.data?.error?.error_user_msg ||
        error.message ||
        'Failed to send reaction message';
      return {
        success: false,
        error: normalizedError
      };
    }
  }

  async markMessageAsRead(messageId, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
    try {
      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        {
          messaging_product: 'whatsapp',
          status: 'read',
          message_id: messageId
        },
        { headers: this.getHeaders() }
      );
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error marking message as read:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }

  async createTemplate(templateData, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
    if (this.mockMode) {
      debugLog(`ðŸ§ª MOCK MODE: Simulating template creation:`, templateData);
      return { 
        success: true, 
        data: { 
          id: 'mock_template_' + Date.now(),
          name: templateData.name,
          status: 'PENDING'
        } 
      };
    }

    try {
      debugLog(`ðŸ“ Creating template: ${templateData.name}`);
      
      // Meta API format for template creation
      const metaTemplateData = {
        name: templateData.name,
        category: templateData.category,
        language: templateData.language,
        components: templateData.components || []
      };

      const response = await axios.post(
        `${this.apiUrl}/${this.wabaId}/message_templates`,
        metaTemplateData,
        { headers: this.getHeaders() }
      );
      
      debugLog('âœ… Template created successfully:', response.data);
      return { 
        success: true, 
        data: response.data 
      };
    } catch (error) {
      console.error('âŒ Failed to create template:', error.response?.data || error.message);
      return { 
        success: false, 
        error: (error.response?.data?.error?.message || error.response?.data?.message || error.message || 'Failed to create template'),
        details: error.response?.data || null
      };
    }
  }

  async getTemplateList(credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }
    try {
      if (!this.wabaId) {
        return {
          success: false,
          error: 'WHATSAPP_BUSINESS_ACCOUNT_ID (WABA ID) is not configured'
        };
      }
      debugLog(`Fetching templates from: ${this.apiUrl}/${this.wabaId}/message_templates`);
      const response = await axios.get(
        `${this.apiUrl}/${this.wabaId}/message_templates`,
        {
          headers: this.getHeaders(),
          timeout: META_REQUEST_TIMEOUT_MS,
          params: {
            fields: 'id,name,language,status,category,components',
            limit: 200
          }
        }
      );
      debugLog('Raw Meta API Response:', JSON.stringify(response.data, null, 2));
      debugLog('Templates found:', response.data.data?.length || 0);
      return { success: true, data: response.data };
    } catch (error) {
      console.error('Error fetching templates:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message 
      };
    }
  }

  async deleteTemplateByName(templateName, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
    }

    try {
      const normalizedName = String(templateName || '').trim();
      if (!normalizedName) {
        return { success: false, error: 'Template name is required' };
      }

      const response = await axios.delete(
        `${this.apiUrl}/${this.wabaId}/message_templates`,
        {
          headers: this.getHeaders(),
          params: { name: normalizedName }
        }
      );

      return { success: true, data: response.data };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.error?.message || error.message
      };
    }
  }

}

module.exports = new WhatsAppService();
