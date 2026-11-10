const axios = require('axios');
const FormData = require('form-data');
const { normalizeTemplateMediaHandle } = require('../utils/templateMediaHandle');
const { isDebugLoggingEnabled } = require('../utils/securityConfig');
const META_REQUEST_TIMEOUT_MS = Number(process.env.WHATSAPP_META_REQUEST_TIMEOUT_MS || 12000);
const TEMPLATE_LIST_TIMEOUT_MS = Number(process.env.WHATSAPP_TEMPLATE_LIST_TIMEOUT_MS || 45000);
const debugLog = (...args) => {
  if (isDebugLoggingEnabled()) {
    console.log(...args);
  }
};

const maskPhoneNumber = (value) => {
  const digits = String(value || '').replace(/\D/g, '');
  return digits ? `***${digits.slice(-4)}` : null;
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

const normalizeMetaTemplateDeleteError = (body = {}, status = null) => {
  const source = body?.error && typeof body.error === 'object' ? body.error : body || {};
  return {
    status: Number.isFinite(Number(status)) ? Number(status) : null,
    message: String(source.message || body?.message || 'Meta template deletion failed'),
    type: source.type || null,
    code: source.code ?? null,
    error_subcode: source.error_subcode ?? null,
    error_user_title: source.error_user_title || null,
    error_user_msg: source.error_user_msg || null,
    is_transient: typeof source.is_transient === 'boolean' ? source.is_transient : null,
    error_data: source.error_data
      ? { details: source.error_data.details || null }
      : null,
    fbtrace_id: source.fbtrace_id || body?.fbtrace_id || null
  };
};
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

const getNumberedVariableCount = (text = '') => {
  const values = [...String(text || '').matchAll(/\{\{(\d+)\}\}/g)]
    .map((match) => Number(match[1]))
    .filter((value) => Number.isInteger(value) && value > 0);
  if (values.length === 0) return 0;
  const unique = [...new Set(values)].sort((a, b) => a - b);
  for (let index = 0; index < unique.length; index += 1) {
    if (unique[index] !== index + 1) {
      throw new Error(`Non-sequential numbered placeholders: expected {{${index + 1}}}, found {{${unique[index]}}}`);
    }
  }
  return unique.length;
};

const validateTemplateSendShape = (template, suppliedComponents, variables, templateName) => {
  const definition = Array.isArray(template?.components) ? template.components : [];
  const supplied = Array.isArray(suppliedComponents) ? suppliedComponents : [];
  const headerDefinition = definition.find((component) => String(component?.type || '').toUpperCase() === 'HEADER');
  const mediaType = String(headerDefinition?.format || '').toLowerCase();
  if (['image', 'video', 'document'].includes(mediaType)) {
    const headerPayload = supplied.find((component) => String(component?.type || '').toUpperCase() === 'HEADER');
    const hasMedia = (Array.isArray(headerPayload?.parameters) ? headerPayload.parameters : []).some((parameter) =>
      String(parameter?.type || '').toLowerCase() === mediaType &&
      Boolean(parameter?.[mediaType]?.id || parameter?.[mediaType]?.link)
    );
    if (!hasMedia) {
      throw new Error(`Template "${templateName}" HEADER component requires ${mediaType} media, but the send payload does not include it.`);
    }
  }
  for (const type of ['HEADER', 'BODY']) {
    const metaComponent = definition.find((component) => String(component?.type || '').toUpperCase() === type);
    const expected = getNumberedVariableCount(metaComponent?.text || '');
    const sentComponent = supplied.find((component) => String(component?.type || '').toUpperCase() === type);
    const sentCount = sentComponent
      ? (Array.isArray(sentComponent.parameters) ? sentComponent.parameters : []).filter((parameter) =>
          String(parameter?.type || '').toLowerCase() === 'text'
        ).length
      : (type === 'BODY' ? variables.length : 0);
    if (expected !== sentCount) {
      throw new Error(`Template "${templateName}" ${type} component expects ${expected} text parameter(s), but the send payload contains ${sentCount}. Refresh the template mapping and try again.`);
    }
    if (sentComponent) {
      const emptyTextParameter = (sentComponent.parameters || []).some((parameter) =>
        String(parameter?.type || '').toLowerCase() === 'text' &&
        !String(parameter?.text || '').trim()
      );
      if (emptyTextParameter) {
        throw new Error(`Template "${templateName}" ${type} component contains an empty text parameter. Provide a value for every template variable.`);
      }
    }
  }
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

const buildTemplateMessageComponents = (
  approvedTemplate = {},
  templateComponents = [],
  variables = [],
) => {
  const components = normalizeTemplateComponents(templateComponents);
  const bodyExists = components.some(
    (component) => String(component?.type || '').toUpperCase() === 'BODY'
  );
  const bodyDefinition = (Array.isArray(approvedTemplate?.components)
    ? approvedTemplate.components
    : []).find((component) => String(component?.type || '').toUpperCase() === 'BODY');
  const expectedBodyVariableCount = getNumberedVariableCount(bodyDefinition?.text || '');
  const bodyVariables = (Array.isArray(variables) ? variables : [])
    .slice(0, expectedBodyVariableCount);
  if (
    !bodyExists &&
    expectedBodyVariableCount > 0 &&
    (bodyVariables.length !== expectedBodyVariableCount ||
      bodyVariables.some((value) => !String(value ?? '').trim()))
  ) {
    throw new Error(`Template BODY requires ${expectedBodyVariableCount} non-empty text parameter(s).`);
  }
  if (!bodyExists && expectedBodyVariableCount > 0 && bodyVariables.length > 0) {
    const bodyComponent = {
      type: 'BODY',
      parameters: bodyVariables.map((value) => ({
        type: 'text',
        text: String(value ?? '').trim()
      }))
    };
    const headerIndex = components.findIndex(
      (component) => String(component?.type || '').toUpperCase() === 'HEADER'
    );
    components.splice(headerIndex >= 0 ? headerIndex + 1 : components.length, 0, bodyComponent);
  }
  return components;
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

const DEFAULT_COUNTRY_CODE = String(
  process.env.WHATSAPP_DEFAULT_COUNTRY_CODE || '91'
)
  .replace(/\D/g, '')
  .trim();

const normalizeWhatsAppPhone = (value = '') => {
  const rawValue = String(value || '').trim();
  const digits = rawValue.replace(/\D/g, '');
  let normalizedPhone = digits;

  if (!normalizedPhone) return '';

  if (digits.length === 10 && DEFAULT_COUNTRY_CODE) {
    return `${DEFAULT_COUNTRY_CODE}${digits}`;
  }

  return normalizedPhone;
};

const normalizeMetaSendError = (error, fallback = 'Failed to send WhatsApp message') => {
  const metaError = error?.response?.data?.error || {};
  const metaMessage =
    metaError?.message ||
    metaError?.error_user_msg ||
    error?.response?.data?.message ||
    '';
  const metaCode = String(metaError?.code || metaError?.error_subcode || '').trim();
  const metaDetails = String(
    metaError?.error_data?.details ||
      metaError?.error_user_title ||
      metaError?.fbtrace_id ||
      ''
  ).trim();
  const status = Number(error?.response?.status || 0) || null;
  const baseMessage = String(metaMessage || error?.message || fallback).trim();
  const parts = [
    baseMessage && baseMessage !== 'Error' ? baseMessage : fallback,
    metaCode ? `code ${metaCode}` : '',
    metaDetails ? `details ${metaDetails}` : '',
    status ? `status ${status}` : ''
  ].filter(Boolean);
  return parts.join(' | ');
};

class WhatsAppService {
  constructor() {
    this.apiUrl = 'https://graph.facebook.com/v20.0';
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
      return {
        success: true,
        data: response.data,
        providerMessageId: String(response.data?.messages?.[0]?.id || response.data?.messageId || '').trim()
      };
    } catch (error) {
      console.error('Error sending text message:', error.response?.data || error.message);
      return { 
        success: false, 
        error: normalizeMetaSendError(error, 'Failed to send text message')
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

    const normalizedTemplateName = String(templateName || '').trim();
    const normalizedProvidedComponents = normalizeTemplateComponents(templateComponents || []);

    try {
      if (!normalizedTemplateName) {
        return { success: false, error: 'Template name is required' };
      }

      // Validate against the live Meta definition before making a send request.
      // This catches stale campaign mappings before Meta returns error 132000.
      const templateList = await this.getTemplateList(credentials || null);
      if (!templateList?.success) {
        return { success: false, error: `Could not validate template "${normalizedTemplateName}" against Meta: ${templateList?.error || 'template catalog unavailable'}` };
      }
      const requestedLanguage = String(language || '').trim().toLowerCase();
      const approvedTemplate = (templateList.data?.data || []).find((candidate) =>
        String(candidate?.name || '').trim().toLowerCase() === normalizedTemplateName.toLowerCase() &&
        String(candidate?.language || '').trim().toLowerCase() === requestedLanguage &&
        String(candidate?.status || '').trim().toUpperCase() === 'APPROVED'
      );
      if (!approvedTemplate) {
        return { success: false, error: `Approved Meta template "${normalizedTemplateName}" (${language}) was not found in the connected WABA.` };
      }
      const components = buildTemplateMessageComponents(
        approvedTemplate,
        normalizedProvidedComponents,
        variables,
      );
      validateTemplateSendShape(approvedTemplate, components, [], normalizedTemplateName);

      const normalizedPhone = normalizeWhatsAppPhone(to);

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

      const diagnostic = {
        event: 'whatsapp_template_send',
        phase: 'request',
        wabaId: this.wabaId || null,
        phoneNumberId: this.phoneNumberId || null,
        recipient: maskPhoneNumber(normalizedPhone),
        templateName: normalizedTemplateName,
        language,
        componentTypes: components.map((component) => component.type),
        componentParameterCounts: components.map((component) => ({
          type: component.type,
          parameters: Array.isArray(component.parameters) ? component.parameters.length : 0
        }))
      };
      debugLog(JSON.stringify(diagnostic));

      const response = await axios.post(
        `${this.apiUrl}/${this.phoneNumberId}/messages`,
        payload,
        { headers: this.getHeaders() }
      );

      debugLog(JSON.stringify({
        ...diagnostic,
        phase: 'response',
        httpStatus: response.status,
        messageId: response.data?.messages?.[0]?.id || null,
        recipientWaId: maskPhoneNumber(response.data?.contacts?.[0]?.wa_id)
      }));

      return {
        success: true,
        data: response.data,
        providerMessageId: String(response.data?.messages?.[0]?.id || response.data?.messageId || '').trim()
      };
    } catch (error) {
      const metaError = error.response?.data?.error || {};
      debugLog(JSON.stringify({
        event: 'whatsapp_template_send',
        phase: 'meta_error',
        wabaId: this.wabaId || null,
        phoneNumberId: this.phoneNumberId || null,
        recipient: maskPhoneNumber(to),
        templateName: normalizedTemplateName,
        language,
        httpStatus: error.response?.status || null,
        metaCode: metaError.code || null,
        metaSubcode: metaError.error_subcode || null,
        metaType: metaError.type || null,
        message: metaError.message || error.message || 'Unknown Meta error',
        userMessage: metaError.error_user_msg || null,
        fbtraceId: metaError.fbtrace_id || null
      }));
      const metaMessage =
        metaError.message ||
        metaError.error_user_msg ||
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

              if (
                normalizedProvidedComponents.length === 0 &&
                !headerRequirements.requiresMedia &&
                headerRequirements.variableCount === 0 &&
                variables.length > requiredBodyVariables
              ) {
                return this.sendTemplateMessage(
                  to,
                  candidate.name || normalizedTemplateName,
                  candidate.language || language,
                  variables.slice(0, requiredBodyVariables),
                  credentials,
                  false,
                  null
                );
              }

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
      if (!normalizedError || normalizedError === 'Error') {
        normalizedError = normalizeMetaSendError(error, 'Failed to send template message');
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

  async uploadTemplateImage(file, credentials) {
    try {
      if (!file?.buffer?.length || !['image/jpeg', 'image/png'].includes(file.mimetype)) {
        return { success: false, error: 'Select a JPEG or PNG template image.' };
      }
      if (file.buffer.length > 5 * 1024 * 1024) {
        return { success: false, error: 'Template images must be 5 MB or smaller.' };
      }
      // Capture request credentials before awaiting; this service is shared across tenants.
      const accessToken = credentials?.accessToken;
      if (!accessToken) return { success: false, error: 'WhatsApp access token is required.' };
      const apiUrl = this.apiUrl;
      const session = await axios.post(`${apiUrl}/app/uploads`, null, {
        params: {
          file_name: file.originalname || 'template-image',
          file_length: file.buffer.length,
          file_type: file.mimetype
        },
        headers: { Authorization: `Bearer ${accessToken}` },
        timeout: META_REQUEST_TIMEOUT_MS
      });
      if (!session.data?.id) throw new Error('Meta did not return an image upload session.');
      const uploaded = await axios.post(`${apiUrl}/${session.data.id}`, file.buffer, {
        headers: {
          Authorization: `OAuth ${accessToken}`,
          'Content-Type': file.mimetype,
          file_offset: '0'
        },
        timeout: META_REQUEST_TIMEOUT_MS,
        maxBodyLength: 5 * 1024 * 1024
      });
      const headerHandle = normalizeTemplateMediaHandle(uploaded.data?.h);
      if (!headerHandle) throw new Error('Meta did not return a valid uploaded image handle.');
      return { success: true, data: { headerHandle } };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.error?.error_user_msg || error.response?.data?.error?.message || error.message,
        details: error.response?.data || null
      };
    }
  }

  async createTemplate(templateData, credentials = null) {
    if (credentials) {
      this.initialize(credentials);
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
    // This singleton serves multiple tenants. Snapshot request identity and
    // headers before awaiting so a concurrent request cannot switch WABA/token.
    const wabaId = String(credentials?.businessAccountId || this.wabaId || '').trim();
    const phoneNumberId = String(credentials?.phoneNumberId || this.phoneNumberId || '').trim();
    const accessToken = credentials?.accessToken || this.accessToken;
    const apiUrl = this.apiUrl;
    const headers = accessToken
      ? { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' }
      : null;
    try {
      if (!wabaId || !accessToken) {
        return {
          success: false,
          error: 'WhatsApp access token and WABA ID are required to fetch templates',
          wabaId: wabaId || null
        };
      }
      debugLog(`Fetching templates from: ${apiUrl}/${wabaId}/message_templates`);
      const endpoint = `${apiUrl}/${wabaId}/message_templates`;
      const params = {
        fields: 'id,name,language,status,category,components',
        limit: 200
      };
      let response = await axios.get(endpoint, {
        headers,
        timeout: TEMPLATE_LIST_TIMEOUT_MS,
        params
      });
      const firstPage = response.data || {};
      const templates = Array.isArray(firstPage.data) ? [...firstPage.data] : [];
      const seenCursors = new Set();
      let cursor = firstPage.paging?.cursors?.after || '';
      let pageCount = 1;

      // Meta paginates large WABAs. Follow cursor pages so callers receive the
      // complete catalog rather than only the first 200 templates.
      while (cursor && !seenCursors.has(cursor) && pageCount < 25) {
        seenCursors.add(cursor);
        response = await axios.get(endpoint, {
          headers,
          timeout: TEMPLATE_LIST_TIMEOUT_MS,
          params: { ...params, after: cursor }
        });
        const page = response.data || {};
        const pageTemplates = Array.isArray(page.data) ? page.data : [];
        templates.push(...pageTemplates);
        pageCount += 1;
        cursor = page.paging?.cursors?.after || '';
        if (pageTemplates.length === 0) break;
      }

      const lastPage = response.data || firstPage;
      const data = {
        ...firstPage,
        data: templates,
        paging: lastPage.paging || firstPage.paging
      };
      debugLog('Templates found across Meta pages:', templates.length);
      return { success: true, data, wabaId, phoneNumberId };
    } catch (error) {
      console.error('Error fetching templates:', error.response?.data || error.message);
      return { 
        success: false, 
        error: error.response?.data || error.message,
        wabaId,
        phoneNumberId
      };
    }
  }

  async deleteTemplateByName(templateName, credentials = null, metaTemplateId = '', debugContext = {}) {
    try {
      const normalizedName = String(templateName || '').trim();
      if (!normalizedName) {
        return { success: false, error: 'Template name is required' };
      }
      // Keep tenant credentials request-local; this service is a shared singleton.
      const wabaId = credentials?.businessAccountId;
      const accessToken = credentials?.accessToken;
      if (!wabaId || !accessToken) {
        return { success: false, error: 'Tenant WhatsApp Business Account ID and access token are required' };
      }
      const templateId = String(metaTemplateId || '').trim();
      if (!templateId) {
        return { success: false, error: 'Meta template ID (hsm_id) is required to safely delete one language version' };
      }

      // TEMPORARY TEMPLATE_DELETE_DEBUG: remove after tenant credential investigation.
      const endpointUrl = `${this.apiUrl}/${wabaId}/message_templates`;
      console.log('[TEMPLATE_DELETE_DEBUG]', {
        userId: debugContext.userId || null,
        businessName: credentials?.companyName || credentials?.username || null,
        credentialOwnerUserId: credentials?.credentialOwnerUserId || null,
        wabaId,
        phoneNumberId: credentials?.phoneNumberId || null,
        tokenConfigured: Boolean(accessToken),
        templateName: normalizedName,
        hsmId: templateId,
        credentialSource: debugContext.credentialSource || credentials?.credentialSource || 'unspecified',
        credentialsFromCache: debugContext.credentialsFromCache ?? null,
        credentialsFromFreshApi: debugContext.credentialsFromFreshApi ?? null,
        endpointUrl,
        apiVersion: this.apiUrl.match(/graph\.facebook\.com\/(v[\d.]+)/)?.[1] || null
      });

      const response = await axios.delete(
        endpointUrl,
        {
          headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
          params: { name: normalizedName, hsm_id: templateId },
          timeout: META_REQUEST_TIMEOUT_MS
        }
      );

      if (response.data?.success !== true) {
        const details = normalizeMetaTemplateDeleteError(response.data, response.status);
        console.error('[TEMPLATE_DELETE] Meta did not confirm deletion', details);
        return { success: false, error: details.message, details };
      }
      return { success: true, data: response.data };
    } catch (error) {
      const details = normalizeMetaTemplateDeleteError(
        error.response?.data || { message: error.message },
        error.response?.status,
      );
      const metaError = error.response?.data?.error;
      // TEMPORARY TEMPLATE_DELETE_DEBUG: allowlist fields so response payloads cannot expose credentials.
      console.error('[TEMPLATE_DELETE_DEBUG] Meta deletion failed', {
        status: error.response?.status ?? null,
        body: {
          message: error.response?.data?.message || null,
          error: metaError ? {
            message: metaError.message || null,
            type: metaError.type || null,
            code: metaError.code ?? null,
            error_subcode: metaError.error_subcode ?? null,
            error_data: metaError.error_data ? { details: metaError.error_data.details || null } : null
          } : null
        }
      });
      console.error('[META_TEMPLATE_DELETE] Meta API error', {
        message: metaError?.message || error.message,
        code: metaError?.code ?? null,
        error_subcode: metaError?.error_subcode ?? null,
        details: metaError?.error_data?.details ?? null
      });
      return {
        success: false,
        error: details.message,
        details
      };
    }
  }

}

module.exports = new WhatsAppService();
module.exports.buildTemplateMessageComponents = buildTemplateMessageComponents;
