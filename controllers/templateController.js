const Template = require('../models/Template');
const { buildTenantResourceFilter } = require('../utils/accessControl');
const whatsappService = require('../services/whatsappService');
const { getWhatsAppCredentialsForUser } = require('../services/userWhatsAppCredentialsService');
const { uploadCampaignCreative } = require('../utils/cloudinaryUpload');
const { resolveCompanyFolders } = require('../services/cloudinaryCompanyFolders');
const { normalizeTemplateMediaHandle } = require('../utils/templateMediaHandle');

const normalizeTemplateLookupValue = (value = '') => String(value || '').trim().toLowerCase();

const normalizeTemplateButtonType = (value = '') => String(value || '').trim().toLowerCase();

const normalizeTemplateButtons = (buttons = []) =>
  (Array.isArray(buttons) ? buttons : []).map((button) => ({
    ...button,
    type: normalizeTemplateButtonType(button?.type),
    phoneNumber: String(button?.phoneNumber || button?.phone_number || '').trim()
  }));

const isMetaDuplicateTemplateError = (result = {}) => {
  const message = [
    result.error,
    result.details?.error?.message,
    result.details?.error?.error_user_msg,
    result.details?.message
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return /already exists|duplicate|name.*taken|template.*exist/.test(message);
};

const findExistingMetaTemplate = (templates = [], name = '', language = '') => {
  const normalizedName = normalizeTemplateLookupValue(name);
  const normalizedLanguage = normalizeTemplateLookupValue(language);

  return (Array.isArray(templates) ? templates : []).find((template) => {
    const templateName = normalizeTemplateLookupValue(template?.name);
    const templateLanguage = normalizeTemplateLookupValue(template?.language);
    return (
      templateName === normalizedName &&
      (!normalizedLanguage || templateLanguage === normalizedLanguage)
    );
  });
};

const buildLocalTemplateContentFromMeta = (components = []) => {
  const safeComponents = Array.isArray(components) ? components : [];
  const headerComponent = safeComponents.find((component) => component?.type === 'HEADER');
  const bodyComponent = safeComponents.find((component) => component?.type === 'BODY');
  const footerComponent = safeComponents.find((component) => component?.type === 'FOOTER');
  const buttonsComponent = safeComponents.find((component) => component?.type === 'BUTTONS');

  return {
    header: {
      type: headerComponent ? String(headerComponent.format || 'text').toLowerCase() : 'text',
      text: headerComponent?.text || '',
      mediaUrl: ''
    },
    body: bodyComponent?.text || '',
    footer: footerComponent?.text || '',
    buttons: normalizeTemplateButtons(buttonsComponent?.buttons || [])
  };
};

const prepareMetaTemplateText = (value = '') => {
  // Preserve the submitted text exactly. Dynamic variables are recognized only
  // from explicit numbered placeholders; phone numbers and URLs stay static.
  return { text: String(value ?? ''), examples: [] };
};

class TemplateController {
  async uploadTemplateImage(req, res) {
    try {
      if (!req.file) return res.status(400).json({ success: false, error: 'Template image file is required.' });
      const result = await whatsappService.uploadTemplateImage(req.file, req.whatsappCredentials);
      if (!result.success) return res.status(400).json(result);
      const folder = resolveCompanyFolders({
        companyId: req.companyId || req.user?.companyId,
        companyName: req.user?.companyName || '',
        companySlug: req.user?.companySlug || '',
        cloudinaryFolderRoot: req.user?.cloudinaryFolderRoot || ''
      }).metaTemplateImagesFolder;
      const mediaUrl = await uploadCampaignCreative(req.file, { folder, resourceType: 'image' });
      return res.json({ success: true, data: { ...result.data, mediaUrl } });
    } catch (error) {
      return res.status(error.status || 500).json({ success: false, error: error.message });
    }
  }

    // Helper function to extract variables from template text
  extractVariables(text) {
        if (!text) return [];
        
        const variablePattern = /\{\{(\d+)\}\}/g;
        const variables = [];
        let match;
        
        const seenVariables = new Set();
        while ((match = variablePattern.exec(text)) !== null) {
            if (seenVariables.has(match[1])) continue;
            seenVariables.add(match[1]);
            variables.push({
                name: `var${match[1]}`,
                example: `Example ${match[1]}`,
                required: false
            });
        }
        
        return variables;
    }

  buildMetaSafeText(text) {
    return prepareMetaTemplateText(text).text;
  }
  async getAllTemplates(req, res) {
    try {
      const { status, isActive, category } = req.query;
      const filters = buildTenantResourceFilter({ req, ownerField: 'userId' });
      
      if (status) filters.status = status;
      if (isActive !== undefined) filters.isActive = isActive === 'true';
      if (category) filters.category = category;

      const templates = await Template.find(filters).sort({ createdAt: -1 });
      res.json({ success: true, data: templates });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getTemplateById(req, res) {
    try {
      const template = await Template.findOne(buildTenantResourceFilter({ req, ownerField: 'userId', base: { _id: req.params.id } }));
      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      res.json({ success: true, data: template });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async createTemplate(req, res) {
    let localTemplate = null;
    try {
      const { name, category, language, content, type = 'custom', components } = req.body;
      const normalizedName = String(name || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_]/g, '')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '');

      if (!normalizedName || (!content && !components)) {
        return res.status(400).json({
          success: false,
          error: 'Template name and content/components are required'
        });
      }

      if (!/^[a-z0-9](?:[a-z0-9_]*[a-z0-9])?$/.test(normalizedName)) {
        return res.status(400).json({
          success: false,
          error: 'Template name must use lowercase letters, numbers, and underscores only, and cannot start or end with underscore.'
        });
      }

      let templateContent = content;
      let bodyText = '';

      // The editor sends `components: []` when it is retrying a saved template.
      // Treat an empty array as absent so the saved content is used to build
      // the Meta components instead of replacing the body with an empty string.
      if (Array.isArray(components) && components.length > 0) {
        const bodyComponent = components.find(comp => comp.type === 'BODY');
        bodyText = bodyComponent ? bodyComponent.text : '';

        const headerComponent = components.find(comp => comp.type === 'HEADER');
        const footerComponent = components.find(comp => comp.type === 'FOOTER');
        const buttonsComponent = components.find(comp => comp.type === 'BUTTONS');

        templateContent = {
          header: {
            type: headerComponent ? (headerComponent.format || 'text').toLowerCase() : 'text',
            text: headerComponent?.text || '',
            mediaUrl: content?.header?.mediaUrl || '',
            mediaHandle: headerComponent?.format === 'IMAGE' ? (headerComponent.example?.header_handle?.[0] || '') : ''
          },
          body: bodyText,
          footer: footerComponent?.text || '',
          buttons: normalizeTemplateButtons(buttonsComponent?.buttons || [])
        };
      }

      if (templateContent?.buttons) {
        templateContent.buttons = normalizeTemplateButtons(templateContent.buttons);
      }

      const imageHeader = Array.isArray(components)
        ? components.find(component => String(component?.type).toUpperCase() === 'HEADER' && String(component?.format).toUpperCase() === 'IMAGE')
        : null;
      const imageHandle = normalizeTemplateMediaHandle(imageHeader?.example?.header_handle || templateContent?.header?.mediaHandle);
      if (templateContent?.header?.type === 'image' && !imageHandle) {
        return res.status(400).json({
          success: false,
          error: 'Upload a JPEG or PNG image first and use the returned Meta headerHandle for the image header.'
        });
      }

      if (templateContent?.header?.type === 'image') {
        templateContent.header.mediaHandle = imageHandle;
      }
      const preparedBody = prepareMetaTemplateText(templateContent?.body || bodyText);
      const bodyForVariables = preparedBody.text;
      const bodyVariableCount = (bodyForVariables.match(/\{\{\d+\}\}/g) || []).length;
      const fixedBodyWordCount = bodyForVariables.trim().split(/\s+/).filter(Boolean).length - bodyVariableCount;
      const minimumFixedBodyWords = (2 * bodyVariableCount) + 1;
      if (!bodyForVariables.replace(/\{\{\d+\}\}/g, '').trim()) {
        return res.status(400).json({
          success: false,
          error: 'Template body must include regular text in addition to variables.'
        });
      }
      if (bodyVariableCount > 0 && fixedBodyWordCount < minimumFixedBodyWords) {
        return res.status(400).json({
          success: false,
          error: `Template body has too many variables for its length. Add more regular words or use fewer variables (aim for at least ${minimumFixedBodyWords} non-variable words for ${bodyVariableCount} variables).`
        });
      }
      const bodyExampleRows = Array.isArray(components)
        ? (components.find(component => String(component?.type || '').toUpperCase() === 'BODY')?.example?.body_text || [])
        : [];
      const bodyExamples = Array.isArray(bodyExampleRows[0]) ? bodyExampleRows[0] : [];
      const variables = this.extractVariables(bodyForVariables).map((variable, index) => {
        const variableNumber = Number(String(variable.name || '').match(/\d+/)?.[0] || index + 1);
        const example = String(bodyExamples[variableNumber - 1] || preparedBody.examples[index] || '').trim();
        return example ? { ...variable, example } : variable;
      });
      const preservedBody = bodyForVariables;

      const templateData = {
        userId: req.user.id,
        companyId: req.companyId,
        name: normalizedName,
        type,
        category: category || 'marketing',
        language: language || 'en_US',
        content: templateContent,
        variables,
        status: 'pending',
        isActive: false,
        createdBy: req.user.username || req.user.email || req.user.id,
        createdById: req.user.id
      };

      const componentsForMeta = Array.isArray(components) && components.length > 0
        ? components
            .map((component) => {
              const componentType = String(component?.type || '').trim().toUpperCase();
              if (!componentType) return null;

              if (componentType === 'BODY') {
                const bodyTextForMeta = prepareMetaTemplateText(component?.text || bodyForVariables).text;
                if (!bodyTextForMeta) return null;

                return {
                  ...component,
                  type: 'BODY',
                  text: bodyTextForMeta
                };
              }

              if (componentType === 'HEADER') {
                const headerFormat = String(component?.format || '').trim().toUpperCase();
                return {
                  ...component,
                  type: 'HEADER',
                  ...(headerFormat ? { format: headerFormat } : {}),
                  ...(headerFormat === 'IMAGE' ? { example: { header_handle: [imageHandle] } } : {}),
                  ...(headerFormat === 'TEXT'
                    ? { text: String(component?.text || '').trim() }
                    : {})
                };
              }

              if (componentType === 'FOOTER') {
                return {
                  ...component,
                  type: 'FOOTER',
                  text: String(component?.text || '').trim()
                };
              }

              return {
                ...component,
                type: componentType
              };
            })
            .filter(Boolean)
        : [
            ...(templateContent?.header?.type === 'image'
              ? [{ type: 'HEADER', format: 'IMAGE', example: { header_handle: [imageHandle] } }]
              : templateContent?.header?.text
              ? [{
                  type: 'HEADER',
                  format: 'TEXT',
                  text: String(templateContent.header.text || '').trim()
                }]
              : []),
            {
              type: 'BODY',
              text: preservedBody,
              ...(preparedBody.examples.length > 0
                ? {
                    example: {
                      body_text: [preparedBody.examples]
                    }
                  }
                : {})
            },
            ...(templateContent?.footer
              ? [{
                  type: 'FOOTER',
                  text: String(templateContent.footer || '').trim()
                }]
              : [])
          ];

      const templateScopeFilter = {
        userId: req.user.id,
        companyId: req.companyId,
        name: normalizedName
      };

      const existingTemplate = req.templateToRetry || await Template.findOne(templateScopeFilter);
      let template;

      if (existingTemplate) {
        existingTemplate.set({
          ...templateData,
          whatsappTemplateId: null
        });
        template = await existingTemplate.save();
      } else {
        template = await Template.create({
          ...templateData,
          whatsappTemplateId: null
        });
      }
      localTemplate = template;

      const metaResult = await whatsappService.createTemplate({
        name: templateData.name,
        category: templateData.category.toUpperCase(),
        language: templateData.language,
        components: componentsForMeta
      }, req.whatsappCredentials);

      let metaTemplateId = metaResult.data?.id || null;
      let existingMetaTemplate = null;

      if (!metaResult.success && isMetaDuplicateTemplateError(metaResult)) {
        const listResult = await whatsappService.getTemplateList(req.whatsappCredentials);
        if (listResult.success) {
          existingMetaTemplate = findExistingMetaTemplate(
            listResult.data?.data || [],
            templateData.name,
            templateData.language
          );
          if (existingMetaTemplate) {
            metaTemplateId = existingMetaTemplate.id || null;
          }
        }
      }

      if (existingMetaTemplate) {
        templateData.category = existingMetaTemplate.category || templateData.category;
        templateData.language = existingMetaTemplate.language || templateData.language;
        templateData.status = String(existingMetaTemplate.status || 'pending').toLowerCase();
        templateData.isActive = templateData.status === 'approved';
        templateData.content = buildLocalTemplateContentFromMeta(existingMetaTemplate.components) || templateData.content;
        templateData.variables = this.extractVariables(templateData.content?.body || '');
      }

      const metaSubmissionFailed = !metaResult.success && !existingMetaTemplate;
      if (metaSubmissionFailed) {
        // A missing Meta ID means this record must not look like a template that
        // is still awaiting Meta review. It is a failed submission that can be
        // retried or safely removed locally.
        templateData.status = 'failed';
        templateData.isActive = false;
      }

      // Preserve failed submissions locally, but mark them explicitly so they do
      // not remain indefinitely as pending records without a Meta template ID.
      const savedTemplate = await Template.findOneAndUpdate(
        { userId: req.user.id, companyId: req.companyId,
          ...(req.templateToRetry ? { _id: req.templateToRetry._id } : { name: normalizedName }) },
        {
          ...templateData,
          whatsappTemplateId: metaTemplateId || null
        },
        { new: true, upsert: true, runValidators: true, setDefaultsOnInsert: true }
      );
      localTemplate = savedTemplate;

      if (metaSubmissionFailed) {
        return res.status(201).json({
          success: true,
          data: savedTemplate,
          message: 'Template saved locally, but Meta submission failed',
          metaSubmission: {
            success: false,
            error: metaResult.error || 'Failed to submit template to Meta',
            details: metaResult.details || null
          }
        });
      }

      res.status(201).json({
        success: true,
        data: savedTemplate,
        message: existingMetaTemplate
          ? 'Template matched an existing Meta template and was saved locally'
          : 'Template created successfully',
        metaSubmission: {
          success: true,
          error: null,
          details: metaResult.details || null
        }
      });
    } catch (error) {
      // The record is created before contacting Meta so a network or unexpected
      // Meta error must also transition it out of pending when no Meta ID exists.
      if (localTemplate?._id && !String(localTemplate.whatsappTemplateId || '').trim()) {
        try {
          await Template.findByIdAndUpdate(localTemplate._id, {
            status: 'failed',
            isActive: false
          });
        } catch (statusUpdateError) {
          console.error('Failed to mark local template submission as failed:', statusUpdateError.message);
        }
      }
      if (error?.name === 'StrictModeError' && /companyId/.test(String(error?.message || ''))) {
        return res.status(500).json({
          success: false,
          error: 'Template schema mismatch detected. Restart backend to load latest schema.',
          details: error.message
        });
      }
      if (error.code === 11000) {
        return res.status(400).json({ success: false, error: 'Template name already exists' });
      }
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async updateTemplate(req, res) {
    try {
      const updateData = { ...req.body };
      const existingTemplate = await Template.findOne({
        _id: req.params.id, userId: req.user.id, companyId: req.companyId
      });
      if (!existingTemplate) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      // Failed/draft local templates have never reached Meta; submit their correction.
      if (!existingTemplate.whatsappTemplateId && ['failed', 'draft'].includes(existingTemplate.status)) {
        if (!req.whatsappCredentials) {
          req.whatsappCredentials = await getWhatsAppCredentialsForUser({
            authHeader: req.headers?.authorization || '', userId: req.user.id
          });
        }
        if (!req.whatsappCredentials) {
          return res.status(400).json({ success: false, error: 'Connect WhatsApp before resubmitting this template.' });
        }
        req.templateToRetry = existingTemplate;
        req.body = {
          name: existingTemplate.name,
          category: existingTemplate.category,
          language: existingTemplate.language,
          type: existingTemplate.type,
          content: existingTemplate.content,
          ...updateData
        };
        return await this.createTemplate(req, res);
      }
      if (updateData.content && typeof updateData.content === 'object') {
        updateData.content = {
          ...updateData.content,
          body: prepareMetaTemplateText(updateData.content.body || '').text,
          footer: prepareMetaTemplateText(updateData.content.footer || '').text,
          buttons: normalizeTemplateButtons(updateData.content.buttons || [])
        };
      }
      const template = await Template.findOneAndUpdate(
        { _id: req.params.id, userId: req.user.id, companyId: req.companyId },
        updateData,
        { new: true, runValidators: true }
      );
      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      res.json({ success: true, data: template });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteTemplate(req, res) {
    try {
      const template = await Template.findOne({
        _id: req.params.id,
        userId: req.user.id,
        companyId: req.companyId
      });

      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }

      const metaTemplateId = String(template.whatsappTemplateId || '').trim();
      if (!metaTemplateId) {
        await Template.deleteOne({ _id: template._id });
        console.info('[TEMPLATE_DELETE] local-only', {
          templateId: String(template._id),
          name: template.name,
          status: template.status
        });
        return res.json({ success: true, message: 'Local template deleted successfully' });
      }

      let credentials;
      try {
        credentials = await getWhatsAppCredentialsForUser({
          authHeader: req.headers.authorization || '',
          userId: req.user?.id || null,
          forceRefresh: true
        });
      } catch (credentialsError) {
        return res.status(Number(credentialsError.statusCode) || 502).json({
          success: false,
          error: 'Failed to fetch WhatsApp credentials before deleting the Meta template'
        });
      }

      if (!credentials) {
        return res.status(400).json({
          success: false,
          error: 'WhatsApp credentials are required to delete this Meta template'
        });
      }

      const workspaceCompanyId = String(req.companyId || req.user?.companyId || '').trim();
      const credentialCompanyId = String(credentials.companyId || '').trim();
      const templateWabaId = String(template.businessAccountId || '').trim();
      const credentialWabaId = String(credentials.businessAccountId || '').trim();
      if (
        workspaceCompanyId &&
        credentialCompanyId &&
        workspaceCompanyId !== credentialCompanyId
      ) {
        return res.status(403).json({
          success: false,
          error: 'WhatsApp credentials do not belong to the active workspace.',
          meta: {
            credentialCompanyId,
            workspaceCompanyId,
            credentialOwnerUserId: credentials.credentialOwnerUserId || null
          }
        });
      }
      if (templateWabaId && templateWabaId !== credentialWabaId) {
        return res.status(409).json({
          success: false,
          error: 'The saved template belongs to a different WhatsApp Business Account than the active credentials.',
          meta: {
            templateWabaId,
            credentialWabaId,
            credentialOwnerUserId: credentials.credentialOwnerUserId || null
          }
        });
      }

      console.info('[TEMPLATE_DELETE] Meta + local', {
        templateId: String(template._id),
        metaTemplateId,
        name: template.name,
        status: template.status
      });
      const metaDeleteResult = await whatsappService.deleteTemplateByName(template.name, credentials, metaTemplateId);
      if (!metaDeleteResult.success) {
        console.error('[TEMPLATE_DELETE] Meta deletion failed', {
          templateId: String(template._id),
          metaTemplateId,
          error: metaDeleteResult.error || 'Unknown Meta deletion error'
        });
        return res.status(502).json({
          success: false,
          error: metaDeleteResult.error || 'Failed to delete template from Meta',
          meta: metaDeleteResult.details || null
        });
      }

      await Template.deleteOne({ _id: template._id });
      return res.json({
        success: true,
        message: metaDeleteResult.alreadyDeleted
          ? 'Template was already absent from Meta; local template deleted successfully'
          : 'Template deleted from Meta and local database'
      });
    } catch (error) {
      console.error('[TEMPLATE_DELETE] Unexpected deletion error:', error);
      if (error?.name === 'CastError') {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteMetaTemplate(req, res) {
    try {
      // Express has already decoded the route parameter.
      const templateName = String(req.params.name || '').trim();
      if (!templateName) {
        return res.status(400).json({ success: false, error: 'Template name is required' });
      }
      const metaTemplateId = String(req.query.hsm_id || '').trim();
      if (!metaTemplateId) {
        return res.status(400).json({
          success: false,
          error: 'hsm_id is required to delete a specific language version safely'
        });
      }
      let freshCredentials;
      try {
        freshCredentials = await getWhatsAppCredentialsForUser({
          authHeader: req.headers?.authorization || '',
          userId: req.user?.id || null,
          forceRefresh: true
        });
      } catch (error) {
        console.error('[TEMPLATE_DELETE_DEBUG] Fresh credential retrieval failed', {
          userId: req.user?.id || null,
          message: error.message
        });
        return res.status(502).json({
          success: false,
          error: 'Unable to retrieve fresh WhatsApp credentials for template deletion'
        });
      }
      if (!freshCredentials?.accessToken || !freshCredentials?.businessAccountId) {
        return res.status(502).json({
          success: false,
          error: 'Fresh WhatsApp credentials are missing a valid access token or WABA ID'
        });
      }
      const template = await Template.findOne({
        userId: req.user.id,
        companyId: req.companyId,
        name: templateName,
        whatsappTemplateId: metaTemplateId
      });
      const workspaceCompanyId = String(req.companyId || req.user?.companyId || '').trim();
      const credentialCompanyId = String(freshCredentials.companyId || '').trim();
      const templateWabaId = String(template?.businessAccountId || '').trim();
      const credentialWabaId = String(freshCredentials.businessAccountId || '').trim();

      if (
        workspaceCompanyId &&
        credentialCompanyId &&
        workspaceCompanyId !== credentialCompanyId
      ) {
        return res.status(403).json({
          success: false,
          error: 'WhatsApp credentials do not belong to the active workspace.',
          meta: {
            credentialCompanyId,
            workspaceCompanyId,
            credentialOwnerUserId: freshCredentials.credentialOwnerUserId || null
          }
        });
      }
      if (templateWabaId && templateWabaId !== credentialWabaId) {
        return res.status(409).json({
          success: false,
          error: 'The saved template belongs to a different WhatsApp Business Account than the active credentials.',
          meta: {
            templateWabaId,
            credentialWabaId,
            credentialOwnerUserId: freshCredentials.credentialOwnerUserId || null
          }
        });
      }

      console.info('[TEMPLATE_DELETE_CREDENTIAL_SCOPE]', {
        userId: req.user?.id || null,
        credentialOwnerUserId: freshCredentials.credentialOwnerUserId || null,
        credentialSource: freshCredentials.credentialSource || 'unspecified',
        workspaceCompanyId: workspaceCompanyId || null,
        credentialCompanyId: credentialCompanyId || null,
        templateWabaId: templateWabaId || null,
        credentialWabaId: credentialWabaId || null
      });

      const metaDeleteResult = await whatsappService.deleteTemplateByName(
        templateName,
        freshCredentials,
        metaTemplateId,
        {
          userId: req.user?.id || null,
          credentialSource: freshCredentials.credentialSource || 'unspecified',
          credentialsFromCache: freshCredentials.credentialSource === 'admin_user_credentials_cache',
          credentialsFromFreshApi: freshCredentials.credentialSource === 'admin_user_credentials_api'
        }
      );

      if (!metaDeleteResult.success) {
        return res.status(502).json({
          success: false,
          error: metaDeleteResult.error || 'Failed to delete template from Meta',
          meta: metaDeleteResult.details || null
        });
      }

      const localDeleteResult = await Template.deleteOne({
        userId: req.user.id,
        companyId: req.companyId,
        name: templateName,
        whatsappTemplateId: metaTemplateId
      });

      res.json({
        success: true,
        message: metaDeleteResult.alreadyDeleted
          ? 'Template was already absent from Meta; local templates deleted successfully'
          : 'Template deleted from Meta and local database',
        deletedLocalCount: localDeleteResult.deletedCount || 0
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async syncWhatsAppTemplates(req, res) {
    try {
      console.log(' Starting template sync from Meta WhatsApp Business API...');

      const userId = req?.user?.id || req?.user?._id || req?.syncUserId || null;
      if (!userId) {
        return res.status(400).json({
          success: false,
          error: 'Template sync requires a user context (req.user.id or syncUserId)'
        });
      }

      const credentials = req?.whatsappCredentials || null;
      const companyId = req.companyId || req.user?.companyId || credentials?.companyId || null;
      
      // 1. Authenticate with Meta API and fetch templates
      const result = await whatsappService.getTemplateList(credentials);
      
      if (!result.success) {
        console.error('[WHATSAPP_TEMPLATE_SYNC]', JSON.stringify({
          outcome: 'failure', stage: 'meta_fetch',
          wabaId: String(result.wabaId || req.whatsappCredentials?.businessAccountId || '').trim(),
          error: String(result.error?.message || result.error || 'Meta template fetch failed')
        }));
        return res.status(500).json({ 
          success: false, 
          error: result.error 
        });
      }

      const whatsappTemplates = result.data.data || [];
      console.log(` Retrieved ${whatsappTemplates.length} templates from Meta`);

      // 2. Process and store templates in local database
      const syncedTemplates = [];
      const failedTemplates = [];
      const syncVersion = new Date().toISOString();
      const businessAccountId = String(result.wabaId || credentials?.businessAccountId || '').trim();
      const phoneNumberId = String(result.phoneNumberId || credentials?.phoneNumberId || '').trim();

      for (const wt of whatsappTemplates) {
        const language = String(wt.language || '').trim();
        let operation = 'create';
        let databaseWriteCompleted = false;
        try {
          // Meta template ids are scoped to a WABA. Keep tenant and WABA in the
          // lookup together with language for language-specific definitions.
          const existingTemplate = await Template.findOne({
            userId,
            companyId,
            businessAccountId,
            whatsappTemplateId: wt.id,
            language
          });
          if (existingTemplate) operation = 'update';
          const metaComponents = Array.isArray(wt.components) ? wt.components : [];
          const header = metaComponents.find(component => component.type === 'HEADER');
          const body = metaComponents.find(component => component.type === 'BODY');
          const footer = metaComponents.find(component => component.type === 'FOOTER');
          const buttons = metaComponents.find(component => component.type === 'BUTTONS');
          const now = new Date();
          const templateData = {
            userId,
            companyId,
            name: wt.name,
            type: 'official',
            category: wt.category || 'utility',
            language: wt.language,
            status: String(wt.status || '').toLowerCase(),
            isActive: String(wt.status || '').toLowerCase() === 'approved',
            content: {
              header: header ? {
                type: String(header.format || 'text').toLowerCase(),
                text: header.text || ''
              } : { type: 'text', text: '' },
              body: body?.text || '',
              footer: footer?.text || '',
              buttons: normalizeTemplateButtons(buttons?.buttons || [])
            },
            // Extract only numbered placeholders present in Meta's BODY text.
            variables: this.extractVariables(body?.text || '').map(variable => {
              const variableNumber = Number(String(variable.name || '').match(/\d+/)?.[0] || 0);
              const bodyExampleRows = Array.isArray(body?.example?.body_text) ? body.example.body_text : [];
              const bodyExamples = Array.isArray(bodyExampleRows[0]) ? bodyExampleRows[0] : [];
              const example = variableNumber > 0 ? String(bodyExamples[variableNumber - 1] || '').trim() : '';
              return example ? { ...variable, example } : variable;
            }),
            metaComponents,
            whatsappTemplateId: wt.id,
            businessAccountId,
            phoneNumberId: result.phoneNumberId || credentials?.phoneNumberId || '',
            updatedAt: now,
            syncedAt: now,
            createdById: userId
          };
          const syncedTemplate = existingTemplate
            ? await Template.findByIdAndUpdate(existingTemplate._id, { $set: templateData }, { new: true })
            : await Template.create({ ...templateData, createdAt: now });
          databaseWriteCompleted = Boolean(syncedTemplate);
          const persistedBodyComponent = (syncedTemplate?.metaComponents || []).find(component => component?.type === 'BODY');
          const expectedBody = String(body?.text || '');
          if (
            !syncedTemplate ||
            String(syncedTemplate.content?.body || '') !== expectedBody ||
            String(persistedBodyComponent?.text || '') !== expectedBody ||
            JSON.stringify(syncedTemplate.metaComponents || []) !== JSON.stringify(metaComponents) ||
            JSON.stringify((syncedTemplate.variables || []).map(variable => variable.name)) !==
              JSON.stringify(templateData.variables.map(variable => variable.name)) ||
            String(syncedTemplate.language || '') !== language ||
            String(syncedTemplate.businessAccountId || '') !== businessAccountId ||
            String(syncedTemplate.whatsappTemplateId || '') !== String(wt.id || '')
          ) {
            throw new Error('Persisted template definition does not match the Meta response');
          }
          syncedTemplates.push(syncedTemplate);
          console.info('[WHATSAPP_TEMPLATE_SYNC]', JSON.stringify({
            outcome: 'success', operation,
            templateName: wt.name || '', metaTemplateId: wt.id || '',
            tenantId: String(userId), clientId: String(companyId || ''),
            wabaId: businessAccountId, language,
            phoneNumberId,
            credentialOwnerUserId: String(credentials?.credentialOwnerUserId || userId),
            credentialSource: credentials?.credentialSource || 'unknown',
            extractedVariableCount: templateData.variables.length,
            databaseUpdated: true, syncVersion
          }));
        } catch (error) {
          const failure = {
            templateName: String(wt.name || ''),
            metaTemplateId: String(wt.id || ''),
            tenantId: String(userId),
            clientId: String(companyId || ''),
            wabaId: businessAccountId,
            phoneNumberId,
            credentialOwnerUserId: String(credentials?.credentialOwnerUserId || userId),
            credentialSource: credentials?.credentialSource || 'unknown',
            language,
            databaseWriteCompleted,
            error: String(error?.message || 'Unknown template sync error')
          };
          failedTemplates.push(failure);
          console.error('[WHATSAPP_TEMPLATE_SYNC]', JSON.stringify({
            outcome: 'failure', operation, ...failure, databaseUpdated: databaseWriteCompleted, syncVersion
          }));
        }
      }

      console.info('[WHATSAPP_TEMPLATE_SYNC_SUMMARY]', JSON.stringify({
        wabaId: businessAccountId,
        tenantId: String(userId),
        clientId: String(companyId || ''),
        phoneNumberId,
        credentialOwnerUserId: String(credentials?.credentialOwnerUserId || userId),
        credentialSource: credentials?.credentialSource || 'unknown',
        fetchedCount: whatsappTemplates.length,
        syncedCount: syncedTemplates.length,
        failedCount: failedTemplates.length,
        syncVersion
      }));

      // 3. Return success response with synced templates
      res.json({ 
        success: failedTemplates.length === 0,
        message: failedTemplates.length === 0
          ? `Successfully synced ${syncedTemplates.length} templates from Meta`
          : `Synced ${syncedTemplates.length} of ${whatsappTemplates.length} templates; ${failedTemplates.length} failed`,
        templates: syncedTemplates,
        count: syncedTemplates.length,
        failedTemplates,
        syncVersion,
        wabaId: businessAccountId,
        phoneNumberId,
        tenantId: String(userId),
        clientId: String(companyId || ''),
        credentialOwnerUserId: String(credentials?.credentialOwnerUserId || userId),
        credentialSource: credentials?.credentialSource || 'unknown'
      });

    } catch (error) {
      console.error(' Template sync failed:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message,
        message: 'Failed to sync templates from Meta WhatsApp Business API'
      });
    }
  }

  async incrementUsage(req, res) {
    try {
      const template = await Template.findOne({ _id: req.params.id, userId: req.user.id });
      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      template.usageCount = (template.usageCount || 0) + 1;
      await template.save();
      res.json({ success: true, data: template });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  // Get templates directly from Meta WhatsApp Business API
  async getMetaTemplates(req, res) {
    try {
      console.log('🔄 Fetching templates directly from Meta WhatsApp Business API...');
      
      const result = await whatsappService.getTemplateList(req.whatsappCredentials);
      
      if (!result.success) {
        console.error('[WHATSAPP_TEMPLATE_CATALOG]', JSON.stringify({
          outcome: 'failure', source: 'meta',
          wabaId: String(result.wabaId || req.whatsappCredentials?.businessAccountId || '').trim(),
          error: String(result.error?.message || result.error || 'Meta template fetch failed')
        }));
        return res.status(500).json({ 
          success: false, 
          error: result.error 
        });
      }

      const metaTemplates = result.data.data || [];
      const fetchedAt = new Date().toISOString();
      const businessAccountId = String(result.wabaId || req.whatsappCredentials?.businessAccountId || '').trim();
      const phoneNumberId = String(result.phoneNumberId || req.whatsappCredentials?.phoneNumberId || '').trim();
      const companyId = req.companyId || req.user?.companyId || req.whatsappCredentials?.companyId || null;
      console.info('[WHATSAPP_TEMPLATE_CATALOG]', JSON.stringify({
        outcome: 'success', source: 'meta', wabaId: businessAccountId, phoneNumberId,
        tenantId: String(req.user?.id || ''), clientId: String(req.companyId || ''),
        credentialOwnerUserId: String(req.whatsappCredentials?.credentialOwnerUserId || req.user?.id || ''),
        credentialSource: req.whatsappCredentials?.credentialSource || 'unknown',
        fetchedCount: metaTemplates.length, fetchedAt,
        templates: metaTemplates.map(template => ({
          name: String(template?.name || ''),
          metaTemplateId: String(template?.id || ''),
          language: String(template?.language || ''),
          status: String(template?.status || ''),
          extractedVariableCount: this.extractVariables(
            template?.components?.find(component => component?.type === 'BODY')?.text || ''
          ).length
        }))
      }));
      console.log(`📋 Retrieved ${metaTemplates.length} templates from Meta`);

      res.json({
        success: true,
        data: metaTemplates,
        count: metaTemplates.length,
        source: 'meta',
        wabaId: businessAccountId,
        phoneNumberId,
        tenantId: String(req.user?.id || ''),
        clientId: String(companyId || ''),
        credentialOwnerUserId: String(req.whatsappCredentials?.credentialOwnerUserId || req.user?.id || ''),
        credentialSource: req.whatsappCredentials?.credentialSource || 'unknown',
        fetchedAt,
        catalogVersion: fetchedAt
      });

    } catch (error) {
      console.error('❌ Failed to fetch Meta templates:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message,
        message: 'Failed to fetch templates from Meta WhatsApp Business API'
      });
    }
  }

  // Sync templates from Meta WhatsApp Business API
  async syncMetaTemplates(req, res) {
    try {
      console.log('🔄 Starting template sync from Meta WhatsApp Business API...');
      
      // Call the existing sync method
      await this.syncWhatsAppTemplates(req, res);
      
    } catch (error) {
      console.error('❌ Meta template sync failed:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message,
        message: 'Failed to sync templates from Meta WhatsApp Business API'
      });
    }
  }
}

module.exports = new TemplateController();





