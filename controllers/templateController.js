const Template = require('../models/Template');
const whatsappService = require('../services/whatsappService');

class TemplateController {
    // Helper function to extract variables from template text
    extractVariables(text) {
        if (!text) return [];
        
        const variablePattern = /\{\{(\d+)\}\}/g;
        const variables = [];
        let match;
        
        while ((match = variablePattern.exec(text)) !== null) {
            variables.push({
                name: `var${match[1]}`,
                example: `Example ${match[1]}`,
                required: false
            });
        }
        
        return variables;
    }
  async getAllTemplates(req, res) {
    try {
      const { status, isActive, category } = req.query;
      const filters = { userId: req.user.id };
      
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
      const template = await Template.findOne({ _id: req.params.id, userId: req.user.id });
      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      res.json({ success: true, data: template });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async createTemplate(req, res) {
    try {
      const { name, category, language, content, type = 'custom', components } = req.body;
      const normalizedName = String(name || '').trim();

      if (!normalizedName || (!content && !components)) {
        return res.status(400).json({
          success: false,
          error: 'Template name and content/components are required'
        });
      }

      let templateContent = content;
      let bodyText = '';

      if (components && Array.isArray(components)) {
        const bodyComponent = components.find(comp => comp.type === 'BODY');
        bodyText = bodyComponent ? bodyComponent.text : '';

        const headerComponent = components.find(comp => comp.type === 'HEADER');
        const footerComponent = components.find(comp => comp.type === 'FOOTER');
        const buttonsComponent = components.find(comp => comp.type === 'BUTTONS');

        templateContent = {
          header: {
            type: headerComponent ? (headerComponent.format || 'text').toLowerCase() : 'text',
            text: headerComponent?.text || '',
            mediaUrl: headerComponent?.format === 'IMAGE' ? (headerComponent.example?.header_handle?.[0] || '') : ''
          },
          body: bodyText,
          footer: footerComponent?.text || '',
          buttons: buttonsComponent?.buttons || []
        };
      }

      if (templateContent?.header && templateContent.header.type === 'image' && !templateContent.header.mediaUrl) {
        return res.status(400).json({
          success: false,
          error: 'Header image URL is required when header type is image'
        });
      }

      const bodyForVariables = templateContent?.body || bodyText;
      const variables = this.extractVariables(bodyForVariables);

      const templateData = {
        userId: req.user.id,
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

      // Strict rule: template must be created in Meta first. Only then save in DB.
      const componentsForMeta = Array.isArray(components) && components.length > 0
        ? components
        : [
            ...(templateContent?.header?.text
              ? [{
                  type: 'HEADER',
                  format: 'TEXT',
                  text: templateContent.header.text
                }]
              : []),
            {
              type: 'BODY',
              text: templateContent?.body || ''
            },
            ...(templateContent?.footer
              ? [{
                  type: 'FOOTER',
                  text: templateContent.footer
                }]
              : [])
          ];

      const metaResult = await whatsappService.createTemplate({
        name: templateData.name,
        category: templateData.category.toUpperCase(),
        language: templateData.language,
        components: componentsForMeta
      }, req.whatsappCredentials);

      if (!metaResult.success) {
        return res.status(400).json({
          success: false,
          error: metaResult.error || 'Failed to submit template to Meta'
        });
      }

      const metaTemplateId = metaResult.data?.id || null;

      // Save locally only after Meta creation is successful.
      const template = await Template.findOneAndUpdate(
        { userId: req.user.id, name: normalizedName },
        {
          ...templateData,
          ...(metaTemplateId ? { whatsappTemplateId: metaTemplateId } : {})
        },
        { new: true, upsert: true, runValidators: true, setDefaultsOnInsert: true }
      );

      res.status(201).json({
        success: true,
        data: template,
        message: 'Template created successfully and submitted for approval'
      });
    } catch (error) {
      if (error.code === 11000) {
        return res.status(400).json({ success: false, error: 'Template name already exists' });
      }
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async updateTemplate(req, res) {
    try {
      const template = await Template.findOneAndUpdate(
        { _id: req.params.id, userId: req.user.id },
        req.body,
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
      const template = await Template.findOneAndDelete({ _id: req.params.id, userId: req.user.id });
      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      res.json({ success: true, message: 'Template deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteMetaTemplate(req, res) {
    try {
      const templateName = decodeURIComponent(String(req.params.name || '')).trim();
      if (!templateName) {
        return res.status(400).json({ success: false, error: 'Template name is required' });
      }

      const metaDeleteResult = await whatsappService.deleteTemplateByName(
        templateName,
        req.whatsappCredentials
      );

      if (!metaDeleteResult.success) {
        return res.status(400).json({
          success: false,
          error: metaDeleteResult.error || 'Failed to delete template from Meta'
        });
      }

      const localDeleteResult = await Template.deleteMany({
        userId: req.user.id,
        name: templateName
      });

      res.json({
        success: true,
        message: 'Template deleted from Meta and local database',
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
      
      // 1. Authenticate with Meta API and fetch templates
      const result = await whatsappService.getTemplateList(credentials);
      
      if (!result.success) {
        return res.status(500).json({ 
          success: false, 
          error: result.error 
        });
      }

      const whatsappTemplates = result.data.data || [];
      console.log(` Retrieved ${whatsappTemplates.length} templates from Meta`);

      // 2. Process and store templates in local database
      const syncedTemplates = [];

      for (const wt of whatsappTemplates) {
        try {
          // Check if template already exists
          const existingTemplate = await Template.findOne({
            userId,
            whatsappTemplateId: wt.id
          });

          // Extract template components (move outside the if block)
          const components = [];
          
          if (wt.components) {
            for (const component of wt.components) {
              switch (component.type) {
                case 'HEADER':
                  components.push({
                    type: 'header',
                    format: component.format,
                    text: component.text
                  });
                  break;
                case 'BODY':
                  components.push({
                    type: 'body',
                    text: component.text
                  });
                  break;
                case 'FOOTER':
                  components.push({
                    type: 'footer',
                    text: component.text
                  });
                  break;
                case 'BUTTONS':
                  // Handle buttons separately, not as content
                  break;
              }
            }
          }

          if (!existingTemplate) {

            // Create template object for our database
            const headerComponent = components.find(c => c.type === 'header');
            const templateData = {
              userId,
              name: wt.name,
              type: 'official',
              category: wt.category || 'utility',
              language: wt.language,
              status: wt.status.toLowerCase(), // Convert to lowercase for database
              isActive: wt.status.toLowerCase() === 'approved',
              content: {
                header: headerComponent ? {
                  type: (headerComponent.format || 'text').toLowerCase(),
                  text: headerComponent.text || ''
                } : {
                  type: 'text',
                  text: ''
                },
                body: components.find(c => c.type === 'body')?.text || '',
                footer: components.find(c => c.type === 'footer')?.text || ''
              },
              variables: this.extractVariables(components.find(c => c.type === 'body')?.text || ''),
              whatsappTemplateId: wt.id,
              businessAccountId: whatsappService.wabaId,
              phoneNumberId: whatsappService.phoneNumberId,
              createdAt: new Date(),
              updatedAt: new Date(),
              syncedAt: new Date(),
              createdById: userId
            };

            const newTemplate = await Template.create(templateData);
            syncedTemplates.push(newTemplate);
            
            console.log(` Synced new template: ${wt.name} (${wt.status})`);
          } else {
            // Update existing template if needed
            const needsUpdate = existingTemplate.status !== wt.status || 
                              existingTemplate.name !== wt.name ||
                              existingTemplate.language !== wt.language;

            if (needsUpdate) {
              const headerComponent = components.find(c => c.type === 'header');
              const updatedTemplate = await Template.findByIdAndUpdate(
                existingTemplate._id,
                {
                  name: wt.name,
                  category: wt.category || 'utility',
                  language: wt.language,
                  status: wt.status.toLowerCase(), // Convert to lowercase for database
                  isActive: wt.status.toLowerCase() === 'approved',
                  content: {
                    header: headerComponent ? {
                      type: (headerComponent.format || 'text').toLowerCase(),
                      text: headerComponent.text || ''
                    } : {
                      type: 'text',
                      text: ''
                    },
                    body: components.find(c => c.type === 'body')?.text || '',
                    footer: components.find(c => c.type === 'footer')?.text || ''
                  },
                  variables: this.extractVariables(components.find(c => c.type === 'body')?.text || ''),
                  updatedAt: new Date(),
                  syncedAt: new Date()
                },
                { new: true }
              );
              syncedTemplates.push(updatedTemplate);
              console.log(` Updated template: ${wt.name} (${wt.status})`);
            } else {
              syncedTemplates.push(existingTemplate);
              console.log(` Template already exists: ${wt.name}`);
            }
          }
        } catch (error) {
          console.error(`❌ Error processing template ${wt.name}:`, error);
          console.error('❌ Template details:', {
            id: wt.id,
            name: wt.name,
            category: wt.category,
            language: wt.language,
            status: wt.status,
            components: wt.components
          });
        }
      }

      console.log(` Successfully processed ${syncedTemplates.length} templates`);

      // 3. Return success response with synced templates
      res.json({ 
        success: true, 
        message: `Successfully synced ${syncedTemplates.length} templates from Meta`,
        templates: syncedTemplates,
        count: syncedTemplates.length
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
        return res.status(500).json({ 
          success: false, 
          error: result.error 
        });
      }

      const metaTemplates = result.data.data || [];
      console.log(`📋 Retrieved ${metaTemplates.length} templates from Meta`);

      res.json({
        success: true,
        data: metaTemplates,
        count: metaTemplates.length
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





