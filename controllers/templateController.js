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
      const filters = {};
      
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
      const template = await Template.findById(req.params.id);
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
      const templateData = {
        ...req.body,
        type: 'custom',
        createdBy: req.body.createdBy || 'system'
      };
      const template = await Template.create(templateData);
      res.status(201).json({ success: true, data: template });
    } catch (error) {
      if (error.code === 11000) {
        return res.status(400).json({ success: false, error: 'Template name already exists' });
      }
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async updateTemplate(req, res) {
    try {
      const template = await Template.findByIdAndUpdate(
        req.params.id,
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
      const template = await Template.findByIdAndDelete(req.params.id);
      if (!template) {
        return res.status(404).json({ success: false, error: 'Template not found' });
      }
      res.json({ success: true, message: 'Template deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async syncWhatsAppTemplates(req, res) {
    try {
      console.log(' Starting template sync from Meta WhatsApp Business API...');
      
      // 1. Authenticate with Meta API and fetch templates
      const result = await whatsappService.getTemplateList();
      
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
              name: wt.name,
              type: 'official',
              category: wt.category || 'utility',
              language: wt.language,
              status: wt.status, // Keep original case from Meta API
              isActive: wt.status === 'APPROVED',
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
              syncedAt: new Date()
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
                  status: wt.status, // Keep original case from Meta API
                  isActive: wt.status === 'APPROVED',
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
      const template = await Template.findById(req.params.id);
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
      
      const result = await whatsappService.getTemplateList();
      
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
