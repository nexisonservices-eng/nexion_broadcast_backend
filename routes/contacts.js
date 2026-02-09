const express = require('express');
const Contact = require('../models/Contact');

const router = express.Router();

// Get all contacts
router.get('/', async (req, res) => {
  try {
    const { search, tags } = req.query;
    const filters = {};
    
    if (search) {
      filters.$or = [
        { name: { $regex: search, $options: 'i' } },
        { phone: { $regex: search, $options: 'i' } },
        { email: { $regex: search, $options: 'i' } }
      ];
    }
    
    if (tags) {
      filters.tags = { $in: tags.split(',') };
    }

    const contacts = await Contact.find(filters).sort({ lastContact: -1 });
    res.json(contacts);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Create new contact
router.post('/', async (req, res) => {
  try {
    const contact = await Contact.create(req.body);
    res.status(201).json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Import multiple contacts
router.post('/import', async (req, res) => {
  try {
    const { contacts } = req.body;
    
    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ error: 'Invalid contacts data' });
    }

    console.log(`📥 Importing ${contacts.length} contacts`);
    
    const results = {
      success: 0,
      failed: 0,
      errors: []
    };

    // Process contacts in batches to avoid overwhelming the database
    const batchSize = 50;
    for (let i = 0; i < contacts.length; i += batchSize) {
      const batch = contacts.slice(i, i + batchSize);
      
      for (const contactData of batch) {
        try {
          // Validate required fields
          if (!contactData.phone) {
            results.failed++;
            results.errors.push({
              line: contactData.lineNumber || 'Unknown',
              error: 'Phone number is required',
              data: contactData
            });
            continue;
          }

          // Check for duplicate phone numbers
          const existingContact = await Contact.findOne({ phone: contactData.phone });
          if (existingContact) {
            results.failed++;
            results.errors.push({
              line: contactData.lineNumber || 'Unknown',
              error: 'Phone number already exists',
              data: contactData
            });
            continue;
          }

          // Create contact
          const contact = new Contact({
            name: contactData.name || '',
            phone: contactData.phone,
            email: contactData.email || '',
            tags: Array.isArray(contactData.tags) ? contactData.tags : [],
            isBlocked: contactData.status === 'Opted-out',
            lastContact: new Date(),
            createdAt: new Date(),
            updatedAt: new Date()
          });

          await contact.save();
          results.success++;
          
        } catch (error) {
          results.failed++;
          results.errors.push({
            line: contactData.lineNumber || 'Unknown',
            error: error.message,
            data: contactData
          });
        }
      }
    }

    console.log(`✅ Import completed: ${results.success} successful, ${results.failed} failed`);
    
    if (results.failed > 0) {
      console.log('❌ Import errors:', results.errors);
    }

    res.json({
      success: true,
      message: `Import completed: ${results.success} contacts imported successfully${results.failed > 0 ? `, ${results.failed} failed` : ''}`,
      results: {
        imported: results.success,
        failed: results.failed,
        errors: results.errors
      }
    });

  } catch (error) {
    console.error('❌ Import error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Import failed: ' + error.message 
    });
  }
});

// Update contact
router.put('/:id', async (req, res) => {
  try {
    const contact = await Contact.findByIdAndUpdate(
      req.params.id,
      req.body,
      { new: true, runValidators: true }
    );
    
    if (!contact) {
      return res.status(404).json({ error: 'Contact not found' });
    }
    
    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete contact
router.delete('/:id', async (req, res) => {
  try {
    const contact = await Contact.findByIdAndDelete(req.params.id);
    
    if (!contact) {
      return res.status(404).json({ error: 'Contact not found' });
    }
    
    res.json({ message: 'Contact deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
