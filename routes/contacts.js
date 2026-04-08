const express = require('express');
const Contact = require('../models/Contact');
const auth = require('../middleware/auth');

const router = express.Router();
router.use(auth);
const CONTACT_LIST_FIELDS = [
  '_id',
  'name',
  'phone',
  'email',
  'tags',
  'stage',
  'status',
  'source',
  'ownerId',
  'sourceType',
  'lastContact',
  'lastContactAt',
  'nextFollowUpAt',
  'isBlocked',
  'leadScore',
  'createdAt',
  'updatedAt'
].join(' ');

// Get all contacts
router.get('/', async (req, res) => {
  try {
    const { search, tags } = req.query;
    const conditions = [{ userId: req.user.id }];
    if (req.companyId) {
      conditions.push({
        $or: [
        { companyId: req.companyId },
        { companyId: null },
        { companyId: { $exists: false } }
      ]
      });
    }
    
    if (search) {
      conditions.push({
        $or: [
        { name: { $regex: search, $options: 'i' } },
        { phone: { $regex: search, $options: 'i' } },
        { email: { $regex: search, $options: 'i' } }
      ]
      });
    }
    
    if (tags) {
      conditions.push({ tags: { $in: tags.split(',') } });
    }

    const filters = conditions.length === 1 ? conditions[0] : { $and: conditions };
    const contacts = await Contact.find(filters)
      .select(CONTACT_LIST_FIELDS)
      .sort({ lastContact: -1, createdAt: -1 })
      .lean();
    res.json(contacts);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Create new contact
router.post('/', async (req, res) => {
  try {
    const contact = await Contact.create({
      ...req.body,
      userId: req.user.id,
      companyId: req.companyId || null,
      sourceType: 'manual'
    });
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
          const existingContact = await Contact.findOne({
            phone: contactData.phone,
            userId: req.user.id,
            ...(req.companyId ? { companyId: req.companyId } : {})
          });
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
            userId: req.user.id,
            companyId: req.companyId || null,
            name: contactData.name || '',
            phone: contactData.phone,
            email: contactData.email || '',
            tags: Array.isArray(contactData.tags) ? contactData.tags : [],
            isBlocked: contactData.status === 'Opted-out',
            sourceType: 'imported',
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
    const existingContact = await Contact.findOne({
      _id: req.params.id,
      userId: req.user.id,
      ...(req.companyId ? { $or: [{ companyId: req.companyId }, { companyId: null }, { companyId: { $exists: false } }] } : {})
    });

    if (!existingContact) {
      return res.status(404).json({ error: 'Contact not found' });
    }

    const updatePayload = { ...req.body };
    if (updatePayload.customFields && typeof updatePayload.customFields === 'object') {
      updatePayload.customFields = {
        ...(existingContact.customFields && typeof existingContact.customFields === 'object' ? existingContact.customFields : {}),
        ...updatePayload.customFields
      };
    }

    const contact = await Contact.findOneAndUpdate(
      { _id: req.params.id, userId: req.user.id },
      updatePayload,
      { new: true, runValidators: true }
    );

    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete contact
router.delete('/:id', async (req, res) => {
  try {
    const contact = await Contact.findOneAndDelete({
      _id: req.params.id,
      userId: req.user.id,
      ...(req.companyId ? { $or: [{ companyId: req.companyId }, { companyId: null }, { companyId: { $exists: false } }] } : {})
    });
    
    if (!contact) {
      return res.status(404).json({ error: 'Contact not found' });
    }
    
    res.json({ message: 'Contact deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
