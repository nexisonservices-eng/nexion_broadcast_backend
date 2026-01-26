const mongoose = require('mongoose');
const Template = require('../models/Template');
require('dotenv').config();

async function deleteAllTemplates() {
  try {
    const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/whatsapp_platform';
    
    // Connect to MongoDB
    await mongoose.connect(MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log('Connected to MongoDB');

    // Delete all templates
    const result = await Template.deleteMany({});
    console.log(`Successfully deleted ${result.deletedCount} templates`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error deleting templates:', error);
    process.exit(1);
  }
}

deleteAllTemplates();
