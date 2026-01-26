const mongoose = require('mongoose');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
require('dotenv').config();

async function testTeamInboxContacts() {
  try {
    const MONGODB_URI = process.env.MONGODB_URI;
    await mongoose.connect(MONGODB_URI);
    console.log('Connected to MongoDB');

    // Test conversation contacts endpoint
    const conversations = await Conversation.find({ status: { $in: ['active', 'pending'] } })
      .select('contactPhone contactName lastMessageTime lastMessage')
      .sort({ lastMessageTime: -1 })
      .limit(5);
    
    console.log(`Found ${conversations.length} conversations:`);
    conversations.forEach(conv => {
      console.log(`- ${conv.contactName || conv.contactPhone}: ${conv.lastMessage?.substring(0, 50)}...`);
    });

    // Test unique contacts extraction
    const uniqueContacts = [];
    const seenPhones = new Set();
    
    conversations.forEach(conv => {
      if (!seenPhones.has(conv.contactPhone)) {
        uniqueContacts.push({
          phone: conv.contactPhone,
          name: conv.contactName || conv.contactPhone,
          lastMessageTime: conv.lastMessageTime,
          lastMessage: conv.lastMessage,
          status: conv.status
        });
        seenPhones.add(conv.contactPhone);
      }
    });
    
    console.log(`\nUnique contacts for broadcast: ${uniqueContacts.length}`);
    uniqueContacts.forEach(contact => {
      console.log(`- ${contact.name} (${contact.phone})`);
    });

    process.exit(0);
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

testTeamInboxContacts();
