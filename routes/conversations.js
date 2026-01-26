const express = require('express');
const router = express.Router();
const conversationController = require('../controllers/conversationController');

// Get all conversations with optional filters
router.get('/', (req, res) => conversationController.getConversations(req, res));

// Get all contacts with optional filters
router.get('/contacts', (req, res) => conversationController.getContacts(req, res));

// Get unique contacts from conversations (for broadcast)
router.get('/contacts/unique', (req, res) => conversationController.getConversationContacts(req, res));

// Create a new contact
router.post('/contacts', (req, res) => conversationController.createContact(req, res));

// Update an existing contact
router.put('/contacts/:id', (req, res) => conversationController.updateContact(req, res));

// Delete a contact
router.delete('/contacts/:id', (req, res) => conversationController.deleteContact(req, res));

// Get a single contact by ID
router.get('/contacts/:id', (req, res) => conversationController.getContactById(req, res));

// Delete all conversations
router.delete('/delete-all', (req, res) => conversationController.deleteAllConversations(req, res));

// Delete selected conversations
router.delete('/delete-selected', (req, res) => conversationController.deleteSelectedConversations(req, res));

// Delete a single conversation (must come after specific routes)
router.delete('/:id', (req, res) => conversationController.deleteConversation(req, res));

module.exports = router;
