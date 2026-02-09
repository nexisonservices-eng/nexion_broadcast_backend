const express = require('express');
const router = express.Router();
const templateController = require('../controllers/templateController');

router.get('/', (req, res) => templateController.getAllTemplates(req, res));
router.get('/sync', (req, res) => templateController.syncWhatsAppTemplates(req, res));

// Meta-specific routes (must come before /:id)
router.get('/meta', (req, res) => templateController.getMetaTemplates(req, res));
router.post('/meta/sync', (req, res) => templateController.syncMetaTemplates(req, res));

// Other routes
router.get('/:id', (req, res) => templateController.getTemplateById(req, res));
router.post('/', (req, res) => templateController.createTemplate(req, res));
router.put('/:id', (req, res) => templateController.updateTemplate(req, res));
router.delete('/:id', (req, res) => templateController.deleteTemplate(req, res));
router.post('/:id/usage', (req, res) => templateController.incrementUsage(req, res));

module.exports = router;
