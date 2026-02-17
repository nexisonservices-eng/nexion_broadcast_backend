const express = require('express');
const router = express.Router();
const templateController = require('../controllers/templateController');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');

router.use(auth);

router.get('/', (req, res) => templateController.getAllTemplates(req, res));
router.get('/sync', requireWhatsAppCredentials, (req, res) => templateController.syncWhatsAppTemplates(req, res));

// Meta-specific routes (must come before /:id)
router.get('/meta', requireWhatsAppCredentials, (req, res) => templateController.getMetaTemplates(req, res));
router.post('/meta/sync', requireWhatsAppCredentials, (req, res) => templateController.syncMetaTemplates(req, res));
router.delete('/meta/:name', requireWhatsAppCredentials, (req, res) => templateController.deleteMetaTemplate(req, res));

// Other routes
router.get('/:id', (req, res) => templateController.getTemplateById(req, res));
router.post('/', requireWhatsAppCredentials, (req, res) => templateController.createTemplate(req, res));
router.put('/:id', (req, res) => templateController.updateTemplate(req, res));
router.delete('/:id', (req, res) => templateController.deleteTemplate(req, res));
router.post('/:id/usage', (req, res) => templateController.incrementUsage(req, res));

module.exports = router;
