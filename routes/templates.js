const express = require('express');
const router = express.Router();
const templateController = require('../controllers/templateController');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const multer = require('multer');
const templateImageUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024, files: 1 },
  fileFilter: (req, file, callback) => {
    if (!['image/jpeg', 'image/png'].includes(file.mimetype)) {
      return callback(new Error('Select a JPEG or PNG template image.'));
    }
    callback(null, true);
  }
}).single('file');

router.use(auth);

router.post('/media', requireWhatsAppCredentials, (req, res) => {
  templateImageUpload(req, res, (error) => {
    if (error) {
      return res.status(400).json({ success: false, error: error.code === 'LIMIT_FILE_SIZE'
        ? 'Template images must be 5 MB or smaller.' : error.message });
    }
    return templateController.uploadTemplateImage(req, res);
  });
});

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
