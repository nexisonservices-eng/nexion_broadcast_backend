const express = require('express');
const router = express.Router();
const templateController = require('../controllers/templateController');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const { startTemplateCatalogDiagnostics } = require('../utils/templateCatalogDiagnostics');
const multer = require('multer');
const { bytesPerMB, maxTemplateMediaMB, getTemplateHeaderMediaType } = require('../utils/templateHeaderMedia');
const templateMediaUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: maxTemplateMediaMB * bytesPerMB + 1, files: 1, fields: 1, fieldSize: 32 },
  fileFilter: (req, file, callback) => {
    if (!getTemplateHeaderMediaType(file.mimetype)) {
      return callback(new Error('Unsupported template media. Select a JPEG, PNG, MP4, or PDF file.'));
    }
    callback(null, true);
  }
}).single('file');

router.use(auth);

router.post('/media', requireWhatsAppCredentials.forTemplateWrite, (req, res) => {
  templateMediaUpload(req, res, (error) => {
    if (error) {
      return res.status(400).json({ success: false, error: error.code === 'LIMIT_FILE_SIZE'
        ? `Template media must be ${maxTemplateMediaMB} MB or smaller.` : error.message });
    }
    return templateController.uploadTemplateMedia(req, res);
  });
});

router.get('/', (req, res) => templateController.getAllTemplates(req, res));
router.get('/sync', requireWhatsAppCredentials.forceRefresh, (req, res) => templateController.syncWhatsAppTemplates(req, res));

// Meta-specific routes (must come before /:id)
router.get('/meta', startTemplateCatalogDiagnostics, requireWhatsAppCredentials.forceRefresh,
  (req, res) => templateController.getMetaTemplates(req, res));
router.post('/meta/sync', requireWhatsAppCredentials.forceRefresh, (req, res) => templateController.syncMetaTemplates(req, res));
router.delete('/meta/:name', (req, res) => templateController.deleteMetaTemplate(req, res));

// Other routes
router.get('/:id', (req, res) => templateController.getTemplateById(req, res));
router.post('/', requireWhatsAppCredentials.forTemplateWrite, (req, res) => templateController.createTemplate(req, res));
router.put('/:id', (req, res) => templateController.updateTemplate(req, res));
router.delete('/:id', (req, res) => templateController.deleteTemplate(req, res));
router.post('/:id/usage', (req, res) => templateController.incrementUsage(req, res));

module.exports = router;
