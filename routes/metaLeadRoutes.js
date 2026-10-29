const express = require('express');

const { getMetaLeads } = require('../controllers/metaLeadController');
const auth = require('../middleware/auth');

const router = express.Router();

router.get('/:formId?', auth, getMetaLeads);

module.exports = router;
