const express = require('express');

const { getMetaLeads } = require('../controllers/metaLeadController');

const router = express.Router();

router.get('/:formId?', getMetaLeads);

module.exports = router;
