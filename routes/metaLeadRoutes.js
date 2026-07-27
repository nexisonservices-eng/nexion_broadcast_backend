const express = require('express');

const { getMetaLeads } = require('../controllers/metaLeadController');

const router = express.Router();

router.get('/', getMetaLeads);

module.exports = router;
