const express = require('express');

const auth = require('../middleware/auth');
const { getMetaLeads } = require('../controllers/metaLeadController');

const router = express.Router();

router.get('/', auth, getMetaLeads);

module.exports = router;
