const express = require('express');
const auth = require('../middleware/auth');
const WhatsAppWorkflow = require('../models/WhatsAppWorkflow');

const router = express.Router();
router.use(auth);

const toSafeString = (value, fallback = '') => {
  const normalized = String(value ?? '').trim();
  return normalized || fallback;
};

const toClientMenu = (doc) => ({
  _id: doc.workflowId,
  id: doc.workflowId,
  promptKey: doc.workflowId,
  menuName: doc.workflowId,
  displayName: doc.name,
  name: doc.name,
  status: doc.status || 'draft',
  workflowConfig: {
    nodes: Array.isArray(doc.nodes) ? doc.nodes : [],
    edges: Array.isArray(doc.edges) ? doc.edges : [],
    settings: doc.metadata || {}
  },
  createdBy: doc.createdBy || '',
  userId: doc.userId || '',
  companyId: doc.companyId || '',
  createdAt: doc.createdAt,
  updatedAt: doc.updatedAt
});

const buildScopedQuery = (req) => ({
  userId: String(req.user?.id || ''),
  companyId: String(req.companyId || '')
});

router.get('/menus', async (req, res) => {
  try {
    const query = buildScopedQuery(req);
    const rows = await WhatsAppWorkflow.find(query).sort({ updatedAt: -1 }).lean();
    return res.json({
      success: true,
      ivrMenus: rows.map(toClientMenu)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to load IVR menus.',
      details: error.message
    });
  }
});

router.get('/menus/:id', async (req, res) => {
  try {
    const workflowId = toSafeString(req.params.id);
    const row = await WhatsAppWorkflow.findOne({
      workflowId,
      ...buildScopedQuery(req)
    }).lean();

    if (!row) {
      return res.status(404).json({
        success: false,
        error: 'IVR menu not found.'
      });
    }

    return res.json({
      success: true,
      ivrMenu: toClientMenu(row)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to load IVR menu.',
      details: error.message
    });
  }
});

router.post('/menus', async (req, res) => {
  try {
    const name = toSafeString(req.body?.name || req.body?.displayName || req.body?.menuName, 'Untitled IVR');
    const workflowId = toSafeString(
      req.body?.workflowId || req.body?.menuId || req.body?.promptKey,
      `ivr_${Date.now().toString(36)}`
    );
    const nodes = Array.isArray(req.body?.nodes) ? req.body.nodes : Array.isArray(req.body?.workflowConfig?.nodes) ? req.body.workflowConfig.nodes : [];
    const edges = Array.isArray(req.body?.edges) ? req.body.edges : Array.isArray(req.body?.workflowConfig?.edges) ? req.body.workflowConfig.edges : [];
    const metadata = req.body?.metadata && typeof req.body.metadata === 'object' ? req.body.metadata : {};

    const created = await WhatsAppWorkflow.create({
      workflowId,
      name,
      description: toSafeString(req.body?.description, ''),
      status: toSafeString(req.body?.status, 'draft'),
      nodes,
      edges,
      metadata,
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || ''),
      createdBy: toSafeString(req.user?.email || req.user?.username || req.user?.id)
    });

    return res.status(201).json({
      success: true,
      ivrMenu: toClientMenu(created)
    });
  } catch (error) {
    if (error?.code === 11000) {
      return res.status(409).json({
        success: false,
        error: 'IVR menu already exists.'
      });
    }
    return res.status(500).json({
      success: false,
      error: 'Failed to create IVR menu.',
      details: error.message
    });
  }
});

router.put('/menus/:id', async (req, res) => {
  try {
    const workflowId = toSafeString(req.params.id);
    const row = await WhatsAppWorkflow.findOne({
      workflowId,
      ...buildScopedQuery(req)
    });

    if (!row) {
      return res.status(404).json({
        success: false,
        error: 'IVR menu not found.'
      });
    }

    if (req.body?.name !== undefined || req.body?.displayName !== undefined || req.body?.menuName !== undefined) {
      row.name = toSafeString(req.body?.name || req.body?.displayName || req.body?.menuName, row.name);
    }
    if (req.body?.description !== undefined) {
      row.description = toSafeString(req.body.description, row.description || '');
    }
    if (req.body?.status !== undefined) {
      row.status = toSafeString(req.body.status, row.status || 'draft');
    }
    if (Array.isArray(req.body?.nodes)) {
      row.nodes = req.body.nodes;
    } else if (Array.isArray(req.body?.workflowConfig?.nodes)) {
      row.nodes = req.body.workflowConfig.nodes;
    }
    if (Array.isArray(req.body?.edges)) {
      row.edges = req.body.edges;
    } else if (Array.isArray(req.body?.workflowConfig?.edges)) {
      row.edges = req.body.workflowConfig.edges;
    }
    if (req.body?.metadata && typeof req.body.metadata === 'object') {
      row.metadata = req.body.metadata;
    }
    row.version = Number(row.version || 1) + 1;
    await row.save();

    return res.json({
      success: true,
      ivrMenu: toClientMenu(row)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to update IVR menu.',
      details: error.message
    });
  }
});

router.delete('/menus/:id', async (req, res) => {
  try {
    const workflowId = toSafeString(req.params.id);
    const deleted = await WhatsAppWorkflow.findOneAndDelete({
      workflowId,
      ...buildScopedQuery(req)
    });

    if (!deleted) {
      return res.status(404).json({
        success: false,
        error: 'IVR menu not found.'
      });
    }

    return res.json({
      success: true,
      message: 'IVR menu deleted successfully.'
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to delete IVR menu.',
      details: error.message
    });
  }
});

router.post('/menus/:id/test', async (req, res) => {
  try {
    const workflowId = toSafeString(req.params.id);
    const row = await WhatsAppWorkflow.findOne({
      workflowId,
      ...buildScopedQuery(req)
    }).lean();

    if (!row) {
      return res.status(404).json({
        success: false,
        error: 'IVR menu not found.'
      });
    }

    return res.json({
      success: true,
      message: 'IVR test scheduled.',
      ivrMenu: toClientMenu(row),
      phoneNumber: toSafeString(req.body?.phoneNumber, '')
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to test IVR menu.',
      details: error.message
    });
  }
});

module.exports = router;
