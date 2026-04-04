const express = require('express');
const auth = require('../middleware/auth');
const WhatsAppWorkflow = require('../models/WhatsAppWorkflow');

const router = express.Router();
router.use(auth);

const MAX_NODES = 300;
const MAX_EDGES = 1000;

const TRIGGER_NODE_TYPES = new Set([
  'message-received',
  'schedule',
  'keyword-match',
  'contact-joined'
]);

const toSafeString = (value, fallback = '') => {
  const next = String(value ?? '').trim();
  return next || fallback;
};

const clone = (value) => JSON.parse(JSON.stringify(value ?? null));

const nowIso = () => new Date().toISOString();

const interpolateTemplate = (template, context = {}) => {
  if (typeof template !== 'string') return '';
  return template.replace(/\{\{\s*([^}]+)\s*\}\}/g, (_, key) => {
    const path = String(key || '')
      .split('.')
      .map((part) => part.trim())
      .filter(Boolean);
    let cursor = context;
    for (const segment of path) {
      if (cursor == null || typeof cursor !== 'object' || !(segment in cursor)) {
        return '';
      }
      cursor = cursor[segment];
    }
    return cursor == null ? '' : String(cursor);
  });
};

const getPath = (obj, path, defaultValue = undefined) => {
  const parts = String(path || '')
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.length) return defaultValue;
  let cursor = obj;
  for (const part of parts) {
    if (cursor == null || typeof cursor !== 'object' || !(part in cursor)) {
      return defaultValue;
    }
    cursor = cursor[part];
  }
  return cursor;
};

const asNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const evaluateOperator = (left, operator, right) => {
  const op = String(operator || '').toLowerCase();
  const lText = left == null ? '' : String(left);
  const rText = right == null ? '' : String(right);
  const lNum = Number(left);
  const rNum = Number(right);
  switch (op) {
    case 'exists':
      return left !== undefined && left !== null && lText !== '';
    case 'not-exists':
    case 'not_exists':
      return left === undefined || left === null || lText === '';
    case 'equals':
    case '=':
      return lText === rText;
    case 'not-equals':
    case '!=':
      return lText !== rText;
    case 'contains':
      return lText.toLowerCase().includes(rText.toLowerCase());
    case 'starts-with':
      return lText.toLowerCase().startsWith(rText.toLowerCase());
    case 'ends-with':
      return lText.toLowerCase().endsWith(rText.toLowerCase());
    case '>':
    case 'greater-than':
      return Number.isFinite(lNum) && Number.isFinite(rNum) ? lNum > rNum : false;
    case '<':
    case 'less-than':
      return Number.isFinite(lNum) && Number.isFinite(rNum) ? lNum < rNum : false;
    case '>=':
    case 'greater-than-equals':
      return Number.isFinite(lNum) && Number.isFinite(rNum) ? lNum >= rNum : false;
    case '<=':
    case 'less-than-equals':
      return Number.isFinite(lNum) && Number.isFinite(rNum) ? lNum <= rNum : false;
    default:
      return Boolean(left);
  }
};

const normalizeNode = (raw, index) => {
  const source = raw && typeof raw === 'object' ? raw : {};
  const id = toSafeString(source.id, `node_${index + 1}`);
  const type = toSafeString(source.type, 'unknown');
  const position = source.position && typeof source.position === 'object'
    ? {
        x: asNumber(source.position.x, 0),
        y: asNumber(source.position.y, 0)
      }
    : { x: 0, y: 0 };

  return {
    ...source,
    id,
    type,
    position
  };
};

const normalizeEdge = (raw, index) => {
  const source = raw && typeof raw === 'object' ? raw : {};
  const id = toSafeString(source.id, `edge_${index + 1}`);
  return {
    ...source,
    id,
    source: toSafeString(source.source),
    sourceHandle: toSafeString(source.sourceHandle),
    target: toSafeString(source.target),
    targetHandle: toSafeString(source.targetHandle)
  };
};

const analyzeGraph = (nodes, edges) => {
  const errors = [];
  const nodeIds = new Set();
  const adjacency = new Map();
  const inDegree = new Map();

  for (const node of nodes) {
    if (!node.id) {
      errors.push('Every node must include an id.');
      continue;
    }
    if (nodeIds.has(node.id)) {
      errors.push(`Duplicate node id detected: ${node.id}`);
      continue;
    }
    nodeIds.add(node.id);
    adjacency.set(node.id, []);
    inDegree.set(node.id, 0);
  }

  const seenEdges = new Set();

  for (const edge of edges) {
    if (!edge.source || !edge.target) {
      errors.push(`Edge ${edge.id || '(unknown)'} is missing source or target.`);
      continue;
    }
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) {
      errors.push(`Edge ${edge.id || '(unknown)'} references a missing node.`);
      continue;
    }
    if (edge.source === edge.target) {
      errors.push(`Self connection is not allowed on node ${edge.source}.`);
      continue;
    }
    const edgeKey = `${edge.source}:${edge.sourceHandle}->${edge.target}:${edge.targetHandle}`;
    if (seenEdges.has(edgeKey)) {
      errors.push(`Duplicate connection detected: ${edgeKey}`);
      continue;
    }
    seenEdges.add(edgeKey);
    adjacency.get(edge.source).push(edge.target);
    inDegree.set(edge.target, (inDegree.get(edge.target) || 0) + 1);
  }

  const state = new Map();
  const cyclePath = [];
  let cycleFound = false;

  const dfs = (nodeId, stack) => {
    if (cycleFound) return;
    state.set(nodeId, 1);
    stack.push(nodeId);
    const neighbors = adjacency.get(nodeId) || [];
    for (const next of neighbors) {
      const nextState = state.get(next) || 0;
      if (nextState === 0) {
        dfs(next, stack);
      } else if (nextState === 1) {
        cycleFound = true;
        const startAt = stack.indexOf(next);
        if (startAt >= 0) {
          cyclePath.push(...stack.slice(startAt), next);
        } else {
          cyclePath.push(next);
        }
        return;
      }
    }
    stack.pop();
    state.set(nodeId, 2);
  };

  for (const nodeId of nodeIds) {
    if ((state.get(nodeId) || 0) === 0) {
      dfs(nodeId, []);
      if (cycleFound) {
        break;
      }
    }
  }

  if (cycleFound) {
    errors.push(`Circular dependency detected: ${cyclePath.join(' -> ')}`);
  }

  return {
    errors,
    inDegree,
    nodeIds
  };
};

const normalizeWorkflowPayload = (body, workflowIdFromPath = '') => {
  const payload = body && typeof body === 'object' ? body : {};
  const source = payload.workflow && typeof payload.workflow === 'object' ? payload.workflow : payload;

  const workflowId = toSafeString(
    workflowIdFromPath || source.id || source.workflowId,
    `wa_wf_${Date.now().toString(36)}`
  );
  const name = toSafeString(source.name, 'Untitled WhatsApp Workflow');
  const description = toSafeString(source.description, '');
  const status = ['draft', 'active', 'archived'].includes(String(source.status || '').toLowerCase())
    ? String(source.status).toLowerCase()
    : 'draft';

  const rawNodes = Array.isArray(source.nodes) ? source.nodes : [];
  const rawEdges = Array.isArray(source.edges) ? source.edges : [];
  const nodes = rawNodes.map(normalizeNode);
  const edges = rawEdges.map(normalizeEdge);

  const diagnostics = analyzeGraph(nodes, edges);

  return {
    workflowId,
    name,
    description,
    status,
    nodes,
    edges,
    metadata: source.metadata && typeof source.metadata === 'object' ? source.metadata : {},
    diagnostics
  };
};

const toClientWorkflow = (doc) => ({
  id: doc.workflowId,
  workflowId: doc.workflowId,
  name: doc.name,
  description: doc.description || '',
  status: doc.status || 'draft',
  nodes: Array.isArray(doc.nodes) ? doc.nodes : [],
  edges: Array.isArray(doc.edges) ? doc.edges : [],
  metadata: doc.metadata || {},
  createdAt: doc.createdAt,
  updatedAt: doc.updatedAt,
  lastRunAt: doc.lastRunAt || null
});

const resolveWorkflowForExecution = async (req, workflowIdHint = '') => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const payloadWorkflow = body.workflow && typeof body.workflow === 'object' ? body.workflow : null;

  if (payloadWorkflow) {
    const normalized = normalizeWorkflowPayload(payloadWorkflow, workflowIdHint || body.workflowId);
    return {
      workflowId: normalized.workflowId,
      workflow: {
        id: normalized.workflowId,
        name: normalized.name,
        description: normalized.description,
        status: normalized.status,
        nodes: normalized.nodes,
        edges: normalized.edges
      },
      diagnostics: normalized.diagnostics
    };
  }

  const requestedId = toSafeString(workflowIdHint || body.workflowId);
  if (!requestedId) {
    return { error: 'workflowId or workflow payload is required.' };
  }

  const record = await WhatsAppWorkflow.findOne({
    workflowId: requestedId,
    userId: String(req.user?.id || ''),
    companyId: String(req.companyId || '')
  }).lean();

  if (!record) {
    return { error: `Workflow "${requestedId}" was not found.` };
  }

  return {
    workflowId: record.workflowId,
    workflow: toClientWorkflow(record),
    diagnostics: analyzeGraph(record.nodes || [], record.edges || [])
  };
};

const chooseStartNodeIds = (nodes, inDegreeMap) => {
  const triggerIds = nodes
    .filter((node) => TRIGGER_NODE_TYPES.has(String(node.type || '').toLowerCase()))
    .map((node) => node.id);
  if (triggerIds.length) return triggerIds;
  return nodes
    .filter((node) => (inDegreeMap.get(node.id) || 0) === 0)
    .map((node) => node.id);
};

const selectOutgoingEdges = (node, edges, runtimeContext) => {
  const outgoing = edges.filter((edge) => edge.source === node.id);
  if (!outgoing.length) return [];

  const type = String(node.type || '').toLowerCase();
  if (type === 'if-else') {
    const conditions = Array.isArray(node?.data?.conditions) ? node.data.conditions : [];
    const isTrue = conditions.every((condition) => {
      const field = toSafeString(condition?.field);
      const operator = toSafeString(condition?.operator, 'exists');
      const value = condition?.value;
      const left = getPath(runtimeContext, field, getPath(runtimeContext, `data.${field}`));
      return evaluateOperator(left, operator, value);
    });
    const preferredHandle = isTrue ? 'yes' : 'no';
    const direct = outgoing.find((edge) => edge.sourceHandle === preferredHandle);
    return direct ? [direct] : outgoing.slice(0, 1);
  }

  if (type === 'random-split') {
    const weighted = outgoing.map((edge) => {
      const handle = toSafeString(edge.sourceHandle || 'a', 'a');
      const weight = asNumber(node?.data?.weights?.[handle], 1);
      return { edge, weight: weight > 0 ? weight : 1 };
    });
    const total = weighted.reduce((sum, item) => sum + item.weight, 0);
    const seed = Math.random() * total;
    let cursor = 0;
    for (const item of weighted) {
      cursor += item.weight;
      if (seed <= cursor) {
        return [item.edge];
      }
    }
    return [weighted[0].edge];
  }

  return outgoing;
};

const buildNodeOutput = (node, runtimeContext = {}) => {
  const data = node?.data || {};
  const type = String(node?.type || '').toLowerCase();

  if (type === 'send-text') {
    const rendered = interpolateTemplate(String(data.message || ''), runtimeContext);
    return { previewMessage: rendered };
  }

  if (type === 'ai-reply') {
    const prompt = interpolateTemplate(String(data.prompt || ''), runtimeContext);
    return {
      model: toSafeString(data.model, 'gpt-5.4-mini'),
      promptPreview: prompt.slice(0, 300),
      generatedText: 'AI reply generated in simulation mode.'
    };
  }

  if (type === 'http-request') {
    return {
      method: toSafeString(data.method, 'GET'),
      url: interpolateTemplate(String(data.url || ''), runtimeContext),
      simulatedStatus: 200
    };
  }

  if (type === 'if-else') {
    return {
      conditions: Array.isArray(data.conditions) ? data.conditions.length : 0
    };
  }

  if (type === 'wait') {
    const duration = asNumber(data.duration, 0);
    return {
      delay: duration,
      unit: toSafeString(data.unit, 'minutes')
    };
  }

  return { ok: true };
};

const simulateExecution = (workflow, runtimeContext = {}, opts = {}) => {
  const nodes = Array.isArray(workflow.nodes) ? workflow.nodes : [];
  const edges = Array.isArray(workflow.edges) ? workflow.edges : [];
  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  const diagnostics = analyzeGraph(nodes, edges);

  const steps = [];
  const startNodeIds = chooseStartNodeIds(nodes, diagnostics.inDegree);
  const queue = [...startNodeIds];
  const visited = new Set();
  const maxSteps = Number.isFinite(opts.maxSteps) ? opts.maxSteps : 500;

  while (queue.length && steps.length < maxSteps) {
    const nodeId = queue.shift();
    if (!nodeId || visited.has(nodeId)) continue;
    const node = nodeById.get(nodeId);
    if (!node) continue;

    visited.add(nodeId);
    const nodeLabel = toSafeString(node?.data?.label, node.type || node.id);
    const output = buildNodeOutput(node, runtimeContext);

    steps.push({
      index: steps.length + 1,
      nodeId: node.id,
      nodeType: node.type,
      label: nodeLabel,
      status: 'success',
      timestamp: nowIso(),
      output
    });

    const nextEdges = selectOutgoingEdges(node, edges, runtimeContext);
    for (const edge of nextEdges) {
      if (edge.target && !visited.has(edge.target)) {
        queue.push(edge.target);
      }
    }
  }

  return {
    runId: `run_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`,
    startedAt: nowIso(),
    finishedAt: nowIso(),
    summary: {
      startNodeCount: startNodeIds.length,
      visitedNodes: steps.length,
      totalNodes: nodes.length,
      totalEdges: edges.length
    },
    diagnostics,
    steps
  };
};

router.get('/', async (req, res) => {
  try {
    const query = {
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || '')
    };

    const rows = await WhatsAppWorkflow.find(query).sort({ updatedAt: -1 }).lean();
    return res.json({
      success: true,
      workflows: rows.map(toClientWorkflow)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to fetch workflows.',
      details: error.message
    });
  }
});

router.get('/:id', async (req, res) => {
  try {
    const workflowId = toSafeString(req.params.id);
    const row = await WhatsAppWorkflow.findOne({
      workflowId,
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || '')
    }).lean();

    if (!row) {
      return res.status(404).json({
        success: false,
        error: 'Workflow not found.'
      });
    }

    return res.json({
      success: true,
      workflow: toClientWorkflow(row)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to fetch workflow.',
      details: error.message
    });
  }
});

router.post('/', async (req, res) => {
  try {
    const normalized = normalizeWorkflowPayload(req.body);
    if (normalized.nodes.length > MAX_NODES) {
      return res.status(400).json({
        success: false,
        error: `Workflow exceeds node limit (${MAX_NODES}).`
      });
    }
    if (normalized.edges.length > MAX_EDGES) {
      return res.status(400).json({
        success: false,
        error: `Workflow exceeds edge limit (${MAX_EDGES}).`
      });
    }
    if (normalized.diagnostics.errors.length) {
      return res.status(400).json({
        success: false,
        error: 'Workflow graph validation failed.',
        issues: normalized.diagnostics.errors
      });
    }

    const doc = await WhatsAppWorkflow.create({
      workflowId: normalized.workflowId,
      name: normalized.name,
      description: normalized.description,
      status: normalized.status,
      nodes: normalized.nodes,
      edges: normalized.edges,
      metadata: normalized.metadata,
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || ''),
      createdBy: toSafeString(req.user?.email || req.user?.username || req.user?.id)
    });

    return res.status(201).json({
      success: true,
      message: 'Workflow created successfully.',
      workflow: toClientWorkflow(doc)
    });
  } catch (error) {
    if (error && error.code === 11000) {
      return res.status(409).json({
        success: false,
        error: 'Workflow id already exists.'
      });
    }
    return res.status(500).json({
      success: false,
      error: 'Failed to create workflow.',
      details: error.message
    });
  }
});

router.put('/:id', async (req, res) => {
  try {
    const normalized = normalizeWorkflowPayload(req.body, req.params.id);
    if (normalized.nodes.length > MAX_NODES) {
      return res.status(400).json({
        success: false,
        error: `Workflow exceeds node limit (${MAX_NODES}).`
      });
    }
    if (normalized.edges.length > MAX_EDGES) {
      return res.status(400).json({
        success: false,
        error: `Workflow exceeds edge limit (${MAX_EDGES}).`
      });
    }
    if (normalized.diagnostics.errors.length) {
      return res.status(400).json({
        success: false,
        error: 'Workflow graph validation failed.',
        issues: normalized.diagnostics.errors
      });
    }

    const filter = {
      workflowId: normalized.workflowId,
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || '')
    };

    const existing = await WhatsAppWorkflow.findOne(filter);
    if (!existing) {
      return res.status(404).json({
        success: false,
        error: 'Workflow not found.'
      });
    }

    existing.name = normalized.name;
    existing.description = normalized.description;
    existing.status = normalized.status;
    existing.nodes = clone(normalized.nodes) || [];
    existing.edges = clone(normalized.edges) || [];
    existing.metadata = normalized.metadata || {};
    existing.version = Number(existing.version || 1) + 1;
    await existing.save();

    return res.json({
      success: true,
      message: 'Workflow saved successfully.',
      workflow: toClientWorkflow(existing)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to save workflow.',
      details: error.message
    });
  }
});

router.delete('/:id', async (req, res) => {
  try {
    const workflowId = toSafeString(req.params.id);
    const deleted = await WhatsAppWorkflow.findOneAndDelete({
      workflowId,
      userId: String(req.user?.id || ''),
      companyId: String(req.companyId || '')
    });

    if (!deleted) {
      return res.status(404).json({
        success: false,
        error: 'Workflow not found.'
      });
    }

    return res.json({
      success: true,
      message: 'Workflow deleted successfully.'
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to delete workflow.',
      details: error.message
    });
  }
});

const handleRunWorkflow = async (req, res, workflowIdFromPath = '') => {
  try {
    const resolved = await resolveWorkflowForExecution(req, workflowIdFromPath);
    if (resolved.error) {
      return res.status(400).json({ success: false, error: resolved.error });
    }
    if (resolved.diagnostics.errors.length) {
      return res.status(400).json({
        success: false,
        error: 'Workflow graph validation failed.',
        issues: resolved.diagnostics.errors
      });
    }

    const runtimeContext =
      req.body && typeof req.body.runtimeContext === 'object' ? req.body.runtimeContext : {};
    const execution = simulateExecution(resolved.workflow, runtimeContext, { maxSteps: 500 });

    await WhatsAppWorkflow.updateOne(
      {
        workflowId: resolved.workflowId,
        userId: String(req.user?.id || ''),
        companyId: String(req.companyId || '')
      },
      { $set: { lastRunAt: new Date() } }
    );

    return res.json({
      success: true,
      message: `Workflow "${resolved.workflow.name}" executed successfully.`,
      workflowId: resolved.workflowId,
      execution
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to run workflow.',
      details: error.message
    });
  }
};

router.post('/run', async (req, res) => handleRunWorkflow(req, res));
router.post('/:id/run', async (req, res) => handleRunWorkflow(req, res, req.params.id));

const handleTestNode = async (req, res, workflowIdFromPath = '') => {
  try {
    const resolved = await resolveWorkflowForExecution(req, workflowIdFromPath);
    if (resolved.error) {
      return res.status(400).json({ success: false, error: resolved.error });
    }
    if (resolved.diagnostics.errors.length) {
      return res.status(400).json({
        success: false,
        error: 'Workflow graph validation failed.',
        issues: resolved.diagnostics.errors
      });
    }

    const runtimeContext =
      req.body && typeof req.body.runtimeContext === 'object' ? req.body.runtimeContext : {};
    const requestedNodeId = toSafeString(req.body?.nodeId || req.body?.node?.id);
    if (!requestedNodeId) {
      return res.status(400).json({
        success: false,
        error: 'nodeId is required for test-node.'
      });
    }

    const node = (Array.isArray(resolved.workflow.nodes) ? resolved.workflow.nodes : []).find(
      (item) => String(item.id) === requestedNodeId
    );

    if (!node) {
      return res.status(404).json({
        success: false,
        error: `Node "${requestedNodeId}" was not found in workflow.`
      });
    }

    return res.json({
      success: true,
      message: `Node "${toSafeString(node?.data?.label, node.type)}" tested successfully.`,
      workflowId: resolved.workflowId,
      nodeId: requestedNodeId,
      result: {
        status: 'success',
        testedAt: nowIso(),
        output: buildNodeOutput(node, runtimeContext)
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to test node.',
      details: error.message
    });
  }
};

router.post('/test-node', async (req, res) => handleTestNode(req, res));
router.post('/:id/test-node', async (req, res) => handleTestNode(req, res, req.params.id));

module.exports = router;
