// routes/campaignRoutes.js
const express = require('express');
const router = express.Router();
const multer = require('multer');
const { body, param, query, validationResult } = require('express-validator');
const protect = require('../middleware/auth');
const Campaign = require('../models/campaign');
const campaignController = require('../controllers/campaigncontroller');
const upload = multer({ storage: multer.memoryStorage() });

// Validation middleware
const validate = (req, res, next) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
        return res.status(400).json({
            success: false,
            errors: errors.array()
        });
    }
    next();
};

const authorize = (...roles) => (req, res, next) => {
    const activeRole = req.user?.role || req.user?.companyRole;
    if (!roles.includes(activeRole)) {
        return res.status(403).json({
            success: false,
            message: `User role ${activeRole || 'unknown'} is not authorized to access this route`
        });
    }
    next();
};

const isLocalOrMetaCampaignId = (value) => {
    const normalized = String(value || '').trim();
    return /^[a-f\d]{24}$/i.test(normalized) || /^meta_[A-Za-z0-9_]+$/i.test(normalized);
};

// Campaign validation rules
const campaignValidation = [
    body('name')
        .notEmpty().withMessage('Campaign name is required')
        .isLength({ max: 100 }).withMessage('Campaign name cannot exceed 100 characters')
        .trim(),
    
    body('platform')
        .optional()
        .isIn(['facebook', 'instagram', 'both']).withMessage('Platform must be facebook, instagram, or both')
        .default('both'),
    
    body('objective')
        .optional()
        .isIn(['awareness', 'traffic', 'engagement', 'leads', 'sales', 'catalog'])
        .withMessage('Invalid campaign objective')
        .default('awareness'),
    
    body('status')
        .optional()
        .isIn(['draft', 'active', 'paused', 'ended', 'archived'])
        .withMessage('Invalid status')
        .default('draft'),
    
    body('dailyBudget')
        .optional()
        .isFloat({ min: 1 }).withMessage('Daily budget must be at least $1'),
    
    body('lifetimeBudget')
        .optional()
        .isFloat({ min: 1 }).withMessage('Lifetime budget must be at least $1'),
    
    body('startDate')
        .optional()
        .isISO8601().withMessage('Valid start date is required')
        .toDate(),
    
    body('endDate')
        .optional()
        .isISO8601().withMessage('Valid end date is required')
        .toDate()
        .custom((value, { req }) => {
            if (value && req.body.startDate && new Date(value) <= new Date(req.body.startDate)) {
                throw new Error('End date must be after start date');
            }
            return true;
        }),
    
    body('targeting')
        .optional()
        .isLength({ max: 500 }).withMessage('Targeting cannot exceed 500 characters')
        .trim(),
    
    body('spent')
        .optional()
        .isFloat({ min: 0 }).withMessage('Spent must be a positive number'),
    
    body('impressions')
        .optional()
        .isInt({ min: 0 }).withMessage('Impressions must be a positive integer'),
    
    body('clicks')
        .optional()
        .isInt({ min: 0 }).withMessage('Clicks must be a positive integer'),
    
    body('revenue')
        .optional()
        .isFloat({ min: 0 }).withMessage('Revenue must be a positive number'),
    
    // Custom validation to ensure only one budget type is set
    body().custom((value) => {
        if (value.dailyBudget && value.lifetimeBudget) {
            throw new Error('Cannot set both daily and lifetime budget');
        }
        return true;
    })
];

// ==================== PUBLIC ROUTES (No auth required for testing) ====================

/**
 * @route   GET /api/campaigns/test
 * @desc    Test route to check if campaign routes are working
 * @access  Public
 */
router.get('/test', (req, res) => {
    res.json({ 
        success: true, 
        message: 'Campaign routes are working',
        timestamp: new Date().toISOString()
    });
});

/**
 * @route   GET /api/campaigns/debug
 * @desc    Debug endpoint to check if controller is loaded
 * @access  Public
 */
router.get('/debug', (req, res) => {
    res.json({
        success: true,
        message: 'Campaign routes debug endpoint',
        controllers: {
            getCampaigns: typeof campaignController.getCampaigns === 'function',
            createCampaign: typeof campaignController.createCampaign === 'function',
            getCampaign: typeof campaignController.getCampaign === 'function',
            updateCampaign: typeof campaignController.updateCampaign === 'function',
            deleteCampaign: typeof campaignController.deleteCampaign === 'function'
        },
        timestamp: new Date().toISOString()
    });
});

// ==================== APPLY AUTH MIDDLEWARE TO ALL OTHER ROUTES ====================
// All routes after this will require authentication
router.use(protect);

// ==================== STATS AND EXPORT ROUTES ====================

/**
 * @route   GET /api/campaigns/stats/overview
 * @desc    Get campaign statistics overview
 * @access  Private
 */
router.get(
    '/stats/overview',
    [
        query('startDate').optional().isISO8601().withMessage('Valid start date required'),
        query('endDate').optional().isISO8601().withMessage('Valid end date required')
    ],
    validate,
    campaignController.getCampaignStats
);

/**
 * @route   GET /api/campaigns/export
 * @desc    Export campaigns data
 * @access  Private
 */
router.get(
    '/export',
    [
        query('format').optional().isIn(['json', 'csv']).withMessage('Format must be json or csv'),
        query('platform').optional().isIn(['facebook', 'instagram', 'both']),
        query('status').optional().isIn(['draft', 'active', 'paused', 'ended', 'archived']),
        query('startDate').optional().isISO8601(),
        query('endDate').optional().isISO8601()
    ],
    validate,
    campaignController.exportCampaigns
);

// ==================== MAIN CAMPAIGN ROUTES ====================

/**
 * @route   GET /api/campaigns
 * @desc    Get all campaigns with filtering, sorting, pagination
 * @access  Private
 */
router.get(
    '/',
    [
        query('page').optional().isInt({ min: 1 }).toInt(),
        query('limit').optional().isInt({ min: 1, max: 100 }).toInt(),
        query('sort').optional().isString(),
        query('fields').optional().isString(),
        query('search').optional().isString(),
        query('platform').optional().isIn(['facebook', 'instagram', 'both', 'all']),
        query('status').optional().isIn(['draft', 'active', 'paused', 'ended', 'archived', 'all']),
        query('objective').optional().isString(),
        query('startDate').optional().isISO8601(),
        query('endDate').optional().isISO8601()
    ],
    validate,
    campaignController.getCampaigns
);

/**
 * @route   POST /api/campaigns
 * @desc    Create a new campaign
 * @access  Private
 */
router.post(
    '/',
    upload.single('creativeImage'),
    campaignValidation,
    validate,
    campaignController.createCampaign
);

// ==================== SINGLE CAMPAIGN ROUTES ====================

/**
 * @route   GET /api/campaigns/:id
 * @desc    Get single campaign by ID
 * @access  Private
 */
router.get(
    '/:id',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.getCampaign
);

/**
 * @route   PUT /api/campaigns/:id
 * @desc    Update campaign
 * @access  Private
 */
router.put(
    '/:id',
    upload.single('creativeImage'),
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        ...campaignValidation
    ],
    validate,
    campaignController.updateCampaign
);

/**
 * @route   DELETE /api/campaigns/:id
 * @desc    Delete campaign
 * @access  Private
 */
router.delete(
    '/:id',
    campaignController.deleteCampaign
);

// ==================== CAMPAIGN ACTION ROUTES ====================

/**
 * @route   POST /api/campaigns/:id/pause
 * @desc    Pause a campaign
 * @access  Private
 */
router.post(
    '/:id/pause',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.pauseCampaign
);

// Backward-compatible alias
router.put(
    '/:id/pause',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.pauseCampaign
);

/**
 * @route   POST /api/campaigns/:id/resume
 * @desc    Resume a paused campaign
 * @access  Private
 */
router.post(
    '/:id/resume',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.resumeCampaign
);

// Backward-compatible alias
router.put(
    '/:id/resume',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.resumeCampaign
);

/**
 * @route   POST /api/campaigns/:id/duplicate
 * @desc    Duplicate a campaign
 * @access  Private
 */
router.post(
    '/:id/duplicate',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        body('name').optional().isString().trim()
    ],
    validate,
    campaignController.duplicateCampaign
);

router.post(
    '/:id/verify-payment',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.verifyCampaignPayment
);

router.post(
    '/:id/submit-review',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        body('reviewNotes').optional().isString().trim()
    ],
    validate,
    campaignController.submitCampaignForReview
);

router.post(
    '/:id/approve',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        body('reviewNotes').optional().isString().trim()
    ],
    validate,
    campaignController.approveCampaignReview
);

router.post(
    '/:id/reject',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        body('reviewNotes').optional().isString().trim()
    ],
    validate,
    campaignController.rejectCampaignReview
);

router.post(
    '/:id/publish',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID')
    ],
    validate,
    campaignController.publishCampaign
);

/**
 * @route   GET /api/campaigns/:id/performance
 * @desc    Get campaign performance metrics
 * @access  Private
 */
router.get(
    '/:id/performance',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        query('dateRange').optional().isIn(['today', 'yesterday', 'last7days', 'last30days', 'thisMonth', 'lastMonth'])
    ],
    validate,
    campaignController.getCampaignPerformance
);

// ==================== BULK OPERATIONS ROUTES (Admin only) ====================

/**
 * @route   PUT /api/campaigns/bulk/update
 * @desc    Bulk update multiple campaigns
 * @access  Private (Admin/Superadmin only)
 */
router.put(
    '/bulk/update',
    authorize('admin', 'superadmin'),
    [
        body('campaignIds').isArray().withMessage('Campaign IDs must be an array')
            .custom((value) => value.length > 0).withMessage('At least one campaign ID required'),
        body('campaignIds.*').isMongoId().withMessage('Invalid campaign ID format'),
        body('updateData').notEmpty().withMessage('Update data is required')
    ],
    validate,
    campaignController.bulkUpdateCampaigns
);

/**
 * @route   DELETE /api/campaigns/bulk/delete
 * @desc    Bulk delete multiple campaigns
 * @access  Private (Admin/Superadmin only)
 */
router.delete(
    '/bulk/delete',
    authorize('admin', 'superadmin'),
    [
        body('campaignIds').isArray().withMessage('Campaign IDs must be an array')
            .custom((value) => value.length > 0).withMessage('At least one campaign ID required'),
        body('campaignIds.*').isMongoId().withMessage('Invalid campaign ID format')
    ],
    validate,
    campaignController.bulkDeleteCampaigns
);

/**
 * @route   PUT /api/campaigns/bulk/status
 * @desc    Bulk update campaign status
 * @access  Private (Admin/Superadmin only)
 */
router.put(
    '/bulk/status',
    authorize('admin', 'superadmin'),
    [
        body('campaignIds').isArray().withMessage('Campaign IDs must be an array'),
        body('status').isIn(['active', 'paused', 'draft', 'ended', 'archived']).withMessage('Invalid status')
    ],
    validate,
    campaignController.bulkUpdateStatus
);

// ==================== TEMPLATE ROUTES ====================

/**
 * @route   POST /api/campaigns/from-template
 * @desc    Create campaign from template
 * @access  Private
 */
router.post(
    '/from-template',
    [
        body('templateId').isMongoId().withMessage('Valid template ID required'),
        body('name').notEmpty().withMessage('Campaign name required'),
        body('modifications').optional().isObject()
    ],
    validate,
    campaignController.createFromTemplate
);

/**
 * @route   POST /api/campaigns/:id/save-as-template
 * @desc    Save campaign as template
 * @access  Private
 */
router.post(
    '/:id/save-as-template',
    [
        param('id').isMongoId().withMessage('Invalid campaign ID'),
        body('templateName').notEmpty().withMessage('Template name required'),
        body('templateDescription').optional().isString()
    ],
    validate,
    campaignController.saveAsTemplate
);

module.exports = router;
