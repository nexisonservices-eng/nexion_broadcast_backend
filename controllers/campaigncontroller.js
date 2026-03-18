// controllers/campaignController.js
const Campaign = require('../models/campaign');
// Ensure User model is registered for population
require('../models/User');
const APIFeatures = require('../utils/apifeature');
const { validationResult } = require('express-validator');

// @desc    Get all campaigns
// @route   GET /api/campaigns
// @access  Private
exports.getCampaigns = async (req, res) => {
    try {
        // Build query
        const query = Campaign.find();
        
        // Add user filter (users can only see their own campaigns)
        if (req.user.role !== 'superadmin') {
            query.where('createdBy').equals(req.user.id);
        }
        
        // Apply filters, sorting, pagination
        const features = new APIFeatures(query, req.query)
            .filter()
            .sort()
            .limitFields()
            .paginate()
            .search(['name', 'objective']);

        // Execute query
        const campaigns = await features.query
            .populate('createdBy', 'name email')
            .populate('updatedBy', 'name email');

        // Get total count for pagination
        const totalCount = await Campaign.countDocuments(features.filterConditions);

        // Get statistics
        const stats = await Campaign.aggregate([
            {
                $match: features.filterConditions
            },
            {
                $group: {
                    _id: null,
                    totalSpent: { $sum: '$spent' },
                    totalRevenue: { $sum: '$revenue' },
                    totalImpressions: { $sum: '$impressions' },
                    totalClicks: { $sum: '$clicks' },
                    avgCTR: { $avg: '$ctr' },
                    avgCPC: { $avg: '$cpc' }
                }
            }
        ]);

        res.status(200).json({
            success: true,
            count: campaigns.length,
            total: totalCount,
            pagination: {
                page: parseInt(req.query.page) || 1,
                limit: parseInt(req.query.limit) || 10,
                total: totalCount,
                pages: Math.ceil(totalCount / (parseInt(req.query.limit) || 10))
            },
            stats: stats[0] || {
                totalSpent: 0,
                totalRevenue: 0,
                totalImpressions: 0,
                totalClicks: 0,
                avgCTR: 0,
                avgCPC: 0
            },
            data: campaigns
        });
    } catch (error) {
        console.error('Error fetching campaigns:', error);
        res.status(500).json({
            success: false,
            message: 'Error fetching campaigns',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Get single campaign
// @route   GET /api/campaigns/:id
// @access  Private
exports.getCampaign = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id)
            .populate('createdBy', 'name email')
            .populate('updatedBy', 'name email');

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        if (req.user.role !== 'superadmin' && campaign.createdBy._id.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to view this campaign'
            });
        }

        res.status(200).json({
            success: true,
            data: campaign
        });
    } catch (error) {
        console.error('Error fetching campaign:', error);
        
        // Check if error is due to invalid ObjectId
        if (error.name === 'CastError') {
            return res.status(400).json({
                success: false,
                message: 'Invalid campaign ID'
            });
        }

        res.status(500).json({
            success: false,
            message: 'Error fetching campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Create campaign
// @route   POST /api/campaigns
// @access  Private
exports.createCampaign = async (req, res) => {
    try {
        // Check for validation errors
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({
                success: false,
                errors: errors.array()
            });
        }

        // Add user to request body
        req.body.createdBy = req.user.id;

        // Validate budget
        if (req.body.dailyBudget && req.body.lifetimeBudget) {
            return res.status(400).json({
                success: false,
                message: 'Cannot set both daily and lifetime budget'
            });
        }

        // Create campaign
        const campaign = await Campaign.create(req.body);

        res.status(201).json({
            success: true,
            message: 'Campaign created successfully',
            data: campaign
        });
    } catch (error) {
        console.error('Error creating campaign:', error);
        
        // Handle duplicate key error
        if (error.code === 11000) {
            return res.status(400).json({
                success: false,
                message: 'Duplicate field value entered'
            });
        }

        res.status(500).json({
            success: false,
            message: 'Error creating campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Update campaign
// @route   PUT /api/campaigns/:id
// @access  Private
exports.updateCampaign = async (req, res) => {
    try {
        // Check for validation errors
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({
                success: false,
                errors: errors.array()
            });
        }

        let campaign = await Campaign.findById(req.params.id);

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        if (req.user.role !== 'superadmin' && campaign.createdBy.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to update this campaign'
            });
        }

        // Add updated by user
        req.body.updatedBy = req.user.id;

        // Validate budget
        if (req.body.dailyBudget && req.body.lifetimeBudget) {
            return res.status(400).json({
                success: false,
                message: 'Cannot set both daily and lifetime budget'
            });
        }

        // Update campaign
        campaign = await Campaign.findByIdAndUpdate(
            req.params.id,
            req.body,
            {
                new: true,
                runValidators: true
            }
        ).populate('createdBy', 'name email')
         .populate('updatedBy', 'name email');

        res.status(200).json({
            success: true,
            message: 'Campaign updated successfully',
            data: campaign
        });
    } catch (error) {
        console.error('Error updating campaign:', error);
        
        // Check if error is due to invalid ObjectId
        if (error.name === 'CastError') {
            return res.status(400).json({
                success: false,
                message: 'Invalid campaign ID'
            });
        }

        res.status(500).json({
            success: false,
            message: 'Error updating campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Delete campaign
// @route   DELETE /api/campaigns/:id
// @access  Private
exports.deleteCampaign = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        if (req.user.role !== 'superadmin' && campaign.createdBy.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to delete this campaign'
            });
        }

        await campaign.deleteOne();

        res.status(200).json({
            success: true,
            message: 'Campaign deleted successfully'
        });
    } catch (error) {
        console.error('Error deleting campaign:', error);
        
        // Check if error is due to invalid ObjectId
        if (error.name === 'CastError') {
            return res.status(400).json({
                success: false,
                message: 'Invalid campaign ID'
            });
        }

        res.status(500).json({
            success: false,
            message: 'Error deleting campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Pause campaign
// @route   PUT /api/campaigns/:id/pause
// @access  Private
exports.pauseCampaign = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        if (req.user.role !== 'superadmin' && campaign.createdBy.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to pause this campaign'
            });
        }

        campaign.status = 'paused';
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign paused successfully',
            data: campaign
        });
    } catch (error) {
        console.error('Error pausing campaign:', error);
        res.status(500).json({
            success: false,
            message: 'Error pausing campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Resume campaign
// @route   PUT /api/campaigns/:id/resume
// @access  Private
exports.resumeCampaign = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        if (req.user.role !== 'superadmin' && campaign.createdBy.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to resume this campaign'
            });
        }

        campaign.status = 'active';
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign resumed successfully',
            data: campaign
        });
    } catch (error) {
        console.error('Error resuming campaign:', error);
        res.status(500).json({
            success: false,
            message: 'Error resuming campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Duplicate campaign
// @route   POST /api/campaigns/:id/duplicate
// @access  Private
exports.duplicateCampaign = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        if (req.user.role !== 'superadmin' && campaign.createdBy.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to duplicate this campaign'
            });
        }

        // Create duplicate campaign
        const duplicateData = campaign.toObject();
        
        // Remove fields that should be unique or not copied
        delete duplicateData._id;
        delete duplicateData.metaCampaignId;
        delete duplicateData.createdAt;
        delete duplicateData.updatedAt;
        delete duplicateData.metaResponse;
        delete duplicateData.__v;
        
        // Modify name to indicate it's a copy
        duplicateData.name = `${duplicateData.name} (Copy)`;
        duplicateData.status = 'draft';
        duplicateData.createdBy = req.user.id;
        
        // Reset performance metrics
        duplicateData.spent = 0;
        duplicateData.impressions = 0;
        duplicateData.clicks = 0;
        duplicateData.ctr = 0;
        duplicateData.cpc = 0;
        duplicateData.revenue = 0;

        const newCampaign = await Campaign.create(duplicateData);

        res.status(201).json({
            success: true,
            message: 'Campaign duplicated successfully',
            data: newCampaign
        });
    } catch (error) {
        console.error('Error duplicating campaign:', error);
        res.status(500).json({
            success: false,
            message: 'Error duplicating campaign',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Get campaign statistics
// @route   GET /api/campaigns/stats/overview
// @access  Private
exports.getCampaignStats = async (req, res) => {
    try {
        // Build filter based on user role
        const filter = {};
        
        if (req.user.role !== 'superadmin') {
            filter.createdBy = req.user.id;
        }

        // Add date range filter if provided
        if (req.query.startDate && req.query.endDate) {
            filter.startDate = { $gte: new Date(req.query.startDate) };
            filter.endDate = { $lte: new Date(req.query.endDate) };
        }

        const stats = await Campaign.aggregate([
            { $match: filter },
            {
                $facet: {
                    overview: [
                        {
                            $group: {
                                _id: null,
                                totalCampaigns: { $sum: 1 },
                                activeCampaigns: {
                                    $sum: { $cond: [{ $eq: ['$status', 'active'] }, 1, 0] }
                                },
                                pausedCampaigns: {
                                    $sum: { $cond: [{ $eq: ['$status', 'paused'] }, 1, 0] }
                                },
                                draftCampaigns: {
                                    $sum: { $cond: [{ $eq: ['$status', 'draft'] }, 1, 0] }
                                },
                                endedCampaigns: {
                                    $sum: { $cond: [{ $eq: ['$status', 'ended'] }, 1, 0] }
                                },
                                totalSpent: { $sum: '$spent' },
                                totalRevenue: { $sum: '$revenue' },
                                totalImpressions: { $sum: '$impressions' },
                                totalClicks: { $sum: '$clicks' },
                                avgCTR: { $avg: '$ctr' },
                                avgCPC: { $avg: '$cpc' }
                            }
                        }
                    ],
                    byPlatform: [
                        {
                            $group: {
                                _id: '$platform',
                                count: { $sum: 1 },
                                spent: { $sum: '$spent' },
                                revenue: { $sum: '$revenue' }
                            }
                        }
                    ],
                    byStatus: [
                        {
                            $group: {
                                _id: '$status',
                                count: { $sum: 1 }
                            }
                        }
                    ],
                    byObjective: [
                        {
                            $group: {
                                _id: '$objective',
                                count: { $sum: 1 },
                                avgCTR: { $avg: '$ctr' },
                                avgCPC: { $avg: '$cpc' }
                            }
                        }
                    ]
                }
            }
        ]);

        res.status(200).json({
            success: true,
            data: {
                overview: stats[0].overview[0] || {
                    totalCampaigns: 0,
                    activeCampaigns: 0,
                    pausedCampaigns: 0,
                    draftCampaigns: 0,
                    endedCampaigns: 0,
                    totalSpent: 0,
                    totalRevenue: 0,
                    totalImpressions: 0,
                    totalClicks: 0,
                    avgCTR: 0,
                    avgCPC: 0
                },
                byPlatform: stats[0].byPlatform,
                byStatus: stats[0].byStatus,
                byObjective: stats[0].byObjective
            }
        });
    } catch (error) {
        console.error('Error fetching campaign stats:', error);
        res.status(500).json({
            success: false,
            message: 'Error fetching campaign statistics',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Bulk update campaigns
// @route   PUT /api/campaigns/bulk
// @access  Private (Admin only)
exports.bulkUpdateCampaigns = async (req, res) => {
    try {
        const { campaignIds, updateData } = req.body;

        if (!campaignIds || !Array.isArray(campaignIds) || campaignIds.length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Please provide campaign IDs array'
            });
        }

        // Check authorization for each campaign
        const campaigns = await Campaign.find({
            _id: { $in: campaignIds }
        });

        if (req.user.role !== 'superadmin') {
            const unauthorized = campaigns.some(
                campaign => campaign.createdBy.toString() !== req.user.id
            );

            if (unauthorized) {
                return res.status(403).json({
                    success: false,
                    message: 'Not authorized to update one or more campaigns'
                });
            }
        }

        // Add updated by user
        updateData.updatedBy = req.user.id;

        // Perform bulk update
        const result = await Campaign.updateMany(
            { _id: { $in: campaignIds } },
            updateData,
            { runValidators: true }
        );

        res.status(200).json({
            success: true,
            message: `Updated ${result.modifiedCount} campaigns successfully`,
            data: result
        });
    } catch (error) {
        console.error('Error in bulk update:', error);
        res.status(500).json({
            success: false,
            message: 'Error performing bulk update',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// @desc    Export campaigns
// @route   GET /api/campaigns/export
// @access  Private
exports.exportCampaigns = async (req, res) => {
    try {
        // Build query
        const query = Campaign.find();
        
        if (req.user.role !== 'superadmin') {
            query.where('createdBy').equals(req.user.id);
        }

        // Apply filters
        const features = new APIFeatures(query, req.query)
            .filter()
            .sort();

        const campaigns = await features.query
            .select('-__v -metaResponse')
            .populate('createdBy', 'name email')
            .lean();

        // Format data for export
        const exportData = campaigns.map(campaign => ({
            ID: campaign._id,
            Name: campaign.name,
            Platform: campaign.platform,
            Objective: campaign.objective,
            Status: campaign.status,
            'Daily Budget': campaign.dailyBudget || 'N/A',
            'Lifetime Budget': campaign.lifetimeBudget || 'N/A',
            'Start Date': campaign.startDate,
            'End Date': campaign.endDate || 'Ongoing',
            Targeting: campaign.targeting,
            Spent: campaign.spent,
            Impressions: campaign.impressions,
            Clicks: campaign.clicks,
            CTR: `${campaign.ctr}%`,
            CPC: campaign.cpc,
            Revenue: campaign.revenue,
            ROAS: campaign.roas,
            'ROI %': campaign.roi,
            Created: campaign.createdAt,
            'Created By': campaign.createdBy?.name || 'Unknown'
        }));

        res.status(200).json({
            success: true,
            count: exportData.length,
            data: exportData
        });
    } catch (error) {
        console.error('Error exporting campaigns:', error);
        res.status(500).json({
            success: false,
            message: 'Error exporting campaigns',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

// ======== PLACEHOLDER / BASIC IMPLEMENTATIONS FOR ROUTES EXPECTED BY campaignRoutes ========

// @desc    Get campaign performance metrics (placeholder)
// @route   GET /api/campaigns/:id/performance
exports.getCampaignPerformance = async (req, res) => {
    return res.status(200).json({
        success: true,
        message: 'Performance endpoint not yet implemented',
        data: {}
    });
};

// @desc    Bulk delete campaigns
// @route   DELETE /api/campaigns/bulk/delete
exports.bulkDeleteCampaigns = async (req, res) => {
    try {
        const { campaignIds } = req.body;

        if (!Array.isArray(campaignIds) || campaignIds.length === 0) {
            return res.status(400).json({ success: false, message: 'campaignIds array required' });
        }

        const result = await Campaign.deleteMany({ _id: { $in: campaignIds } });

        res.status(200).json({
            success: true,
            message: `Deleted ${result.deletedCount} campaigns`
        });
    } catch (error) {
        console.error('Error bulk deleting campaigns:', error);
        res.status(500).json({ success: false, message: 'Error bulk deleting campaigns' });
    }
};

// @desc    Bulk update campaign status
// @route   PUT /api/campaigns/bulk/status
exports.bulkUpdateStatus = async (req, res) => {
    try {
        const { campaignIds, status } = req.body;

        if (!Array.isArray(campaignIds) || campaignIds.length === 0 || !status) {
            return res.status(400).json({ success: false, message: 'campaignIds array and status are required' });
        }

        const result = await Campaign.updateMany(
            { _id: { $in: campaignIds } },
            { status, updatedBy: req.user?.id }
        );

        res.status(200).json({
            success: true,
            message: `Updated status for ${result.modifiedCount} campaigns`
        });
    } catch (error) {
        console.error('Error bulk updating status:', error);
        res.status(500).json({ success: false, message: 'Error bulk updating status' });
    }
};

// @desc    Sync a single campaign with Meta Ads (placeholder)
// @route   POST /api/campaigns/meta/sync/:id
exports.syncWithMeta = async (req, res) => {
    return res.status(200).json({
        success: true,
        message: 'Meta sync not yet implemented'
    });
};

// @desc    Sync all campaigns with Meta Ads (placeholder)
// @route   POST /api/campaigns/meta/sync-all
exports.syncAllWithMeta = async (_req, res) => {
    return res.status(200).json({
        success: true,
        message: 'Meta sync-all not yet implemented'
    });
};

// @desc    Create campaign from template (placeholder)
// @route   POST /api/campaigns/from-template
exports.createFromTemplate = async (req, res) => {
    return res.status(200).json({
        success: true,
        message: 'Create from template not yet implemented',
        templateId: req.body.templateId
    });
};

// @desc    Save campaign as template (placeholder)
// @route   POST /api/campaigns/:id/save-as-template
exports.saveAsTemplate = async (req, res) => {
    return res.status(200).json({
        success: true,
        message: 'Save as template not yet implemented',
        campaignId: req.params.id
    });
};
