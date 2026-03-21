// controllers/campaignController.js
const Campaign = require('../models/campaign');
// Ensure User model is registered for population
require('../models/User');
const APIFeatures = require('../utils/apifeature');
const { validationResult } = require('express-validator');
const metaAdsService = require('../services/metaAdsService');
const { uploadCampaignCreative } = require('../utils/cloudinaryUpload');

const buildMetaCreateErrorMessage = (metaError) => {
    const details = metaError?.details?.error || {};
    const title = String(details.error_user_title || '').trim();
    const userMessage = String(details.error_user_msg || '').trim();
    const stage = String(metaError?.stage || '').trim();
    const errorCode = Number(details.code || 0);
    const errorSubcode = Number(details.error_subcode || 0);
    const rawMessage = String(details.message || metaError?.message || '').trim();

    if (stage === 'Creative creation' && /invalid page id/i.test(title || metaError?.message || '')) {
        const parts = [
            'Image upload worked, but Meta rejected the ad creative because the selected Facebook Page is invalid or not accessible for this admin login.'
        ];

        if (userMessage) {
            parts.push(userMessage);
        }

        parts.push('Reconnect Meta with the correct admin account, select a valid Facebook Page in Meta Connect, and then try publishing again.');
        return parts.join(' ');
    }

    if (
        stage === 'Creative creation' &&
        (
            /no permission to access this profile/i.test(title) ||
            /required permission to access this profile/i.test(userMessage) ||
            /application does not have permission for this action/i.test(rawMessage) ||
            (errorCode === 10 && errorSubcode === 1341012)
        )
    ) {
        const parts = [
            'Meta rejected the ad creative because this admin login does not have permission to use the selected Facebook Page profile.'
        ];

        parts.push(
            'Use a Facebook login that is an admin/editor of that Page, reconnect Meta, approve page permissions, and then try again.'
        );

        return parts.join(' ');
    }

    if (
        stage === 'Ad creation' &&
        (
            /please authenticate your account/i.test(title) ||
            /pending action/i.test(rawMessage) ||
            (errorCode === 31 && errorSubcode === 3858385)
        )
    ) {
        return 'Meta blocked new ad creation because this ad account has a pending security/authentication action. Open Ads Manager with the same account, complete the account authentication prompt, and then publish again.';
    }

    if (userMessage) {
        return `${metaError.message || 'Meta campaign creation failed'}. ${userMessage}`;
    }

    return metaError?.message || 'Meta campaign creation failed';
};

const sendMetaError = (res, metaError, fallbackMessage) =>
  res.status(metaError.status || 400).json({
    success: false,
    message: metaError.message || fallbackMessage,
    details: metaError.details || null,
    metaStage: metaError.stage || null
  });

const serializeCampaignRecord = (campaign) => {
    const source =
        typeof campaign?.toObject === 'function'
            ? campaign.toObject({ virtuals: true })
            : { ...(campaign || {}) };

    const createdByValue =
        source?.createdBy && typeof source.createdBy === 'object' && source.createdBy !== null
            ? source.createdBy._id || source.createdBy.id || source.createdBy
            : source?.createdBy;
    const updatedByValue =
        source?.updatedBy && typeof source.updatedBy === 'object' && source.updatedBy !== null
            ? source.updatedBy._id || source.updatedBy.id || source.updatedBy
            : source?.updatedBy;

    return {
        ...source,
        createdById: createdByValue ? String(createdByValue) : '',
        updatedById: updatedByValue ? String(updatedByValue) : '',
        createdBy:
            source?.createdBy && typeof source.createdBy === 'object' && source.createdBy !== null
                ? source.createdBy
                : (createdByValue ? String(createdByValue) : null),
        updatedBy:
            source?.updatedBy && typeof source.updatedBy === 'object' && source.updatedBy !== null
                ? source.updatedBy
                : (updatedByValue ? String(updatedByValue) : null)
    };
};

const buildCampaignStats = (campaigns = []) =>
    campaigns.reduce((acc, campaign) => {
        acc.totalSpent += Number(campaign?.spent || 0);
        acc.totalRevenue += Number(campaign?.revenue || 0);
        acc.totalImpressions += Number(campaign?.impressions || 0);
        acc.totalClicks += Number(campaign?.clicks || 0);
        acc.avgCtrSource.push(Number(campaign?.ctr || 0));
        acc.avgCpcSource.push(Number(campaign?.cpc || 0));
        return acc;
    }, {
        totalSpent: 0,
        totalRevenue: 0,
        totalImpressions: 0,
        totalClicks: 0,
        avgCtrSource: [],
        avgCpcSource: []
    });

const ensureCampaignOwnership = (campaign, req, res, actionMessage) => {
    if (!campaign) {
        res.status(404).json({
            success: false,
            message: 'Campaign not found'
        });
        return false;
    }

    if (req.user.role !== 'superadmin' && campaign.createdBy.toString() !== req.user.id) {
        res.status(403).json({
            success: false,
            message: actionMessage
        });
        return false;
    }

    return true;
};

const normalizeLifecycleState = ({ requestedStatus, existingCampaign } = {}) => {
    const wantsActiveLaunch = String(requestedStatus || '').toLowerCase() === 'active';
    if (!wantsActiveLaunch) {
        return {
            status: 'draft',
            lifecycleStatus:
                existingCampaign?.lifecycleStatus === 'pending_payment' ||
                existingCampaign?.lifecycleStatus === 'payment_verified' ||
                existingCampaign?.lifecycleStatus === 'pending_review' ||
                existingCampaign?.lifecycleStatus === 'approved'
                    ? 'draft'
                    : (existingCampaign?.lifecycleStatus || 'draft'),
            paymentStatus: existingCampaign?.paymentStatus || 'verified',
            reviewStatus: existingCampaign?.reviewStatus || 'approved',
            deliveryStatus: existingCampaign?.deliveryStatus || 'not_published'
        };
    }

    return {
        status: 'draft',
        lifecycleStatus: 'approved',
        paymentStatus: 'verified',
        reviewStatus: 'approved',
        deliveryStatus: 'not_published'
    };
};

// @desc    Get all campaigns
// @route   GET /api/campaigns
// @access  Private
exports.getCampaigns = async (req, res) => {
    try {
        // Build query
        const query = Campaign.find();
        const ownershipFilter = {};
        
        // Add user filter (users can only see their own campaigns)
        if (req.user.role !== 'superadmin') {
            query.where('createdBy').equals(req.user.id);
            ownershipFilter.createdBy = req.user.id;
        }
        
        // Apply filters, sorting, pagination
        const features = new APIFeatures(query, req.query)
            .filter()
            .sort()
            .limitFields()
            .paginate()
            .search(['name', 'objective']);

        // Execute query
        const campaigns = await features.query;

        const localCampaigns = campaigns.map((campaign) => serializeCampaignRecord(campaign));

        // Get total count for pagination
        const combinedFilter = {
            ...ownershipFilter,
            ...(features.filterConditions || {})
        };
        const totalCount = await Campaign.countDocuments(combinedFilter);
        let remoteCampaigns = [];
        try {
            remoteCampaigns = await metaAdsService.fetchRemoteCampaigns({
                userId: req.user.id,
                filters: req.query
            });
        } catch (remoteError) {
            console.warn('Unable to fetch remote Meta campaigns:', remoteError.message || remoteError);
        }

        const existingMetaCampaignIds = new Set(
            localCampaigns.map((campaign) => String(campaign.metaCampaignId || '').trim()).filter(Boolean)
        );
        const remoteOnlyCampaigns = remoteCampaigns.filter(
            (campaign) => !existingMetaCampaignIds.has(String(campaign.metaCampaignId || '').trim())
        );
        const mergedCampaigns = [...localCampaigns, ...remoteOnlyCampaigns];
        const mergedStats = buildCampaignStats(mergedCampaigns);
        const avgCtrSource = mergedStats.avgCtrSource.filter((value) => Number.isFinite(value));
        const avgCpcSource = mergedStats.avgCpcSource.filter((value) => Number.isFinite(value));

        res.status(200).json({
            success: true,
            count: mergedCampaigns.length,
            total: totalCount + remoteOnlyCampaigns.length,
            pagination: {
                page: parseInt(req.query.page) || 1,
                limit: parseInt(req.query.limit) || 10,
                total: totalCount + remoteOnlyCampaigns.length,
                pages: Math.ceil((totalCount + remoteOnlyCampaigns.length) / (parseInt(req.query.limit) || 10))
            },
            stats: {
                totalSpent: mergedStats.totalSpent,
                totalRevenue: mergedStats.totalRevenue,
                totalImpressions: mergedStats.totalImpressions,
                totalClicks: mergedStats.totalClicks,
                avgCTR: avgCtrSource.length
                    ? avgCtrSource.reduce((sum, value) => sum + value, 0) / avgCtrSource.length
                    : 0,
                avgCPC: avgCpcSource.length
                    ? avgCpcSource.reduce((sum, value) => sum + value, 0) / avgCpcSource.length
                    : 0
            },
            data: mergedCampaigns
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
        const campaign = await Campaign.findById(req.params.id);

        if (!campaign) {
            return res.status(404).json({
                success: false,
                message: 'Campaign not found'
            });
        }

        // Check authorization
        const createdById = campaign.createdBy?._id ? campaign.createdBy._id.toString() : String(campaign.createdBy || '');
        if (req.user.role !== 'superadmin' && createdById !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'Not authorized to view this campaign'
            });
        }

        res.status(200).json({
            success: true,
            data: serializeCampaignRecord(campaign)
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

        if (req.file?.buffer) {
            req.body.imageUrl = await uploadCampaignCreative(req.file, {
                folder: process.env.CLOUDINARY_FOLDER || 'meta-ads'
            });
        }

        const wantsActiveLaunch = String(req.body.status || '').toLowerCase() === 'active';
        Object.assign(req.body, normalizeLifecycleState({ requestedStatus: req.body.status }));

        // Create campaign
        const campaign = await Campaign.create(req.body);

        res.status(201).json({
            success: true,
            message: wantsActiveLaunch
                ? 'Campaign created and is ready to publish.'
                : 'Campaign draft created successfully.',
            data: serializeCampaignRecord(campaign)
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

        if (req.file?.buffer) {
            req.body.imageUrl = await uploadCampaignCreative(req.file, {
                folder: process.env.CLOUDINARY_FOLDER || 'meta-ads'
            });
        }

        const nextName = req.body.name || campaign.name;
        const nextStatus = req.body.status || campaign.status;
        const isPublished = Boolean(campaign.metaCampaignId);

        if (isPublished) {
            try {
                const metaUpdate = await metaAdsService.updateCampaign({
                    userId: req.user.id,
                    campaignId: campaign.metaCampaignId,
                    name: nextName,
                    status: ['active', 'paused'].includes(String(nextStatus || '').toLowerCase())
                        ? String(nextStatus).toUpperCase()
                        : undefined
                });
                req.body.metaResponse = metaUpdate;
            } catch (metaError) {
                return sendMetaError(res, metaError, 'Meta campaign update failed');
            }
        }

        if (!isPublished) {
            Object.assign(req.body, normalizeLifecycleState({ requestedStatus: nextStatus, existingCampaign: campaign }));
        }

        // Update campaign
        campaign = await Campaign.findByIdAndUpdate(
            req.params.id,
            req.body,
            {
                new: true,
                runValidators: true
            }
        );

        res.status(200).json({
            success: true,
            message: 'Campaign updated successfully',
            data: serializeCampaignRecord(campaign)
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
        const requestedId = String(req.params.id || '').trim();
        let campaign = null;

        if (!requestedId.startsWith('meta_')) {
            campaign = await Campaign.findById(req.params.id);
        }

        if (!campaign && req.body?.metaCampaignId) {
            try {
                const metaDeletion = await metaAdsService.archiveMetaCrudAssets({
                    userId: req.user.id,
                    campaignId: req.body.metaCampaignId,
                    adSetId: req.body.metaAdSetId,
                    adId: req.body.metaAdId
                });

                return res.status(200).json({
                    success: true,
                    message: 'Meta campaign archived successfully',
                    meta: metaDeletion || null
                });
            } catch (metaError) {
                return sendMetaError(
                    res,
                    metaError,
                    'Meta campaign deletion failed.'
                );
            }
        }

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

        let metaDeletion = null;
        if (campaign.metaCampaignId || campaign.metaAdSetId || campaign.metaAdId) {
            try {
                metaDeletion = await metaAdsService.archiveMetaCrudAssets({
                    userId: req.user.id,
                    campaignId: campaign.metaCampaignId,
                    adSetId: campaign.metaAdSetId,
                    adId: campaign.metaAdId
                });
            } catch (metaError) {
                return sendMetaError(
                    res,
                    metaError,
                    'Meta campaign deletion failed. The local campaign was not removed.'
                );
            }
        }

        await campaign.deleteOne();

        res.status(200).json({
            success: true,
            message: metaDeletion?.archived?.length
                ? 'Campaign deleted locally and archived in Meta successfully'
                : 'Campaign deleted successfully',
            meta: metaDeletion || null
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

        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to pause this campaign')) return;

        if (campaign.metaCampaignId) {
            try {
                const metaUpdate = await metaAdsService.pauseCampaign({
                    userId: req.user.id,
                    campaignId: campaign.metaCampaignId,
                    adSetId: campaign.metaAdSetId,
                    adId: campaign.metaAdId
                });
                campaign.metaResponse = metaUpdate;
            } catch (metaError) {
                return sendMetaError(res, metaError, 'Meta campaign pause failed');
            }
        }

        campaign.status = 'paused';
        campaign.lifecycleStatus = 'paused';
        campaign.deliveryStatus = campaign.metaCampaignId ? 'paused' : campaign.deliveryStatus;
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

        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to resume this campaign')) return;

        if (!campaign.metaCampaignId) {
            return res.status(400).json({
                success: false,
                message: 'Publish the campaign before running it.'
            });
        }

        if (campaign.metaCampaignId) {
            try {
                const metaUpdate = await metaAdsService.resumeCampaign({
                    userId: req.user.id,
                    campaignId: campaign.metaCampaignId,
                    adSetId: campaign.metaAdSetId,
                    adId: campaign.metaAdId
                });
                campaign.metaResponse = metaUpdate;
            } catch (metaError) {
                return sendMetaError(res, metaError, 'Meta campaign resume failed');
            }
        }

        campaign.status = 'active';
        campaign.lifecycleStatus = 'running';
        campaign.deliveryStatus = 'active';
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

        if (String(req.body?.name || '').trim()) {
            duplicateData.name = String(req.body.name).trim();
        }

        duplicateData.lifecycleStatus = 'draft';
        duplicateData.paymentStatus = 'verified';
        duplicateData.reviewStatus = 'approved';
        duplicateData.deliveryStatus = 'not_published';
        duplicateData.reviewNotes = '';
        duplicateData.paymentVerifiedAt = null;
        duplicateData.submittedForReviewAt = null;
        duplicateData.reviewedAt = null;
        duplicateData.publishedAt = null;

        const newCampaign = await Campaign.create(duplicateData);

        res.status(201).json({
            success: true,
            message: 'Campaign duplicated successfully',
            data: serializeCampaignRecord(newCampaign)
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

exports.verifyCampaignPayment = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);
        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to verify payment for this campaign')) return;

        campaign.paymentStatus = 'verified';
        campaign.paymentVerifiedAt = new Date();
        if (campaign.lifecycleStatus === 'pending_payment' || campaign.lifecycleStatus === 'draft') {
            campaign.lifecycleStatus = 'pending_review';
        }
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign billing state marked as ready.',
            data: campaign
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Error verifying campaign payment',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

exports.submitCampaignForReview = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);
        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to submit this campaign for review')) return;

        campaign.reviewStatus = 'pending_review';
        campaign.lifecycleStatus = 'pending_review';
        campaign.paymentStatus = 'verified';
        campaign.submittedForReviewAt = new Date();
        campaign.reviewNotes = String(req.body?.reviewNotes || campaign.reviewNotes || '').trim();
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign submitted for review.',
            data: campaign
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Error submitting campaign for review',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

exports.approveCampaignReview = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);
        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to approve this campaign')) return;

        if (campaign.reviewStatus !== 'pending_review') {
            return res.status(400).json({
                success: false,
                message: 'Only campaigns pending review can be approved.'
            });
        }

        campaign.reviewStatus = 'approved';
        campaign.lifecycleStatus = 'approved';
        campaign.reviewedAt = new Date();
        campaign.reviewNotes = String(req.body?.reviewNotes || campaign.reviewNotes || '').trim();
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign approved successfully.',
            data: campaign
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Error approving campaign review',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

exports.rejectCampaignReview = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);
        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to reject this campaign')) return;

        campaign.reviewStatus = 'rejected';
        campaign.lifecycleStatus = 'rejected';
        campaign.deliveryStatus = 'rejected';
        campaign.reviewedAt = new Date();
        campaign.reviewNotes = String(req.body?.reviewNotes || campaign.reviewNotes || '').trim();
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign review rejected.',
            data: campaign
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Error rejecting campaign review',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
};

exports.publishCampaign = async (req, res) => {
    try {
        const campaign = await Campaign.findById(req.params.id);
        if (!ensureCampaignOwnership(campaign, req, res, 'Not authorized to publish this campaign')) return;

        const metaSetup = await metaAdsService.getSetupBundle({ userId: req.user.id });
        if (!metaSetup?.pageId) {
            campaign.lifecycleStatus = 'draft';
            campaign.deliveryStatus = 'not_published';
            await campaign.save();

            return res.status(400).json({
                success: false,
                message: metaSetup?.setupError
                    ? `Meta page setup incomplete. ${metaSetup.setupError}`
                    : 'Meta page setup incomplete. Reconnect Meta and select a Facebook page before publishing.',
                details: {
                    setup: {
                        pageId: metaSetup?.pageId || '',
                        adAccountId: metaSetup?.adAccountId || '',
                        authSource: metaSetup?.authSource || metaSetup?.mode || '',
                        pagesAvailable: Array.isArray(metaSetup?.pages) ? metaSetup.pages.length : 0,
                        setupError: metaSetup?.setupError || ''
                    }
                },
                metaStage: 'Meta setup'
            });
        }

        campaign.lifecycleStatus = 'publishing';
        campaign.deliveryStatus = 'publishing';
        await campaign.save();

        if (!campaign.metaCampaignId) {
            let metaCampaign = null;
            try {
                metaCampaign = await metaAdsService.createMetaAdStackFromCrud({
                    userId: req.user.id,
                    campaignName: campaign.name,
                    objective: campaign.objective,
                    dailyBudget: campaign.dailyBudget || campaign.lifetimeBudget,
                    startDate: campaign.startDate,
                    endDate: campaign.endDate,
                    targeting: campaign.targeting,
                    ageMin: campaign.ageMin,
                    ageMax: campaign.ageMax,
                    gender: campaign.gender,
                    primaryText: campaign.primaryText,
                    headline: campaign.headline,
                    description: campaign.description,
                    destinationUrl: campaign.destinationUrl,
                    callToAction: campaign.callToAction,
                    optimizationGoal: campaign.optimizationGoal,
                    bidStrategy: campaign.bidStrategy,
                    imageUrl: campaign.imageUrl,
                    status: 'ACTIVE'
                });
            } catch (metaError) {
                campaign.lifecycleStatus = 'draft';
                campaign.deliveryStatus = 'not_published';
                await campaign.save();
                return res.status(metaError.status || 400).json({
                    success: false,
                    message: buildMetaCreateErrorMessage(metaError),
                    details: metaError.details || metaError.response?.data || null,
                    metaStage: metaError.stage || 'Meta publish',
                    rawError: process.env.NODE_ENV === 'development'
                        ? {
                            message: metaError.message || '',
                            stack: metaError.stack || ''
                        }
                        : undefined
                });
            }

            if (metaCampaign?.campaignId) {
                campaign.metaCampaignId = metaCampaign.campaignId;
                campaign.metaAdSetId = metaCampaign.adSetId;
                campaign.metaAdId = metaCampaign.adId;
                campaign.metaCreativeId = metaCampaign.creativeId;
                campaign.metaImageHash = metaCampaign.imageHash;
                campaign.metaResponse = metaCampaign;
            }
        }

        try {
            campaign.metaResponse = await metaAdsService.resumeCampaign({
                userId: req.user.id,
                campaignId: campaign.metaCampaignId,
                adSetId: campaign.metaAdSetId,
                adId: campaign.metaAdId
            });
        } catch (metaError) {
            campaign.lifecycleStatus = 'draft';
            campaign.deliveryStatus = 'paused';
            await campaign.save();
            return sendMetaError(res, metaError, 'Meta campaign publish failed');
        }

        campaign.status = 'active';
        campaign.lifecycleStatus = 'running';
        campaign.deliveryStatus = 'active';
        campaign.publishedAt = new Date();
        await campaign.save();

        res.status(200).json({
            success: true,
            message: 'Campaign published and running.',
            data: campaign
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Error publishing campaign',
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

        const campaigns = await Campaign.find({ _id: { $in: campaignIds } });

        if (req.user.role !== 'superadmin') {
            const unauthorized = campaigns.some(
                (campaign) => campaign.createdBy.toString() !== req.user.id
            );

            if (unauthorized) {
                return res.status(403).json({
                    success: false,
                    message: 'Not authorized to delete one or more campaigns'
                });
            }
        }

        const archivedMetaAssets = [];
        for (const campaign of campaigns) {
            if (!campaign.metaCampaignId && !campaign.metaAdSetId && !campaign.metaAdId) {
                continue;
            }

            try {
                const archiveResult = await metaAdsService.archiveMetaCrudAssets({
                    userId: req.user.id,
                    campaignId: campaign.metaCampaignId,
                    adSetId: campaign.metaAdSetId,
                    adId: campaign.metaAdId
                });

                archivedMetaAssets.push({
                    campaignId: String(campaign._id),
                    name: campaign.name,
                    archived: archiveResult?.archived || []
                });
            } catch (metaError) {
                return sendMetaError(
                    res,
                    metaError,
                    'Meta campaign deletion failed. No local campaigns were removed.'
                );
            }
        }

        const result = await Campaign.deleteMany({ _id: { $in: campaignIds } });

        res.status(200).json({
            success: true,
            message: `Deleted ${result.deletedCount} campaigns`,
            meta: archivedMetaAssets
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
