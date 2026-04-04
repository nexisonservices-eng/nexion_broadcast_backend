// models/Campaign.js
const mongoose = require('mongoose');

const campaignSchema = new mongoose.Schema({
    name: {
        type: String,
        required: [true, 'Campaign name is required'],
        trim: true,
        maxlength: [100, 'Campaign name cannot exceed 100 characters']
    },
    platform: {
        type: String,
        required: [true, 'Platform is required'],
        enum: {
            values: ['facebook', 'instagram', 'both'],
            message: 'Platform must be facebook, instagram, or both'
        },
        default: 'both'
    },
    objective: {
        type: String,
        required: [true, 'Campaign objective is required'],
        enum: {
            values: ['awareness', 'traffic', 'engagement', 'leads', 'sales', 'catalog'],
            message: 'Invalid campaign objective'
        },
        default: 'awareness'
    },
    status: {
        type: String,
        enum: {
            values: ['draft', 'active', 'paused', 'ended', 'archived'],
            message: 'Invalid status'
        },
        default: 'draft'
    },
    lifecycleStatus: {
        type: String,
        enum: [
            'draft',
            'pending_payment',
            'payment_verified',
            'pending_review',
            'approved',
            'rejected',
            'publishing',
            'running',
            'paused',
            'completed',
            'archived'
        ],
        default: 'draft'
    },
    paymentStatus: {
        type: String,
        enum: ['pending', 'verified', 'failed'],
        default: 'pending'
    },
    reviewStatus: {
        type: String,
        enum: ['not_submitted', 'pending_review', 'approved', 'rejected'],
        default: 'not_submitted'
    },
    deliveryStatus: {
        type: String,
        enum: ['not_published', 'publishing', 'active', 'paused', 'rejected', 'completed'],
        default: 'not_published'
    },
    reviewNotes: {
        type: String,
        trim: true,
        maxlength: [2000, 'Review notes cannot exceed 2000 characters'],
        default: ''
    },
    paymentVerifiedAt: {
        type: Date,
        default: null
    },
    submittedForReviewAt: {
        type: Date,
        default: null
    },
    reviewedAt: {
        type: Date,
        default: null
    },
    publishedAt: {
        type: Date,
        default: null
    },
    dailyBudget: {
        type: Number,
        min: [1, 'Daily budget must be at least $1'],
        validate: {
            validator: function(value) {
                // If daily budget is set, lifetime budget should not be set
                return !value || !this.lifetimeBudget;
            },
            message: 'Cannot have both daily and lifetime budget'
        }
    },
    lifetimeBudget: {
        type: Number,
        min: [1, 'Lifetime budget must be at least $1']
    },
    startDate: {
        type: Date,
        required: [true, 'Start date is required']
    },
    endDate: {
        type: Date,
        validate: {
            validator: function(value) {
                if (!value) return true;

                // Support both document validation and query update validation.
                let startDate = this.startDate;
                if (this instanceof mongoose.Query) {
                    const update = this.getUpdate() || {};
                    startDate =
                        update.startDate ||
                        (update.$set && update.$set.startDate) ||
                        this.get('startDate') ||
                        startDate;
                }

                return !startDate || new Date(value) > new Date(startDate);
            },
            message: 'End date must be after start date'
        }
    },
    targeting: {
        type: String,
        trim: true,
        maxlength: [500, 'Targeting description cannot exceed 500 characters']
    },
    // Performance metrics
    spent: {
        type: Number,
        default: 0,
        min: 0
    },
    impressions: {
        type: Number,
        default: 0,
        min: 0
    },
    clicks: {
        type: Number,
        default: 0,
        min: 0
    },
    ctr: {
        type: Number,
        default: 0,
        min: 0,
        max: 100
    },
    cpc: {
        type: Number,
        default: 0,
        min: 0
    },
    revenue: {
        type: Number,
        default: 0,
        min: 0
    },
    // Meta/Facebook specific fields
    metaCampaignId: {
        type: String,
        sparse: true,
        unique: true
    },
    metaAdSetId: {
        type: String,
        sparse: true
    },
    metaAdId: {
        type: String,
        sparse: true
    },
    metaCreativeId: {
        type: String,
        sparse: true
    },
    metaImageHash: {
        type: String,
        trim: true
    },
    metaVideoId: {
        type: String,
        trim: true
    },
    ageMin: {
        type: Number,
        min: 13,
        max: 65,
        default: 18
    },
    ageMax: {
        type: Number,
        min: 13,
        max: 65,
        default: 65
    },
    gender: {
        type: String,
        enum: ['all', 'male', 'female'],
        default: 'all'
    },
    interests: {
        type: String,
        trim: true,
        maxlength: [500, 'Interests cannot exceed 500 characters']
    },
    behaviors: {
        type: String,
        trim: true,
        maxlength: [500, 'Behaviors cannot exceed 500 characters']
    },
    primaryText: {
        type: String,
        trim: true,
        maxlength: [5000, 'Primary text cannot exceed 5000 characters']
    },
    headline: {
        type: String,
        trim: true,
        maxlength: [255, 'Headline cannot exceed 255 characters']
    },
    description: {
        type: String,
        trim: true,
        maxlength: [500, 'Description cannot exceed 500 characters']
    },
    destinationUrl: {
        type: String,
        trim: true
    },
    imageUrl: {
        type: String,
        trim: true
    },
    videoUrl: {
        type: String,
        trim: true
    },
    mediaType: {
        type: String,
        enum: ['image', 'video'],
        default: 'image'
    },
    callToAction: {
        type: String,
        trim: true,
        default: 'LEARN_MORE'
    },
    optimizationGoal: {
        type: String,
        trim: true,
        default: 'LINK_CLICKS'
    },
    bidStrategy: {
        type: String,
        trim: true,
        default: 'LOWEST_COST_WITHOUT_CAP'
    },
    metaResponse: {
        type: mongoose.Schema.Types.Mixed
    },
    // User tracking
    createdBy: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User',
        required: true
    },
    updatedBy: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User'
    }
}, {
    timestamps: true,
    toJSON: { virtuals: true },
    toObject: { virtuals: true }
});

// Indexes for better query performance
campaignSchema.index({ status: 1, platform: 1 });
campaignSchema.index({ lifecycleStatus: 1, paymentStatus: 1, reviewStatus: 1, deliveryStatus: 1 });
campaignSchema.index({ createdBy: 1 });
campaignSchema.index({ startDate: 1, endDate: 1 });
campaignSchema.index({ name: 'text', objective: 'text' });

// Virtual for ROAS (Return on Ad Spend)
campaignSchema.virtual('roas').get(function() {
    if (this.spent > 0) {
        return (this.revenue / this.spent).toFixed(2);
    }
    return 0;
});

// Virtual for ROI percentage
campaignSchema.virtual('roi').get(function() {
    if (this.spent > 0) {
        return ((this.revenue - this.spent) / this.spent * 100).toFixed(1);
    }
    return 0;
});

// Virtual for budget type
campaignSchema.virtual('budgetType').get(function() {
    return this.dailyBudget ? 'daily' : 'lifetime';
});

// Virtual for formatted date range
campaignSchema.virtual('dateRange').get(function() {
    const start = this.startDate ? new Date(this.startDate).toLocaleDateString() : 'N/A';
    const end = this.endDate ? new Date(this.endDate).toLocaleDateString() : 'Ongoing';
    return `${start} - ${end}`;
});

// Pre-save middleware
campaignSchema.pre('save', function(next) {
    // Calculate CTR if impressions > 0
    if (this.impressions > 0 && this.clicks > 0) {
        this.ctr = (this.clicks / this.impressions * 100).toFixed(2);
    }
    
    // Calculate CPC if clicks > 0
    if (this.clicks > 0 && this.spent > 0) {
        this.cpc = (this.spent / this.clicks).toFixed(2);
    }
    
    next();
});

module.exports = mongoose.model('Campaign', campaignSchema);
