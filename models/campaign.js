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
                // End date should be after start date if provided
                return !value || value > this.startDate;
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