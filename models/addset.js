// models/AdSet.js
const mongoose = require('mongoose');

const adSetSchema = new mongoose.Schema({
    name: {
        type: String,
        required: [true, 'Ad set name is required'],
        trim: true
    },
    campaignId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Campaign',
        required: true
    },
    status: {
        type: String,
        enum: ['active', 'paused', 'draft', 'archived'],
        default: 'draft'
    },
    dailyBudget: {
        type: Number,
        min: 1
    },
    lifetimeBudget: {
        type: Number,
        min: 1
    },
    targeting: {
        locations: [String],
        ageMin: { type: Number, min: 13, default: 18 },
        ageMax: { type: Number, max: 65, default: 65 },
        genders: [String],
        interests: [String],
        behaviors: [String],
        customAudiences: [String]
    },
    placement: {
        platforms: [String],
        devices: [String],
        positions: [String]
    },
    schedule: {
        startDate: Date,
        endDate: Date,
        deliveryOptimization: String
    },
    performance: {
        impressions: { type: Number, default: 0 },
        clicks: { type: Number, default: 0 },
        spent: { type: Number, default: 0 },
        conversions: { type: Number, default: 0 }
    },
    metaAdSetId: String,
    createdBy: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User',
        required: true
    }
}, {
    timestamps: true
});

module.exports = mongoose.model('AdSet', adSetSchema);