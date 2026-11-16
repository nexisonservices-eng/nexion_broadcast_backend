const mongoose = require('mongoose');

const schema = new mongoose.Schema({
    // This is the authenticated admin service user ID, not a local User reference.
    userId: { type: String, required: true, unique: true, immutable: true },
    locations: { type: [String], default: [] },
    interests: { type: [String], default: [] },
    behaviors: { type: [String], default: [] }
}, { timestamps: true });

module.exports = mongoose.model('DefaultAudienceTargeting', schema);
