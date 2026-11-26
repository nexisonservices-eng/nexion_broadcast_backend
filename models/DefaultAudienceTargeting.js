const mongoose = require('mongoose');

const profileSchema = new mongoose.Schema({
    name: { type: String, required: true, trim: true, maxlength: 100 },
    locations: { type: [String], default: [] },
    interests: { type: [String], default: [] },
    behaviors: { type: [String], default: [] }
}, { _id: true });

const schema = new mongoose.Schema({
    // This is the authenticated admin service user ID, not a local User reference.
    userId: { type: String, required: true, unique: true, immutable: true },
    locations: { type: [String], default: [] },
    interests: { type: [String], default: [] },
    behaviors: { type: [String], default: [] },
    profiles: { type: [profileSchema], default: [] }
}, { timestamps: true });

module.exports = mongoose.model('DefaultAudienceTargeting', schema);
