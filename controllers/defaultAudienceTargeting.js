const DefaultAudienceTargeting = require('../models/DefaultAudienceTargeting');

const fields = ['locations', 'interests', 'behaviors'];

const normalizeDefaults = (body) => {
    if (!body || typeof body !== 'object' || Array.isArray(body)
        || Object.keys(body).some((key) => !fields.includes(key))) {
        throw new Error('Only locations, interests and behaviors may be saved.');
    }
    return Object.fromEntries(fields.map((field) => {
        const values = body[field];
        if (!Array.isArray(values) || values.length > 200
            || values.some((value) => typeof value !== 'string' || /[\n\r,]/.test(value))) {
            throw new Error(`${field} must contain up to 200 individual text values.`);
        }
        const seen = new Set();
        const terms = values.map((value) => value.trim()).filter((value) => {
            const key = value.toLowerCase();
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
        });
        // Match the existing campaign payload/model limit without changing Meta integration.
        if (terms.join(', ').length > 500) throw new Error(`${field} cannot exceed 500 characters.`);
        return [field, terms];
    }));
};

exports.getDefaults = async (req, res) => {
    if (!req.user?.id) return res.status(401).json({ success: false, message: 'Authentication required.' });
    try {
        const saved = await DefaultAudienceTargeting.findOne({ userId: req.user.id }).lean();
        const data = Object.fromEntries(fields.map((field) => [field, saved?.[field] || []]));
        return res.json({ success: true, eligible: true, data });
    } catch (error) {
        return res.status(500).json({ success: false, message: 'Unable to load default audience targeting.' });
    }
};

exports.saveDefaults = async (req, res) => {
    if (!req.user?.id) return res.status(401).json({ success: false, message: 'Authentication required.' });
    let data;
    try {
        data = normalizeDefaults(req.body);
    } catch (error) {
        return res.status(400).json({ success: false, message: error.message });
    }
    try {
        await DefaultAudienceTargeting.findOneAndUpdate(
            { userId: req.user.id },
            { $set: data, $setOnInsert: { userId: req.user.id } },
            { upsert: true, new: true, runValidators: true }
        );
        return res.json({ success: true, eligible: true, data });
    } catch (error) {
        return res.status(500).json({ success: false, message: 'Unable to save default audience targeting.' });
    }
};
