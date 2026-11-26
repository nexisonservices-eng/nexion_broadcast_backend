const DefaultAudienceTargeting = require('../models/DefaultAudienceTargeting');

const fields = ['locations', 'interests', 'behaviors'];

const normalizeDefaults = (body) => {
    const normalizeFields = (source) => Object.fromEntries(fields.map((field) => {
        const values = source[field];
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

    if (!body || typeof body !== 'object' || Array.isArray(body)) {
        throw new Error('Audience profiles are required.');
    }
    if (Array.isArray(body.profiles)) {
        if (body.profiles.length > 50) throw new Error('A maximum of 50 profiles may be saved.');
        const names = new Set();
        const profiles = body.profiles.map((profile) => {
            const name = String(profile?.name || '').trim();
            const key = name.toLowerCase();
            if (!name || name.length > 100 || names.has(key)) throw new Error('Profile names must be unique and non-empty.');
            names.add(key);
            return { ...(profile?._id ? { _id: profile._id } : {}), name, ...normalizeFields(profile || {}) };
        });
        const first = profiles[0] || { locations: [], interests: [], behaviors: [] };
        return { profiles, locations: first.locations, interests: first.interests, behaviors: first.behaviors };
    }
    if (Object.keys(body).some((key) => !fields.includes(key))) {
        throw new Error('Only locations, interests and behaviors may be saved.');
    }
    return { ...normalizeFields(body), profiles: [{ name: 'Default Audience', ...normalizeFields(body) }] };
};

const profileData = (saved) => {
    if (Array.isArray(saved?.profiles) && saved.profiles.length) return saved.profiles;
    const legacy = Object.fromEntries(fields.map((field) => [field, saved?.[field] || []]));
    const hasLegacyValues = fields.some((field) => legacy[field].length);
    return hasLegacyValues ? [{ name: 'Default Audience', ...legacy }] : [];
};

exports.getDefaults = async (req, res) => {
    if (!req.user?.id) return res.status(401).json({ success: false, message: 'Authentication required.' });
    try {
        const saved = await DefaultAudienceTargeting.findOne({ userId: req.user.id }).lean();
        const data = Object.fromEntries(fields.map((field) => [field, saved?.[field] || []]));
        return res.json({ success: true, eligible: true, data: { ...data, profiles: profileData(saved) } });
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
        const saved = await DefaultAudienceTargeting.findOneAndUpdate(
            { userId: req.user.id },
            { $set: data, $setOnInsert: { userId: req.user.id } },
            { upsert: true, new: true, runValidators: true }
        );
        const savedData = saved?.toObject ? saved.toObject() : saved;
        return res.json({ success: true, eligible: true, data: { ...data, profiles: savedData?.profiles || data.profiles } });
    } catch (error) {
        return res.status(500).json({ success: false, message: 'Unable to save default audience targeting.' });
    }
};
