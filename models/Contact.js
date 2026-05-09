const mongoose = require('mongoose');

const toCleanString = (value = '') => String(value || '').trim();
const toLowerCleanString = (value = '') => toCleanString(value).toLowerCase();
const toDigitsString = (value = '') => toCleanString(value).replace(/\D/g, '');

const applyDerivedContactSearchFields = (doc = {}) => {
  doc.nameLower = toLowerCleanString(doc.name);
  doc.phoneDigits = toDigitsString(doc.phone);
  return doc;
};

const applyDerivedContactSearchFieldsToUpdate = (update = {}) => {
  const nextUpdate = { ...update };
  const operatorKeys = Object.keys(nextUpdate).filter((key) => key.startsWith('$'));
  const isReplacementUpdate = operatorKeys.length === 0;

  if (isReplacementUpdate) {
    if (nextUpdate.name !== undefined) {
      nextUpdate.nameLower = toLowerCleanString(nextUpdate.name);
    }
    if (nextUpdate.phone !== undefined) {
      nextUpdate.phoneDigits = toDigitsString(nextUpdate.phone);
    }
    return nextUpdate;
  }

  const $set = { ...(nextUpdate.$set || {}) };
  const $unset = { ...(nextUpdate.$unset || {}) };

  const nameProvided =
    nextUpdate.name !== undefined || $set.name !== undefined || $unset.name !== undefined;
  const phoneProvided =
    nextUpdate.phone !== undefined || $set.phone !== undefined || $unset.phone !== undefined;

  if (nextUpdate.name !== undefined && $set.name === undefined) {
    $set.name = nextUpdate.name;
    delete nextUpdate.name;
  }
  if (nextUpdate.phone !== undefined && $set.phone === undefined) {
    $set.phone = nextUpdate.phone;
    delete nextUpdate.phone;
  }

  if (nameProvided) {
    $set.nameLower = $unset.name !== undefined ? '' : toLowerCleanString($set.name ?? '');
  }
  if (phoneProvided) {
    $set.phoneDigits = $unset.phone !== undefined ? '' : toDigitsString($set.phone ?? '');
  }

  nextUpdate.$set = $set;
  if (Object.keys($unset).length) {
    nextUpdate.$unset = $unset;
  } else {
    delete nextUpdate.$unset;
  }

  return nextUpdate;
};

const ContactSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, default: null },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true, default: null },
    stage: { type: String, default: 'new', index: true },
    status: { type: String, default: 'new', index: true },
    lastStageChangedAt: { type: Date, default: null },
    name: { type: String, default: '' },
    nameLower: { type: String, default: '', index: true },
    phone: { type: String, required: true, index: true },
    phoneDigits: { type: String, default: '', index: true },
    email: String,
    tags: [{ type: String }],
    customFields: mongoose.Schema.Types.Mixed,
    notes: String,
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    lastContact: Date,
    isBlocked: { type: Boolean, default: false }
  },
  {
    strict: false
  }
);

ContactSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  applyDerivedContactSearchFields(this);
  next();
});

ContactSchema.pre(['findOneAndUpdate', 'updateOne', 'updateMany', 'replaceOne'], function(next) {
  const update = this.getUpdate() || {};
  this.setUpdate(applyDerivedContactSearchFieldsToUpdate(update));
  next();
});

ContactSchema.pre('insertMany', function(next, docs) {
  (Array.isArray(docs) ? docs : []).forEach((doc) => {
    if (!doc) return;
    applyDerivedContactSearchFields(doc);
    doc.updatedAt = Date.now();
  });
  next();
});

ContactSchema.index({ companyId: 1, userId: 1, phone: 1 });
ContactSchema.index({ companyId: 1, userId: 1, nameLower: 1, lastContact: -1, createdAt: -1, _id: -1 });
ContactSchema.index({ companyId: 1, userId: 1, phoneDigits: 1, lastContact: -1, createdAt: -1, _id: -1 });
ContactSchema.index({
  companyId: 1,
  userId: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});
ContactSchema.index({
  companyId: 1,
  userId: 1,
  ownerId: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});
ContactSchema.index({
  companyId: 1,
  userId: 1,
  nameLower: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});
ContactSchema.index({
  companyId: 1,
  userId: 1,
  ownerId: 1,
  nameLower: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});
ContactSchema.index({
  companyId: 1,
  userId: 1,
  phoneDigits: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});
ContactSchema.index({
  companyId: 1,
  userId: 1,
  ownerId: 1,
  phoneDigits: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});
ContactSchema.index({ companyId: 1, userId: 1, lastContact: -1, createdAt: -1, _id: -1 });
ContactSchema.index({ companyId: 1, userId: 1, lastContactAt: -1, createdAt: -1, _id: -1 });
ContactSchema.index({ companyId: 1, userId: 1, lastInboundMessageAt: -1, createdAt: -1, _id: -1 });
ContactSchema.index({ companyId: 1, userId: 1, whatsappOptInStatus: 1, sourceType: 1, lastContact: -1, createdAt: -1 });

module.exports = mongoose.model('Contact', ContactSchema);
