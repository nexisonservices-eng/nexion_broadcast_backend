const mongoose = require('mongoose');
const { MIN_SORT_DATE, normalizeDealSortDate } = require('../utils/dealPagination');

const toCleanString = (value = '') => String(value || '').trim();
const toLowerCleanString = (value = '') => toCleanString(value).toLowerCase();

const applyDerivedDealSearchFields = (doc = {}) => {
  doc.titleLower = toLowerCleanString(doc.title);
  doc.productNameLower = toLowerCleanString(doc.productName);
  doc.sourceLower = toLowerCleanString(doc.source);
  doc.lostReasonLower = toLowerCleanString(doc.lostReason);
  doc.expectedCloseAtSort = normalizeDealSortDate(doc.expectedCloseAt);
  return doc;
};

const applyDerivedDealSearchFieldsToUpdate = (update = {}) => {
  const nextUpdate = { ...update };
  const operatorKeys = Object.keys(nextUpdate).filter((key) => key.startsWith('$'));
  const isReplacementUpdate = operatorKeys.length === 0;

  if (isReplacementUpdate) {
    if (nextUpdate.title !== undefined) {
      nextUpdate.titleLower = toLowerCleanString(nextUpdate.title);
    }
    if (nextUpdate.productName !== undefined) {
      nextUpdate.productNameLower = toLowerCleanString(nextUpdate.productName);
    }
    if (nextUpdate.source !== undefined) {
      nextUpdate.sourceLower = toLowerCleanString(nextUpdate.source);
    }
    if (nextUpdate.lostReason !== undefined) {
      nextUpdate.lostReasonLower = toLowerCleanString(nextUpdate.lostReason);
    }
    nextUpdate.expectedCloseAtSort = normalizeDealSortDate(nextUpdate.expectedCloseAt);
    return nextUpdate;
  }

  const $set = { ...(nextUpdate.$set || {}) };
  const $unset = { ...(nextUpdate.$unset || {}) };

  const titleProvided =
    nextUpdate.title !== undefined || $set.title !== undefined || $unset.title !== undefined;
  const productNameProvided =
    nextUpdate.productName !== undefined ||
    $set.productName !== undefined ||
    $unset.productName !== undefined;
  const sourceProvided =
    nextUpdate.source !== undefined || $set.source !== undefined || $unset.source !== undefined;
  const lostReasonProvided =
    nextUpdate.lostReason !== undefined ||
    $set.lostReason !== undefined ||
    $unset.lostReason !== undefined;
  const expectedCloseAtProvided =
    nextUpdate.expectedCloseAt !== undefined ||
    $set.expectedCloseAt !== undefined ||
    $unset.expectedCloseAt !== undefined;

  if (nextUpdate.title !== undefined && $set.title === undefined) {
    $set.title = nextUpdate.title;
    delete nextUpdate.title;
  }
  if (nextUpdate.productName !== undefined && $set.productName === undefined) {
    $set.productName = nextUpdate.productName;
    delete nextUpdate.productName;
  }
  if (nextUpdate.source !== undefined && $set.source === undefined) {
    $set.source = nextUpdate.source;
    delete nextUpdate.source;
  }
  if (nextUpdate.lostReason !== undefined && $set.lostReason === undefined) {
    $set.lostReason = nextUpdate.lostReason;
    delete nextUpdate.lostReason;
  }
  if (nextUpdate.expectedCloseAt !== undefined && $set.expectedCloseAt === undefined) {
    $set.expectedCloseAt = nextUpdate.expectedCloseAt;
    delete nextUpdate.expectedCloseAt;
  }

  if (titleProvided) {
    $set.titleLower = $unset.title !== undefined ? '' : toLowerCleanString($set.title ?? '');
  }
  if (productNameProvided) {
    $set.productNameLower =
      $unset.productName !== undefined ? '' : toLowerCleanString($set.productName ?? '');
  }
  if (sourceProvided) {
    $set.sourceLower = $unset.source !== undefined ? '' : toLowerCleanString($set.source ?? '');
  }
  if (lostReasonProvided) {
    $set.lostReasonLower =
      $unset.lostReason !== undefined ? '' : toLowerCleanString($set.lostReason ?? '');
  }
  if (expectedCloseAtProvided) {
    $set.expectedCloseAtSort =
      $unset.expectedCloseAt !== undefined
        ? new Date(MIN_SORT_DATE.getTime())
        : normalizeDealSortDate($set.expectedCloseAt ?? null);
  }

  nextUpdate.$set = $set;
  if (Object.keys($unset).length) {
    nextUpdate.$unset = $unset;
  } else {
    delete nextUpdate.$unset;
  }

  return nextUpdate;
};

const DealSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'company', index: true },
    contactId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contact', required: true, index: true },
    conversationId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Conversation',
      default: null,
      index: true
    },
    title: { type: String, required: true, trim: true, maxlength: 200 },
    titleLower: { type: String, default: '', index: true },
    stage: {
      type: String,
      enum: ['discovery', 'qualified', 'proposal', 'negotiation', 'won', 'lost'],
      default: 'discovery',
      index: true
    },
    status: {
      type: String,
      enum: ['open', 'won', 'lost'],
      default: 'open',
      index: true
    },
    value: { type: Number, default: 0, min: 0 },
    probability: { type: Number, default: 0, min: 0, max: 100 },
    currency: { type: String, default: 'INR', trim: true, maxlength: 12 },
    expectedCloseAt: { type: Date, default: null, index: true },
    expectedCloseAtSort: {
      type: Date,
      default: () => new Date(MIN_SORT_DATE.getTime()),
      index: true
    },
    ownerId: { type: String, default: null, index: true },
    productName: { type: String, default: '', trim: true, maxlength: 200 },
    productNameLower: { type: String, default: '', index: true },
    source: { type: String, default: '', trim: true, maxlength: 200 },
    sourceLower: { type: String, default: '', index: true },
    notes: { type: String, default: '', trim: true, maxlength: 4000 },
    lostReason: { type: String, default: '', trim: true, maxlength: 1000 },
    lostReasonLower: { type: String, default: '', index: true },
    wonAt: { type: Date, default: null },
    lostAt: { type: Date, default: null },
    createdBy: { type: String, default: null },
    updatedBy: { type: String, default: null }
  },
  {
    timestamps: true
  }
);

DealSchema.pre('save', function(next) {
  applyDerivedDealSearchFields(this);
  next();
});

DealSchema.pre(['findOneAndUpdate', 'updateOne', 'updateMany', 'replaceOne'], function(next) {
  const update = this.getUpdate() || {};
  this.setUpdate(applyDerivedDealSearchFieldsToUpdate(update));
  next();
});

DealSchema.pre('insertMany', function(next, docs) {
  (Array.isArray(docs) ? docs : []).forEach((doc) => {
    if (!doc) return;
    applyDerivedDealSearchFields(doc);
  });
  next();
});

DealSchema.index({ companyId: 1, userId: 1, stage: 1, status: 1, expectedCloseAt: 1 });
DealSchema.index({ companyId: 1, userId: 1, contactId: 1, updatedAt: -1 });
DealSchema.index({ companyId: 1, userId: 1, ownerId: 1, status: 1, expectedCloseAt: 1 });
DealSchema.index({
  companyId: 1,
  userId: 1,
  stage: 1,
  status: 1,
  expectedCloseAtSort: 1,
  updatedAt: -1,
  createdAt: -1,
  _id: -1
});
DealSchema.index({
  companyId: 1,
  userId: 1,
  ownerId: 1,
  status: 1,
  expectedCloseAtSort: 1,
  updatedAt: -1,
  createdAt: -1,
  _id: -1
});
DealSchema.index({ companyId: 1, userId: 1, titleLower: 1, expectedCloseAtSort: 1, updatedAt: -1, createdAt: -1, _id: -1 });
DealSchema.index({
  companyId: 1,
  userId: 1,
  productNameLower: 1,
  expectedCloseAtSort: 1,
  updatedAt: -1,
  createdAt: -1,
  _id: -1
});
DealSchema.index({ companyId: 1, userId: 1, sourceLower: 1, expectedCloseAtSort: 1, updatedAt: -1, createdAt: -1, _id: -1 });
DealSchema.index({
  companyId: 1,
  userId: 1,
  lostReasonLower: 1,
  expectedCloseAtSort: 1,
  updatedAt: -1,
  createdAt: -1,
  _id: -1
});
DealSchema.index({ companyId: 1, userId: 1, expectedCloseAtSort: 1, updatedAt: -1, createdAt: -1, _id: -1 });

module.exports = mongoose.model('Deal', DealSchema);
