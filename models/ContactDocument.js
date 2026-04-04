const mongoose = require('mongoose');

const CONTACT_DOCUMENT_TYPES = [
  'id_proof',
  'address_proof',
  'proposal',
  'quote',
  'invoice',
  'contract',
  'payment_receipt',
  'other'
];

const CONTACT_DOCUMENT_STATUSES = ['active', 'archived', 'deleted'];
const CONTACT_DOCUMENT_VERIFICATION_STATUSES = ['pending', 'approved', 'rejected', 'not_required'];

const ContactDocumentSchema = new mongoose.Schema(
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
    title: { type: String, default: '' },
    description: { type: String, default: '' },
    documentType: {
      type: String,
      enum: CONTACT_DOCUMENT_TYPES,
      default: 'other',
      index: true
    },
    status: {
      type: String,
      enum: CONTACT_DOCUMENT_STATUSES,
      default: 'active',
      index: true
    },
    verificationStatus: {
      type: String,
      enum: CONTACT_DOCUMENT_VERIFICATION_STATUSES,
      default: 'not_required',
      index: true
    },
    tags: [{ type: String }],
    attachment: {
      storageProvider: { type: String, default: 'cloudinary' },
      direction: { type: String, enum: ['sent', 'received'], default: 'sent' },
      username: String,
      folder: String,
      publicId: String,
      resourceType: { type: String, enum: ['image', 'video', 'raw', 'auto'], default: 'raw' },
      fileCategory: { type: String, enum: ['image', 'audio', 'document'], default: 'document' },
      mimeType: String,
      originalFileName: String,
      extension: String,
      bytes: { type: Number, default: 0 },
      width: { type: Number, default: null },
      height: { type: Number, default: null },
      pages: { type: Number, default: null },
      secureUrl: String,
      sender: String,
      recipient: String,
      uploadedAt: { type: Date, default: Date.now },
      deletedAt: { type: Date, default: null },
      deletedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null }
    },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
    createdBy: { type: String, default: null }
  },
  {
    timestamps: true
  }
);

ContactDocumentSchema.index({ companyId: 1, userId: 1, contactId: 1, createdAt: -1 });
ContactDocumentSchema.index({ companyId: 1, userId: 1, 'attachment.publicId': 1 });
ContactDocumentSchema.index({ companyId: 1, userId: 1, documentType: 1, status: 1, createdAt: -1 });

module.exports = {
  ContactDocument: mongoose.model('ContactDocument', ContactDocumentSchema),
  CONTACT_DOCUMENT_TYPES,
  CONTACT_DOCUMENT_STATUSES,
  CONTACT_DOCUMENT_VERIFICATION_STATUSES
};
