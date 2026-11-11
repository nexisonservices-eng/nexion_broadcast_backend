const buildMetaTemplateIdentity = ({ userId, companyId = null, businessAccountId, metaTemplateId, whatsappTemplateId }) => ({
  userId,
  companyId: companyId || null,
  businessAccountId: String(businessAccountId || '').trim(),
  whatsappTemplateId: String(metaTemplateId || whatsappTemplateId || '').trim(),
});

const upsertMetaTemplate = async (Template, identity, templateData, now = new Date()) => {
  const filter = buildMetaTemplateIdentity(identity);
  const existing = await Template.find(filter).sort({ usageCount: -1, createdAt: 1 }).lean();
  if (existing.length > 1) {
    await Template.deleteMany({ _id: { $in: existing.slice(1).map((template) => template._id) } });
  }
  const update = {
    $set: { ...templateData, usageCount: Number(existing[0]?.usageCount || templateData.usageCount || 0) },
    $setOnInsert: { createdAt: existing[0]?.createdAt || now },
  };
  try {
    return await Template.findOneAndUpdate(filter, update, {
      upsert: true,
      new: true,
      setDefaultsOnInsert: true,
    });
  } catch (error) {
    if (error?.code !== 11000) throw error;
    return Template.findOneAndUpdate(filter, { $set: update.$set }, { new: true });
  }
};

module.exports = { buildMetaTemplateIdentity, upsertMetaTemplate };
