const OWNER_CONTACT_INDEX = Object.freeze({
  companyId: 1,
  userId: 1,
  ownerId: 1,
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});

const OWNER_CONTACT_NAME_INDEX = Object.freeze({
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

const OWNER_CONTACT_PHONE_INDEX = Object.freeze({
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

const toCleanString = (value = '') => String(value || '').trim();

const buildCrmContactListHint = ({ ownerScopeId = '', searchPlan = {} } = {}) => {
  const normalizedOwnerScopeId = toCleanString(ownerScopeId);
  if (!normalizedOwnerScopeId) {
    return searchPlan?.hint || null;
  }

  if (searchPlan?.mode === 'digits') {
    return OWNER_CONTACT_PHONE_INDEX;
  }

  if (searchPlan?.mode === 'name') {
    return OWNER_CONTACT_NAME_INDEX;
  }

  return OWNER_CONTACT_INDEX;
};

module.exports = {
  OWNER_CONTACT_INDEX,
  OWNER_CONTACT_NAME_INDEX,
  OWNER_CONTACT_PHONE_INDEX,
  buildCrmContactListHint
};
