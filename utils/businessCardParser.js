const toCleanString = (value = '') => String(value || '').trim();
const normalizeWhitespace = (value = '') => toCleanString(value).replace(/\s+/g, ' ');

const EMAIL_REGEX = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const PHONE_REGEX = /(?:\+?\d[\d\s().-]{7,}\d)/g;

const NAME_PREFIXES = new Set(['mr', 'mrs', 'ms', 'miss', 'dr', 'prof']);
const DESIGNATION_KEYWORDS = [
  'founder',
  'co-founder',
  'director',
  'manager',
  'head',
  'lead',
  'owner',
  'ceo',
  'cto',
  'cfo',
  'coo',
  'president',
  'vp',
  'vice president',
  'principal',
  'consultant',
  'specialist',
  'executive',
  'engineer',
  'developer',
  'architect',
  'designer',
  'sales',
  'marketing'
];
const COMPANY_KEYWORDS = [
  'inc',
  'llc',
  'ltd',
  'limited',
  'corp',
  'corporation',
  'company',
  'co.',
  'solutions',
  'systems',
  'technologies',
  'technology',
  'services',
  'consulting',
  'group',
  'studio',
  'labs',
  'digital',
  'ventures',
  'enterprises',
  'enterprise',
  'global',
  'partners',
  'media',
  'software'
];
const NON_NAME_KEYWORDS = [
  ...DESIGNATION_KEYWORDS,
  ...COMPANY_KEYWORDS,
  'mobile',
  'phone',
  'tel',
  'fax',
  'email',
  'web',
  'website',
  'www',
  'http',
  'https',
  'address',
  'office',
  'branch'
];

const splitLines = (text = '') =>
  String(text || '')
    .split(/\r?\n+/)
    .map((line) => normalizeWhitespace(line))
    .filter(Boolean);

const isLikelyPhoneLine = (line = '') => {
  const normalized = toCleanString(line).toLowerCase();
  return /(?:mobile|cell|phone|tel|whatsapp|m\s*[:.-]?|p\s*[:.-]?|mob)/.test(normalized);
};

const isLikelyDesignationLine = (line = '') => {
  const normalized = toCleanString(line).toLowerCase();
  return DESIGNATION_KEYWORDS.some((keyword) => normalized.includes(keyword));
};

const isLikelyCompanyLine = (line = '') => {
  const normalized = toCleanString(line).toLowerCase();
  return COMPANY_KEYWORDS.some((keyword) => normalized.includes(keyword));
};

const isLikelyNameLine = (line = '') => {
  const normalized = toCleanString(line);
  if (!normalized) return false;
  if (/[0-9@]/.test(normalized)) return false;
  const lower = normalized.toLowerCase();
  if (NON_NAME_KEYWORDS.some((keyword) => lower.includes(keyword))) return false;
  const words = normalized.split(/\s+/).filter(Boolean);
  if (words.length < 2 || words.length > 5) return false;
  return words.every((word) => /^(?:[A-Z][a-z.'-]*|[A-Z]{2,}|[A-Z]|&|and)$/.test(word));
};

const normalizePhoneCandidate = (candidate = '') => {
  const cleaned = String(candidate || '').replace(/[^\d+]/g, '');
  if (!cleaned) return '';
  const digits = cleaned.replace(/\D/g, '');
  if (digits.length < 10 || digits.length > 15) return '';
  return cleaned.startsWith('+') ? `+${digits}` : digits;
};

const extractEmails = (lines = [], text = '') => {
  const matches = new Set();
  const sources = [text, ...(Array.isArray(lines) ? lines : [])];
  for (const source of sources) {
    const raw = String(source || '');
    const found = raw.match(EMAIL_REGEX) || [];
    found.forEach((email) => matches.add(email.toLowerCase()));
  }
  return Array.from(matches);
};

const extractPhoneCandidates = (lines = [], text = '') => {
  const scoredCandidates = new Map();
  const sources = [
    ...((Array.isArray(lines) ? lines : []).map((line, index) => ({ line, index }))),
    ...String(text || '')
      .split(/\s+/)
      .map((token, index) => ({ line: token, index }))
  ];

  for (const { line, index } of sources) {
    const raw = String(line || '');
    const matches = raw.match(PHONE_REGEX) || [];
    for (const match of matches) {
      const normalized = normalizePhoneCandidate(match);
      if (!normalized) continue;
      const digits = normalized.replace(/\D/g, '');
      let score = digits.length;
      if (match.includes('+')) score += 3;
      if (isLikelyPhoneLine(raw)) score += 6;
      if (/[xX]\s*\d+/.test(raw)) score -= 2;
      if (/\b(fax|office)\b/i.test(raw)) score -= 1;
      if (index <= 2) score += 2;
      const previous = scoredCandidates.get(normalized);
      if (!previous || score > previous.score) {
        scoredCandidates.set(normalized, { value: normalized, score, source: raw });
      }
    }
  }

  return Array.from(scoredCandidates.values())
    .sort((a, b) => b.score - a.score)
    .map((entry) => entry.value);
};

const scoreLineForName = (line = '', index = 0) => {
  const normalized = normalizeWhitespace(line);
  if (!normalized || /[0-9@]/.test(normalized)) return -100;
  const lower = normalized.toLowerCase();
  if (NON_NAME_KEYWORDS.some((keyword) => lower.includes(keyword))) return -100;

  const words = normalized.split(/\s+/).filter(Boolean);
  let score = 0;

  if (words.length >= 2 && words.length <= 4) score += 5;
  if (index <= 2) score += 4;
  if (index <= 4) score += 2;
  if (/^(?:[A-Z][a-z.'-]+|[A-Z])(?:\s+(?:[A-Z][a-z.'-]+|[A-Z]))+$/.test(normalized)) score += 5;
  if (/^[A-Z][a-z]+$/.test(words[0] || '')) score += 1;
  if (NAME_PREFIXES.has((words[0] || '').replace(/\./g, '').toLowerCase())) score += 2;
  if (/^[A-Z\s.'-]+$/.test(normalized)) score += 1;
  if (normalized.length > 36) score -= 2;

  return score;
};

const scoreLineForDesignation = (line = '', index = 0) => {
  const normalized = normalizeWhitespace(line);
  if (!normalized) return -100;
  const lower = normalized.toLowerCase();
  if (/[0-9@]/.test(normalized)) return -20;
  let score = 0;
  if (DESIGNATION_KEYWORDS.some((keyword) => lower.includes(keyword))) score += 8;
  if (/\b(founder|director|manager|lead|head|engineer|developer|designer|consultant|specialist|executive|owner)\b/i.test(normalized)) {
    score += 4;
  }
  if (/\b(sales|marketing|operations|product|growth)\b/i.test(normalized)) score += 2;
  if (index >= 1 && index <= 4) score += 2;
  if (normalized.length <= 42) score += 1;
  return score;
};

const scoreLineForCompany = (line = '', index = 0) => {
  const normalized = normalizeWhitespace(line);
  if (!normalized) return -100;
  const lower = normalized.toLowerCase();
  if (/[0-9@]/.test(normalized)) return -20;
  let score = 0;
  if (isLikelyCompanyLine(normalized)) score += 8;
  if (/^[A-Z0-9\s&.'-]+$/.test(normalized) && normalized.length <= 50) score += 3;
  if (/\b(inc|llc|ltd|limited|corp|corporation|company|co\.|solutions|systems|technologies|technology|services|consulting|group|studio|labs|digital|ventures|enterprise|partners|media|software)\b/i.test(lower)) {
    score += 4;
  }
  if (index <= 3) score += 2;
  if (normalized.length >= 4 && normalized.length <= 48) score += 1;
  return score;
};

const pickBestLine = (lines = [], scorer = () => 0) => {
  let best = { value: '', score: -100 };
  (Array.isArray(lines) ? lines : []).forEach((line, index) => {
    const score = scorer(line, index);
    if (score > best.score) {
      best = { value: normalizeWhitespace(line), score };
    }
  });
  return best.value;
};

const extractBusinessCardFields = (text = '') => {
  const lines = splitLines(text);
  const normalizedText = normalizeWhitespace(text);
  const emails = extractEmails(lines, normalizedText);
  const phoneCandidates = extractPhoneCandidates(lines, normalizedText);

  const mobileNumber = phoneCandidates[0] || '';
  const email = emails[0] || '';

  const fullName = pickBestLine(lines, scoreLineForName);
  const designation = pickBestLine(lines, scoreLineForDesignation);
  const companyName = pickBestLine(lines, scoreLineForCompany);

  const inferenceSignals = {
    hasName: Boolean(fullName),
    hasPhone: Boolean(mobileNumber),
    hasEmail: Boolean(email),
    hasCompany: Boolean(companyName),
    hasDesignation: Boolean(designation)
  };

  return {
    fullName,
    mobileNumber,
    email,
    companyName,
    designation,
    lines,
    emails,
    phoneCandidates,
    inferenceSignals
  };
};

module.exports = {
  extractBusinessCardFields,
  extractEmails,
  extractPhoneCandidates,
  normalizePhoneCandidate,
  splitLines
};
