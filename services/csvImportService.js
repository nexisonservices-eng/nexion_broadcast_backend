const fs = require("fs");
const readline = require("readline");
const Contact = require("../models/Contact");
const {
  buildPhoneCandidates,
} = require("./whatsappOutreach/conversationResolver");

const normalizePhoneDigits = (value = "") =>
  String(value || "").replace(/\D/g, "");

const normalizePhoneForImport = (value = "") => {
  const candidates = buildPhoneCandidates(value);
  return candidates[0] || "";
};

const isValidPhoneNumber = (value = "") => {
  const digits = normalizePhoneDigits(value);
  return digits.length >= 10 && digits.length <= 15;
};

const normalizeHeaderKey = (value = "") =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const getRowValue = (row = {}, aliases = []) => {
  if (!row || typeof row !== "object") return "";
  const normalizedLookup = new Map(
    Object.entries(row).map(([key, value]) => [normalizeHeaderKey(key), value]),
  );

  for (const alias of Array.isArray(aliases) ? aliases : []) {
    const raw = normalizedLookup.get(normalizeHeaderKey(alias));
    if (raw === undefined || raw === null) continue;
    const cleaned = typeof raw === "string" ? raw.trim() : String(raw).trim();
    if (!cleaned) continue;
    return cleaned;
  }

  return "";
};

const parseTags = (row = {}) => {
  const rawTags = getRowValue(row, [
    "tags",
    "tag",
    "contact tags",
    "contactTags",
  ]);
  if (!rawTags) return [];
  return String(rawTags)
    .split(",")
    .map((tag) => tag.trim())
    .filter(Boolean);
};

const normalizeImportedRow = (row = {}, rowNumber = null) => {
  const phone = normalizePhoneForImport(
    getRowValue(row, [
      "phone",
      "phone number",
      "mobile",
      "mobile number",
      "whatsapp number",
      "whatsapp",
    ]),
  );
  const name = getRowValue(row, [
    "name",
    "full name",
    "first name",
    "display name",
    "contact name",
  ]);
  const email = getRowValue(row, ["email", "email address"]);
  const sourceType =
    getRowValue(row, ["source type", "sourceType", "source"]) || "imported";
  return {
    phone,
    phoneDigits: normalizePhoneDigits(phone),
    name,
    email,
    sourceType,
    tags: parseTags(row),
    data: row,
    rowNumber,
  };
};

const buildContactUpsert = (
  contact = {},
  { userId, companyId, consentReferenceId = "", importJobId = "" } = {},
) => {
  const now = new Date();
  const phone = String(contact?.phone || "").trim();
  const phoneDigits =
    String(contact?.phoneDigits || "").trim() || normalizePhoneDigits(phone);
  const tags = Array.isArray(contact?.tags) ? contact.tags : [];
  const name = String(contact?.name || "").trim();
  const email = String(contact?.email || "").trim();
  const sourceType =
    String(contact?.sourceType || "imported").trim() || "imported";
  const upsertData = {
    userId,
    companyId: companyId || null,
    name,
    nameLower: name.toLowerCase(),
    phone,
    phoneDigits,
    email,
    tags,
    sourceType,
    source: "csv_import",
    isBlocked: false,
    lastContact: now,
    lastContactAt: now,
    whatsappOptInStatus: "opted_in",
    whatsappOptInAt: now,
    whatsappOptInSource: "csv_import",
    whatsappOptInScope: "marketing",
    whatsappOptInTextSnapshot: "Imported via CSV contact upload.",
    whatsappOptInProofType: "csv_file",
    whatsappOptInProofId:
      consentReferenceId || `csv-import-${Date.now().toString(36)}`,
    updatedAt: now,
  };

  return {
    updateOne: {
      filter: {
        userId,
        companyId: companyId || null,
        phoneDigits,
      },
      update: {
        $set: upsertData,
        $setOnInsert: {
          createdAt: now,
          importJobId: String(importJobId || "").trim() || null,
        },
      },
      upsert: true,
    },
  };
};

const bulkUpsertImportedContacts = async (rows = [], scope = {}) => {
  const operations = [];
  const normalizedRows = Array.isArray(rows) ? rows : [];
  const seenPhones = new Set();
  let skipped = 0;
  let duplicateCount = 0;

  for (const row of normalizedRows) {
    const normalized = normalizeImportedRow(
      row?.data || row,
      row?.rowNumber || null,
    );
    if (!normalized.phone) {
      skipped += 1;
      continue;
    }
    if (!isValidPhoneNumber(normalized.phone)) {
      skipped += 1;
      continue;
    }

    const dedupeKey = normalized.phoneDigits || normalized.phone;
    if (seenPhones.has(dedupeKey)) {
      skipped += 1;
      duplicateCount += 1;
      continue;
    }
    seenPhones.add(dedupeKey);
    operations.push(buildContactUpsert(normalized, scope));
  }

  if (!operations.length) {
    return {
      matchedCount: 0,
      modifiedCount: 0,
      upsertedCount: 0,
      insertedCount: 0,
      skipped,
      duplicateCount,
      success: 0,
    };
  }

  const result = await Contact.bulkWrite(operations, { ordered: false });
  const insertedCount = Number(result?.upsertedCount || 0);
  const modifiedCount = Number(result?.modifiedCount || 0);
  const matchedCount = Number(result?.matchedCount || 0);

  return {
    matchedCount,
    modifiedCount,
    upsertedCount: insertedCount,
    insertedCount,
    skipped,
    duplicateCount,
    success: insertedCount + modifiedCount,
  };
};

const countCsvDataRows = async (filePath) => {
  const stream = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let totalLines = 0;

  try {
    for await (const line of rl) {
      if (String(line || "").trim()) {
        totalLines += 1;
      }
    }
  } finally {
    rl.close();
    stream.destroy();
  }

  return Math.max(0, totalLines - 1);
};

const removeFileQuietly = async (filePath) => {
  if (!filePath) return;
  try {
    await fs.promises.unlink(filePath);
  } catch {
    // ignore cleanup failures
  }
};

const getEtaMs = (startedAtMs, processedRows, totalRows) => {
  if (!startedAtMs || !processedRows || !totalRows) return null;
  const elapsedMs = Math.max(1, Date.now() - startedAtMs);
  const ratePerMs = processedRows / elapsedMs;
  if (!Number.isFinite(ratePerMs) || ratePerMs <= 0) return null;
  return Math.max(0, Math.round((totalRows - processedRows) / ratePerMs));
};

module.exports = {
  normalizePhoneDigits,
  normalizePhoneForImport,
  isValidPhoneNumber,
  normalizeImportedRow,
  bulkUpsertImportedContacts,
  countCsvDataRows,
  removeFileQuietly,
  getEtaMs,
};
