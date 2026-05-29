const express = require("express");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const auth = require("../middleware/auth");
const requirePlanFeature = require("../middleware/planGuard");
const requireWhatsAppCredentials = require("../middleware/requireWhatsAppCredentials");
const broadcastService = require("../services/broadcastService");
const {
  enqueueCsvImport,
  cancelCsvImportQueueJob,
  csvImportQueue,
} = require("../queues/csvImportQueue");
const CsvImportJob = require("../models/CsvImportJob");
const {
  processCsvImport,
  failCsvImport,
} = require("../services/csvImportProcessor");
const Contact = require("../models/Contact");
const {
  buildPhoneCandidates: buildPhoneCandidatesFromResolver,
  buildPhoneLookupFilters,
} = require("../services/whatsappOutreach/conversationResolver");
const {
  buildBroadcastAudienceValidation,
  toCleanString,
} = require("../services/whatsappOutreach/policy");
const { isRedisDisabled } = require("../config/redis");

const router = express.Router();
router.use(auth);

const CSV_IMPORT_STORAGE_DIR = path.join(process.cwd(), "tmp", "csv-imports");
fs.mkdirSync(CSV_IMPORT_STORAGE_DIR, { recursive: true });

const csvUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, CSV_IMPORT_STORAGE_DIR),
    filename: (_req, file, cb) => {
      const safeBase = String(file?.originalname || "csv-import").replace(
        /[^a-z0-9._-]+/gi,
        "_",
      );
      cb(
        null,
        `${Date.now()}-${Math.random().toString(16).slice(2)}-${safeBase}`,
      );
    },
  }),
  limits: {
    fileSize: Math.max(
      5 * 1024 * 1024,
      Number(process.env.CSV_IMPORT_MAX_FILE_SIZE_BYTES || 50 * 1024 * 1024),
    ),
  },
  fileFilter: (_req, file, cb) => {
    const name = String(file?.originalname || "")
      .trim()
      .toLowerCase();
    const mime = String(file?.mimetype || "")
      .trim()
      .toLowerCase();
    if (
      name.endsWith(".csv") ||
      mime === "text/csv" ||
      mime === "application/vnd.ms-excel"
    ) {
      cb(null, true);
      return;
    }
    cb(new Error("Only CSV files are supported"));
  },
});

const parseCsvUpload = (req, res) =>
  new Promise((resolve, reject) => {
    csvUpload.single("csv_file")(req, res, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });

const normalizeCsvImportJob = (job = {}) => ({
  id: String(job?._id || job?.id || "").trim(),
  _id: String(job?._id || job?.id || "").trim(),
  userId: String(job?.userId || "").trim(),
  companyId: job?.companyId || null,
  originalFileName: String(job?.originalFileName || "").trim(),
  storedFileName: String(job?.storedFileName || "").trim(),
  filePath: String(job?.filePath || "").trim(),
  queueJobId: String(job?.queueJobId || "").trim(),
  status: String(job?.status || "queued").trim(),
  totalRows: Number(job?.totalRows || 0),
  processedRows: Number(job?.processedRows || 0),
  successCount: Number(job?.successCount || 0),
  failedCount: Number(job?.failedCount || 0),
  duplicateCount: Number(job?.duplicateCount || 0),
  skippedCount: Number(job?.skippedCount || 0),
  currentStage: String(job?.currentStage || "queued").trim(),
  percentComplete: Number(job?.percentComplete || 0),
  etaMs: Number.isFinite(Number(job?.etaMs)) ? Number(job.etaMs) : null,
  errorMessage: String(job?.errorMessage || "").trim(),
  startedAt: job?.startedAt || null,
  completedAt: job?.completedAt || null,
  lastProgressAt: job?.lastProgressAt || null,
  createdAt: job?.createdAt || null,
  updatedAt: job?.updatedAt || null,
});

const assertCsvImportOwnership = async (req, importJobId) => {
  const job = await CsvImportJob.findOne({
    _id: importJobId,
    userId: req.user.id,
    ...(req.companyId ? { companyId: req.companyId } : {}),
  }).lean();

  return job || null;
};

const queueCsvImportProcessing = async ({
  importJobId,
  userId,
  companyId,
  filePath,
  originalFileName,
}) => {
  if (!importJobId || !userId || !filePath) {
    return { success: false, error: "Missing CSV import job inputs" };
  }

  if (!isRedisDisabled && csvImportQueue?.add) {
    const queueResult = await enqueueCsvImport({
      importJobId,
      userId,
      companyId,
      filePath,
      originalFileName,
    });
    if (queueResult?.success) {
      return queueResult;
    }
  }

  setImmediate(() => {
    processCsvImport({
      id: `local-${importJobId}`,
      data: {
        importJobId,
        userId,
        companyId,
        filePath,
        originalFileName,
      },
    }).catch(async (error) => {
      await failCsvImport({
        importJobId,
        userId,
        filePath,
        error,
      });
    });
  });

  return {
    success: true,
    data: {
      jobId: `local-${importJobId}`,
      fallback: true,
    },
  };
};

const buildPhoneCandidates =
  typeof buildPhoneCandidatesFromResolver === "function"
    ? buildPhoneCandidatesFromResolver
    : (value = "") => {
        const rawValue = String(value || "").trim();
        const normalizedPhone = rawValue.replace(/\D/g, "");

        return Array.from(
          new Set(
            [
              rawValue,
              normalizedPhone,
              normalizedPhone ? `+${normalizedPhone}` : "",
              normalizedPhone.length > 10 ? normalizedPhone.slice(-10) : "",
            ].filter(Boolean),
          ),
        );
      };

const buildScopedContactFilter = (req, extra = {}) => {
  const scopedConditions = [{ userId: req.user.id }];
  if (req.companyId) {
    scopedConditions.push({ companyId: req.companyId });
  }
  if (extra && Object.keys(extra).length > 0) {
    scopedConditions.push(extra);
  }

  return scopedConditions.length === 1
    ? scopedConditions[0]
    : { $and: scopedConditions };
};

const buildContactsByPhoneMap = async (req, recipients = []) => {
  try {
    const phoneFilters = Array.from(
      new Set(
        (Array.isArray(recipients) ? recipients : [])
          .map((recipient) => String(recipient?.phone || "").trim())
          .filter(Boolean),
      ),
    )
      .map((phone) => buildPhoneLookupFilters(phone))
      .filter(Boolean);

    if (!phoneFilters.length) {
      return new Map();
    }

    const contacts = await Contact.find(
      buildScopedContactFilter(
        req,
        phoneFilters.length === 1 ? phoneFilters[0] : { $or: phoneFilters },
      ),
    )
      .select(
        "_id phone isBlocked sourceType whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata whatsappOptOutAt serviceWindowClosesAt lastInboundMessageAt",
      )
      .lean();

    const contactsByPhone = new Map();
    for (const contact of contacts) {
      for (const candidate of buildPhoneCandidates(contact?.phone || "")) {
        if (!contactsByPhone.has(candidate)) {
          contactsByPhone.set(candidate, contact);
        }
      }
    }

    return contactsByPhone;
  } catch (error) {
    console.error(
      "[bulk] phone lookup failed, continuing without contact enrichment:",
      {
        userId: req?.user?.id || null,
        companyId: req?.companyId || null,
        error: error?.message || error,
      },
    );
    return new Map();
  }
};

router.post(
  "/imports",
  requirePlanFeature("broadcastMessaging"),
  async (req, res) => {
    try {
      await parseCsvUpload(req, res);
      const uploadedFile = req.file;
      if (!uploadedFile) {
        return res.status(400).json({
          success: false,
          error: "CSV file is required",
        });
      }

      const userId = String(req.user?.id || "").trim();
      const companyId = req.companyId || null;
      const originalFileName = String(uploadedFile.originalname || "").trim();
      const storedFileName = String(uploadedFile.filename || "").trim();
      const filePath = String(uploadedFile.path || "").trim();

      const importJob = await CsvImportJob.create({
        userId,
        companyId,
        originalFileName,
        storedFileName,
        filePath,
        status: "queued",
        currentStage: "queued",
        percentComplete: 0,
        processedRows: 0,
        successCount: 0,
        failedCount: 0,
        duplicateCount: 0,
        skippedCount: 0,
        errorMessage: "",
        lastProgressAt: new Date(),
      });

      const queueResult = await queueCsvImportProcessing({
        importJobId: String(importJob._id),
        userId,
        companyId,
        filePath,
        originalFileName,
      });

      await CsvImportJob.findByIdAndUpdate(importJob._id, {
        $set: {
          queueJobId: String(
            queueResult?.data?.jobId || queueResult?.data?.queueJobId || "",
          ),
          updatedAt: new Date(),
        },
      });

      const updatedImportJob = await CsvImportJob.findById(
        importJob._id,
      ).lean();
      return res.status(202).json({
        success: true,
        queued: true,
        data: {
          importJob: normalizeCsvImportJob(updatedImportJob || importJob),
          queueJobId: String(queueResult?.data?.jobId || "").trim(),
          fallback: Boolean(queueResult?.data?.fallback),
        },
      });
    } catch (error) {
      const filePath = String(req.file?.path || "").trim();
      if (filePath) {
        try {
          await fs.promises.unlink(filePath);
        } catch {
          // ignore cleanup failures
        }
      }

      const message = String(
        error?.message || "Failed to start CSV import",
      ).trim();
      if (message === "Only CSV files are supported") {
        return res.status(400).json({ success: false, error: message });
      }
      if (String(error?.code || "").trim() === "LIMIT_FILE_SIZE") {
        return res
          .status(413)
          .json({ success: false, error: "CSV file is too large" });
      }

      return res.status(500).json({
        success: false,
        error: message,
      });
    }
  },
);

router.get(
  "/imports",
  requirePlanFeature("broadcastMessaging"),
  async (req, res) => {
    try {
      const limit = Math.max(
        1,
        Math.min(50, Number(req.query?.limit || 10) || 10),
      );
      const jobs = await CsvImportJob.find({
        userId: req.user.id,
        ...(req.companyId ? { companyId: req.companyId } : {}),
      })
        .sort({ updatedAt: -1, createdAt: -1 })
        .limit(limit)
        .lean();

      return res.json({
        success: true,
        data: {
          jobs: jobs.map(normalizeCsvImportJob),
        },
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Failed to load CSV imports",
      });
    }
  },
);

router.get(
  "/imports/:id",
  requirePlanFeature("broadcastMessaging"),
  async (req, res) => {
    try {
      const job = await assertCsvImportOwnership(req, req.params.id);
      if (!job) {
        return res.status(404).json({
          success: false,
          error: "CSV import job not found",
        });
      }

      return res.json({
        success: true,
        data: {
          job: normalizeCsvImportJob(job),
        },
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Failed to load CSV import job",
      });
    }
  },
);

router.post(
  "/imports/:id/cancel",
  requirePlanFeature("broadcastMessaging"),
  async (req, res) => {
    try {
      const job = await assertCsvImportOwnership(req, req.params.id);
      if (!job) {
        return res.status(404).json({
          success: false,
          error: "CSV import job not found",
        });
      }

      if (
        ["completed", "failed", "cancelled"].includes(
          String(job.status || "").toLowerCase(),
        )
      ) {
        return res.json({
          success: true,
          data: {
            job: normalizeCsvImportJob(job),
            cancelled: false,
          },
        });
      }

      await CsvImportJob.findByIdAndUpdate(job._id, {
        $set: {
          status: "cancelled",
          currentStage: "cancelled",
          completedAt: new Date(),
          lastProgressAt: new Date(),
          updatedAt: new Date(),
          errorMessage: "Cancelled by user",
        },
      });

      if (job.queueJobId) {
        await cancelCsvImportQueueJob(job.queueJobId);
      }

      return res.json({
        success: true,
        data: {
          job: normalizeCsvImportJob(
            await CsvImportJob.findById(job._id).lean(),
          ),
          cancelled: true,
        },
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Failed to cancel CSV import job",
      });
    }
  },
);

router.post(
  "/validate-audience",
  requirePlanFeature("broadcastMessaging"),
  async (req, res) => {
    try {
      const recipients = Array.isArray(req.body?.recipients)
        ? req.body.recipients
        : [];
      const messageType = toCleanString(
        req.body?.messageType || req.body?.message_type || "template",
      );
      const templateCategory =
        toCleanString(req.body?.templateCategory || "") || "utility";

      if (!recipients.length) {
        return res.status(400).json({
          success: false,
          error: "Recipients are required for validation",
        });
      }

      const contactsByPhone = await buildContactsByPhoneMap(req, recipients);
      const validation = buildBroadcastAudienceValidation({
        recipients,
        contactsByPhone,
        messageType,
        templateCategory,
      });

      return res.json({
        success: true,
        data: {
          ...validation,
          canProceed: recipients.length > 0,
        },
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: error.message || "Failed to validate audience",
      });
    }
  },
);

router.post(
  "/send",
  requirePlanFeature("broadcastMessaging"),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      const {
        broadcast_name,
        name,
        messageType = "text",
        message_type,
        templateName,
        template_name,
        templateCategory = "",
        templateContent = "",
        template_content = "",
        language = "en_US",
        mediaUrl = "",
        mediaType = "",
        customMessage = "",
        custom_message = "",
        recipients = [],
        deliveryPolicy = {},
        retryPolicy = {},
        compliancePolicy = {},
        scheduledAt = null,
      } = req.body || {};

      const resolvedRecipients = Array.isArray(recipients) ? recipients : [];
      if (!resolvedRecipients.length) {
        return res.status(400).json({
          success: false,
          error: "Recipients data is required",
        });
      }

      const created = await broadcastService.createBroadcast({
        name: String(
          broadcast_name || name || `Bulk Send - ${new Date().toISOString()}`,
        ).trim(),
        companyId: req.companyId || null,
        messageType:
          String(message_type || messageType || "text").trim() || "text",
        message: String(custom_message || customMessage || "").trim(),
        templateName:
          String(template_name || templateName || "").trim() || null,
        templateCategory:
          String(templateCategory || "")
            .trim()
            .toLowerCase() || "utility",
        templateContent: String(
          template_content || templateContent || "",
        ).trim(),
        language: String(language || "en_US").trim() || "en_US",
        mediaUrl: String(mediaUrl || "").trim(),
        mediaType: String(mediaType || "")
          .trim()
          .toLowerCase(),
        recipients: resolvedRecipients,
        recipientCount: resolvedRecipients.length,
        audienceSource:
          req.body?.audienceSource &&
          typeof req.body.audienceSource === "object"
            ? req.body.audienceSource
            : {},
        audienceSnapshot:
          req.body?.audienceSnapshot &&
          typeof req.body.audienceSnapshot === "object"
            ? req.body.audienceSnapshot
            : {},
        createdBy: req.user.username || req.user.email || req.user.id,
        createdByEmail: req.user.email,
        createdById: req.user.id,
        deliveryPolicy,
        retryPolicy,
        compliancePolicy,
        scheduledAt: scheduledAt || undefined,
      });

      if (!created?.success || !created?.data?._id) {
        return res.status(400).json({
          success: false,
          error: created?.error || "Failed to create broadcast",
        });
      }

      const sendResult = await broadcastService.sendBroadcast(
        created.data._id,
        null,
        req.whatsappCredentials || null,
      );

      if (!sendResult?.success) {
        return res.status(400).json(sendResult);
      }

      return res.status(200).json({
        success: true,
        queued: false,
        broadcastId: created.data._id,
        total_sent: resolvedRecipients.length,
        successful: Number(sendResult?.data?.successful || 0),
        failed: Number(sendResult?.data?.failed || 0),
        results: sendResult?.data?.results || [],
        data: sendResult.data,
        message: "Broadcast sent. Inbox updates were written immediately.",
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  },
);

module.exports = router;
