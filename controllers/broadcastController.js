const broadcastService = require("../services/broadcastService");
const {
  enqueueBroadcastSend,
  getBroadcastQueueCounts,
} = require("../queues/broadcastQueue");
const {
  getBroadcastInboxQueueCounts,
  getBroadcastInboxQueueLagSnapshot,
} = require("../queues/broadcastInboxQueue");
const { getQueueLagSnapshot } = require("../queues/broadcastQueue");
const { normalizeRole, isTenantWideRole } = require("../utils/accessControl");
const { emitAuthAuditLog } = require("../utils/authAuditLogger");
const {
  resolveAudienceRecipients,
} = require("../services/broadcastAudienceResolver");

class BroadcastController {
  async assertOwnership(broadcastId, userId, companyId, role, req = null) {
    const result = await broadcastService.getBroadcastById(broadcastId);
    if (!result.success) {
      emitAuthAuditLog({
        event: "broadcast_ownership",
        allowed: false,
        reason: "broadcast_not_found",
        req,
        extra: { broadcastId: String(broadcastId || "") },
      });
      return { ok: false, status: 404, body: result };
    }
    const normalizedRole = normalizeRole(role);
    const tenantWideAccess = isTenantWideRole(normalizedRole);
    if (
      (!tenantWideAccess &&
        String(result.data.createdById || "") !== String(userId)) ||
      (companyId && String(result.data.companyId || "") !== String(companyId))
    ) {
      emitAuthAuditLog({
        event: "broadcast_ownership",
        allowed: false,
        reason: "broadcast_forbidden",
        req,
        extra: {
          broadcastId: String(broadcastId || ""),
          broadcastOwnerId: String(result.data.createdById || ""),
          broadcastCompanyId: String(result.data.companyId || ""),
        },
      });
      return {
        ok: false,
        status: 404,
        body: { success: false, error: "Broadcast not found" },
      };
    }
    return { ok: true, data: result.data };
  }

  async createBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };
      const creds = req.whatsappCredentials || null;
      const payload = {
        ...req.body,
        companyId: req.companyId || null,
        createdById: req.user.id,
        createdBy: req.user.username || req.user.email || req.user.id,
        createdByEmail: req.user.email,
        createdByWorkspaceRole:
          req.user?.normalizedRole || req.user?.companyRole || req.user?.role || "",
        authHeaderSnapshot: req.headers.authorization || null,
        credentialsSnapshot: creds
          ? {
              accessToken: creds.accessToken || creds.whatsappToken || null,
              businessAccountId:
                creds.businessAccountId || creds.whatsappBusiness || null,
              phoneNumberId: creds.phoneNumberId || creds.whatsappId || null,
              whatsappToken: creds.whatsappToken || creds.accessToken || null,
              whatsappBusiness:
                creds.whatsappBusiness || creds.businessAccountId || null,
              whatsappId: creds.whatsappId || creds.phoneNumberId || null,
              twilioId: creds.twilioId || null,
            }
          : undefined,
      };

      const result = await broadcastService.createBroadcast(
        payload,
        broadcaster,
      );
      if (result.success) {
        res.status(201).json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async duplicateBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };

      const ownership = await this.assertOwnership(
        req.params.id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const result = await broadcastService.duplicateBroadcastDraft(
        req.params.id,
        {
          createdById: req.user.id,
          createdBy: req.user.username || req.user.email || req.user.id,
          createdByEmail: req.user.email,
          createdByWorkspaceRole:
            req.user?.normalizedRole ||
            req.user?.companyRole ||
            req.user?.role ||
            "",
          name: String(req.body?.name || "").trim(),
          broadcaster,
          credentials: req.whatsappCredentials || null,
        },
      );

      if (result.success) {
        return res.status(201).json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async updateBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };

      const ownership = await this.assertOwnership(
        req.params.id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const updatePayload = {
        ...req.body,
        ...(req.body?.audienceSource && typeof req.body.audienceSource === "object"
          ? { audienceSource: req.body.audienceSource }
          : {}),
        ...(req.body?.audienceFilters && typeof req.body.audienceFilters === "object"
          ? { audienceFilters: req.body.audienceFilters }
          : {}),
      };

      const result = await broadcastService.updateBroadcastDraft(
        req.params.id,
        updatePayload,
        {
          broadcaster,
          credentials: req.whatsappCredentials || null,
        },
      );

      if (result.success) {
        return res.status(200).json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async sendBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };

      const ownership = await this.assertOwnership(
        req.params.id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const recipientCount = Number(ownership.data?.recipientCount || 0);
      req.broadcastMessageCount = Array.isArray(ownership.data?.recipients)
        ? ownership.data.recipients.length
        : recipientCount > 0
          ? recipientCount
          : 1;

      if (String(req.user?.planCode || "").toLowerCase() === "trial") {
        const usedMessages = Number(
          req.user?.trialUsage?.whatsappMessages || 0,
        );
        const messageLimit = Number(
          req.user?.trialLimits?.whatsappMessages || 50,
        );
        if (
          usedMessages + Number(req.broadcastMessageCount || 1) >
          messageLimit
        ) {
          return res.status(403).json({
            success: false,
            error: "Trial message limit reached. Upgrade to continue.",
          });
        }
      }

      const result = await broadcastService.sendBroadcast(
        req.params.id,
        broadcaster,
        req.whatsappCredentials || null,
      );

      if (!result.success) {
        return res.status(400).json(result);
      }

      res.status(200).json({
        success: true,
        queued: false,
        broadcastId: req.params.id,
        message: "Broadcast sent. Inbox updates were written immediately.",
        data: result.data,
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getBroadcasts(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const filters = {};
      if (req.query.status) filters.status = req.query.status;
      if (req.query.createdBy) filters.createdBy = req.query.createdBy;
      if (req.query.createdById) filters.createdById = req.query.createdById;
      if (req.query.search) filters.search = req.query.search;
      if (req.query.cursor) filters.cursor = req.query.cursor;
      if (req.query.limit) filters.limit = req.query.limit;
      if (!tenantWideAccess) {
        filters.createdById = req.user.id;
      }
      if (req.companyId) {
        filters.companyId = req.companyId;
      }

      const result = await broadcastService.getBroadcasts(filters);
      res.json(result);
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getCampaignSelectionBroadcasts(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const filters = {
        search: req.query.search || "",
        status: req.query.status || "",
        cursor: req.query.cursor || "",
        limit: req.query.limit || 20,
      };
      if (!tenantWideAccess) {
        filters.createdById = req.user.id;
      }
      if (req.companyId) {
        filters.companyId = req.companyId;
      }
      const result =
        await broadcastService.getCampaignSelectionBroadcasts(filters);
      res.json(result);
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getBroadcastAudienceRecipients(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const filters = {
        search: req.query.search || "",
        cursor: req.query.cursor || "",
        limit: req.query.limit || 50,
      };
      if (!tenantWideAccess) {
        filters.createdById = req.user.id;
      }
      if (req.companyId) {
        filters.companyId = req.companyId;
      }
      const result = await broadcastService.getBroadcastAudienceRecipients(
        req.params.id,
        filters,
      );
      if (!result.success) {
        return res.status(404).json(result);
      }
      res.json(result);
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async previewBroadcastAudience(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const result = await resolveAudienceRecipients({
        companyId: req.companyId || null,
        userId: tenantWideAccess ? null : req.user.id,
        mode: req.body?.mode || req.query?.mode || "manual_contacts",
        segmentId: req.body?.segmentId || req.query?.segmentId || "",
        broadcastId: req.body?.broadcastId || req.query?.broadcastId || "",
        importJobId: req.body?.importJobId || req.query?.importJobId || "",
        contacts: Array.isArray(req.body?.contacts) ? req.body.contacts : [],
        contactIds: Array.isArray(req.body?.contactIds) ? req.body.contactIds : [],
        filters:
          req.body?.filters && typeof req.body.filters === "object"
            ? req.body.filters
            : {},
      });

      return res.json({
        success: true,
        data: {
          mode: result.mode,
          source: result.source
            ? {
                id: String(result.source._id || result.source.id || ""),
                name: String(result.source.name || ""),
              }
            : null,
          summary: result.summary,
          recipients: result.recipients.slice(0, 20),
          invalidRecipients: result.invalidRecipients.slice(0, 20),
        },
      });
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async getBroadcastAudienceSummary(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const ownership = await this.assertOwnership(
        req.params.id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const result = await broadcastService.getBroadcastAudienceSummary(
        req.params.id,
      );
      if (result.success) {
        return res.json({
          success: true,
          data: {
            ...result.data,
            access: tenantWideAccess ? "tenant_wide" : "owner_only",
          },
        });
      }

      return res.status(404).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async saveBroadcastAudienceAsSegment(req, res) {
    try {
      const ownership = await this.assertOwnership(
        req.params.id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const result = await broadcastService.saveBroadcastAudienceAsSegment(
        req.params.id,
        {
          name: String(req.body?.name || "").trim(),
          description: String(req.body?.description || "").trim(),
          userId: req.user.id,
          companyId: req.companyId || null,
          updatedBy: req.user.username || req.user.email || req.user.id,
        },
      );

      if (result.success) {
        return res.status(201).json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async createBroadcastFromSegment(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };

      const segmentId = String(req.body?.segmentId || req.params.segmentId || "").trim();
      if (!segmentId) {
        return res.status(400).json({
          success: false,
          error: "Group id is required",
        });
      }

      const result = await broadcastService.createBroadcastDraftFromSegment(
        segmentId,
        {
          createdById: req.user.id,
          createdBy: req.user.username || req.user.email || req.user.id,
          createdByEmail: req.user.email,
          createdByWorkspaceRole:
            req.user?.normalizedRole ||
            req.user?.companyRole ||
            req.user?.role ||
            "",
          name: String(req.body?.name || "").trim(),
          broadcaster,
          credentials: req.whatsappCredentials || null,
        },
      );

      if (result.success) {
        return res.status(201).json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async getBroadcastById(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const result = await broadcastService.getBroadcastById(req.params.id);
      if (
        result.success &&
        ((!tenantWideAccess &&
          String(result.data.createdById || "") !== String(req.user.id)) ||
          (req.companyId &&
            String(result.data.companyId || "") !== String(req.companyId)))
      ) {
        emitAuthAuditLog({
          event: "broadcast_ownership",
          allowed: false,
          reason: "broadcast_forbidden",
          req,
          extra: {
            broadcastId: String(req.params.id || ""),
            broadcastOwnerId: String(result?.data?.createdById || ""),
            broadcastCompanyId: String(result?.data?.companyId || ""),
          },
        });
        return res
          .status(404)
          .json({ success: false, error: "Broadcast not found" });
      }
      if (result.success) {
        res.json(result);
      } else {
        emitAuthAuditLog({
          event: "broadcast_ownership",
          allowed: false,
          reason: "broadcast_not_found",
          req,
          extra: { broadcastId: String(req.params.id || "") },
        });
        res.status(404).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async checkScheduledBroadcasts(req, res) {
    try {
      const broadcaster = req.app?.locals?.broadcast;
      await broadcastService.checkScheduledBroadcasts(broadcaster);
      res.json({ success: true, message: "Scheduled broadcasts checked" });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async syncBroadcastStats(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.syncBroadcastStats(id, broadcaster);
      if (result.success) {
        res.json(result);
      } else {
        res.status(404).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.deleteBroadcast(id, broadcaster);
      if (result.success) {
        res.json(result);
      } else {
        res.status(404).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async pauseBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.pauseBroadcast(id, broadcaster);
      if (result.success) {
        res.json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async resumeBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.resumeBroadcast(id, broadcaster);
      if (result.success) {
        res.json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async cancelScheduledBroadcast(req, res) {
    try {
      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.cancelScheduledBroadcast(
        id,
        broadcaster,
      );
      if (result.success) {
        res.json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getReliabilitySummary(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const filters = {};

      if (!tenantWideAccess) {
        filters.createdById = req.user.id;
      }
      if (req.companyId) {
        filters.companyId = req.companyId;
      }
      if (req.query.status) {
        filters.status = req.query.status;
      }
      if (req.query.dateFrom) {
        filters.createdFrom = req.query.dateFrom;
      }
      if (req.query.dateTo) {
        filters.createdTo = req.query.dateTo;
      }

      const result = await broadcastService.getReliabilitySummary(filters);
      if (result.success) {
        return res.json(result);
      }
      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async getOverviewSummary(req, res) {
    try {
      const normalizedRole = normalizeRole(
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
      );
      const tenantWideAccess = isTenantWideRole(normalizedRole);
      const filters = {};

      if (!tenantWideAccess) {
        filters.createdById = req.user.id;
      }
      if (req.companyId) {
        filters.companyId = req.companyId;
      }
      if (req.query.status) {
        filters.status = req.query.status;
      }
      if (req.query.dateFrom) {
        filters.createdFrom = req.query.dateFrom;
      }
      if (req.query.dateTo) {
        filters.createdTo = req.query.dateTo;
      }

      const result = await broadcastService.getOverviewSummary(filters);
      if (result.success) {
        return res.json(result);
      }
      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async retryFailedRecipients(req, res) {
    try {
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };

      const result = await broadcastService.retryFailedRecipients(
        id,
        broadcaster,
        req.whatsappCredentials || null,
      );
      if (result.success) {
        return res.json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async repairBroadcastTemplateHeaderAndRetry(req, res) {
    try {
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === "function") {
          sendToUser(String(req.user.id), payload);
        }
      };

      const result =
        await broadcastService.repairBroadcastTemplateHeaderAndRetry(
          id,
          {
            mediaUrl: req.body?.mediaUrl || "",
            mediaType: req.body?.mediaType || "image",
          },
          broadcaster,
          req.whatsappCredentials || null,
        );
      if (result.success) {
        return res.json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async repairBroadcastDispatchInbox(req, res) {
    try {
      const { id } = req.params;
      const ownership = await this.assertOwnership(
        id,
        req.user.id,
        req.companyId,
        req.user?.normalizedRole || req.user?.companyRole || req.user?.role,
        req,
      );
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const limit = Math.max(
        1,
        Number(req.query?.limit || req.body?.limit || 50) || 50,
      );
      const result =
        await broadcastService.repairBroadcastDispatchInboxForBroadcast(
          id,
          limit,
        );
      if (result.success) {
        return res.json(result);
      }

      return res.status(400).json(result);
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  async getQueueMetrics(req, res) {
    try {
      const [sendCounts, inboxCounts, sendLag, inboxLag, rateLimitSnapshot] =
        await Promise.all([
          getBroadcastQueueCounts(),
          getBroadcastInboxQueueCounts(),
          getQueueLagSnapshot(),
          getBroadcastInboxQueueLagSnapshot(),
          req.query?.broadcastId ||
          req.query?.phoneNumberId ||
          req.query?.businessAccountId
            ? broadcastService.getBroadcastRateLimitSnapshot({
                broadcastId: req.query?.broadcastId || null,
                credentials: {
                  phoneNumberId: req.query?.phoneNumberId || "",
                  businessAccountId: req.query?.businessAccountId || "",
                  whatsappId: req.query?.phoneNumberId || "",
                  whatsappBusiness: req.query?.businessAccountId || "",
                },
              })
            : Promise.resolve(null),
        ]);
      return res.json({
        success: true,
        data: {
          queues: {
            "broadcast-send": sendCounts,
            "broadcast-inbox-write": inboxCounts,
          },
          lag: {
            "broadcast-send": sendLag,
            "broadcast-inbox-write": inboxLag,
          },
          rateLimit: rateLimitSnapshot || null,
        },
      });
    } catch (error) {
      return res.status(500).json({ success: false, error: error.message });
    }
  }
}

module.exports = new BroadcastController();
