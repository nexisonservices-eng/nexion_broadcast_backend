const broadcastService = require('../services/broadcastService');

class BroadcastController {
  async assertOwnership(broadcastId, userId) {
    const result = await broadcastService.getBroadcastById(broadcastId);
    if (!result.success) return { ok: false, status: 404, body: result };
    if (String(result.data.createdById || '') !== String(userId)) {
      return { ok: false, status: 404, body: { success: false, error: 'Broadcast not found' } };
    }
    return { ok: true, data: result.data };
  }

  async createBroadcast(req, res) {
    try {
      const payload = {
        ...req.body,
        createdById: req.user.id,
        createdBy: req.user.username || req.user.email || req.user.id,
        createdByEmail: req.user.email
      };

      const result = await broadcastService.createBroadcast(payload);
      if (result.success) {
        res.status(201).json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async sendBroadcast(req, res) {
    try {
      const ownership = await this.assertOwnership(req.params.id, req.user.id);
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }

      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === 'function') {
          sendToUser(String(req.user.id), payload);
        }
      };
      const result = await broadcastService.sendBroadcast(req.params.id, broadcaster, req.whatsappCredentials);
      if (result.success) {
        res.json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getBroadcasts(req, res) {
    try {
      const filters = {};
      if (req.query.status) filters.status = req.query.status;
      if (req.query.createdBy) filters.createdBy = req.query.createdBy;
      filters.createdById = req.user.id;
      
      const result = await broadcastService.getBroadcasts(filters);
      res.json(result);
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getBroadcastById(req, res) {
    try {
      const result = await broadcastService.getBroadcastById(req.params.id);
      if (result.success && String(result.data.createdById || '') !== String(req.user.id)) {
        return res.status(404).json({ success: false, error: 'Broadcast not found' });
      }
      if (result.success) {
        res.json(result);
      } else {
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
      res.json({ success: true, message: 'Scheduled broadcasts checked' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async syncBroadcastStats(req, res) {
    try {
      const { id } = req.params;
      const ownership = await this.assertOwnership(id, req.user.id);
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.syncBroadcastStats(id);
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
      const { id } = req.params;
      const ownership = await this.assertOwnership(id, req.user.id);
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.deleteBroadcast(id);
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
      const { id } = req.params;
      const ownership = await this.assertOwnership(id, req.user.id);
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.pauseBroadcast(id);
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
      const { id } = req.params;
      const ownership = await this.assertOwnership(id, req.user.id);
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.resumeBroadcast(id);
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
      const { id } = req.params;
      const ownership = await this.assertOwnership(id, req.user.id);
      if (!ownership.ok) {
        return res.status(ownership.status).json(ownership.body);
      }
      const result = await broadcastService.cancelScheduledBroadcast(id);
      if (result.success) {
        res.json(result);
      } else {
        res.status(400).json(result);
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
}

module.exports = new BroadcastController();

