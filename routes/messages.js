const express = require('express');
const router = express.Router();
const Message = require('../models/Message');

// Delete selected messages
router.delete('/delete-selected', async (req, res) => {
  try {
    const { messageIds } = req.body;
    
    if (!messageIds || !Array.isArray(messageIds)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message IDs array is required' 
      });
    }
    
    // Delete messages from database
    const deleteResult = await Message.deleteMany({ _id: { $in: messageIds } });
    
    res.json({ 
      success: true, 
      message: `${deleteResult.deletedCount} messages deleted successfully`,
      deletedCount: deleteResult.deletedCount
    });
  } catch (error) {
    console.error('Error deleting messages:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
