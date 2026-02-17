const mongoose = require('mongoose');

const AuthUserSchema = new mongoose.Schema(
  {
    username: String,
    email: String,
    whatsapptoken: String,
    whatsappbussiness: String,
    whatsappid: String,
    twilioid: String
  },
  {
    strict: false,
    collection: 'admin'
  }
);

module.exports = mongoose.models.AdminAuthUser || mongoose.model('AdminAuthUser', AuthUserSchema);