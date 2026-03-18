const jwt = require('jsonwebtoken');

// Temporary hardcoded JWT secret for testing
// In production, this should be properly configured via environment variables
const JWT_SECRET = process.env.JWT_SECRET || 'technova_jwt_secret_key_2024';

/**
 * Authentication guard: verifies Bearer token and attaches user info.
 */
const protect = (req, res, next) => {
  try {
    const authHeader = req.headers.authorization || '';
    if (!authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, error: 'Unauthorized: token missing' });
    }

    const token = authHeader.split(' ')[1];
    const decoded = jwt.verify(token, JWT_SECRET);

    req.user = {
      id: decoded.userId || decoded.id,
      email: decoded.email,
      role: decoded.role,
      username: decoded.username
    };

    if (!req.user.id && !req.user.email) {
      return res.status(401).json({ success: false, error: 'Unauthorized: invalid token payload' });
    }

    next();
  } catch (error) {
    return res.status(401).json({ success: false, error: 'Unauthorized: invalid token' });
  }
};

/**
 * Role-based authorization guard.
 * Usage: router.use(authorize('admin', 'superadmin'));
 */
const authorize = (...roles) => (req, res, next) => {
  if (!req.user || !roles.includes(req.user.role)) {
    return res.status(403).json({ success: false, error: 'Forbidden: insufficient permissions' });
  }
  next();
};

// Export both default middleware for backward compatibility (router.use(auth))
// and named helpers for explicit imports.
module.exports = protect;
module.exports.protect = protect;
module.exports.authorize = authorize;
