require('dotenv').config();

console.log('=== Environment Variables Debug ===');
console.log('MONGODB_URI:', process.env.MONGODB_URI ? 'Present' : 'Missing');
console.log('PORT:', process.env.PORT);
console.log('WHATSAPP_PHONE_NUMBER_ID:', process.env.WHATSAPP_PHONE_NUMBER_ID);
console.log('WHATSAPP_BUSINESS_ACCOUNT_ID:', process.env.WHATSAPP_BUSINESS_ACCOUNT_ID);
console.log('WHATSAPP_ACCESS_TOKEN length:', process.env.WHATSAPP_ACCESS_TOKEN ? process.env.WHATSAPP_ACCESS_TOKEN.length : 0);
console.log('WHATSAPP_ACCESS_TOKEN first 20 chars:', process.env.WHATSAPP_ACCESS_TOKEN ? process.env.WHATSAPP_ACCESS_TOKEN.substring(0, 20) + '...' : 'Missing');
console.log('WEBHOOK_VERIFY_TOKEN:', process.env.WEBHOOK_VERIFY_TOKEN);
console.log('FRONTEND_URL:', process.env.FRONTEND_URL);

// Test if the token looks like a valid Facebook token
const token = process.env.WHATSAPP_ACCESS_TOKEN;
if (token) {
  console.log('\n=== Token Analysis ===');
  console.log('Token starts with EAAU?', token.startsWith('EAAU'));
  console.log('Token length reasonable?', token.length > 50 && token.length < 500);
  console.log('Token contains only valid chars?', /^[A-Za-z0-9_\-]+$/.test(token));
}
