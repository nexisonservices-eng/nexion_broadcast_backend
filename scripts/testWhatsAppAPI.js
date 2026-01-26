const whatsappService = require('../services/whatsappService');

async function testWhatsAppAPI() {
  console.log('🧪 Testing WhatsApp API Configuration...');
  console.log('Phone Number ID:', process.env.WHATSAPP_PHONE_NUMBER_ID);
  console.log('Business Account ID:', process.env.WHATSAPP_BUSINESS_ACCOUNT_ID);
  console.log('Access Token:', process.env.WHATSAPP_ACCESS_TOKEN ? 'Present' : 'Missing');
  
  // Test sending a simple text message to a test number
  const testPhone = '+1234567890'; // Use one of the test numbers from your database
  
  try {
    console.log(`📤 Sending test message to ${testPhone}...`);
    const result = await whatsappService.sendTextMessage(testPhone, 'Test message from API check');
    
    if (result.success) {
      console.log('✅ WhatsApp API working successfully!');
      console.log('Response:', JSON.stringify(result.data, null, 2));
    } else {
      console.log('❌ WhatsApp API failed!');
      console.log('Error:', result.error);
    }
  } catch (error) {
    console.error('❌ Test failed with error:', error.message);
  }
}

testWhatsAppAPI();
