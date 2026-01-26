const axios = require('axios');

async function testBulkSend() {
  try {
    console.log('🧪 Testing bulk send endpoint...');
    
    const testData = {
      message_type: 'custom',
      custom_message: 'Test message from bulk send test',
      broadcast_name: 'Test Bulk Send',
      recipients: [
        { phone: '+1234567890', name: 'Test User', variables: [] }
      ]
    };

    const response = await axios.post('http://localhost:3001/api/bulk/send', testData, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    console.log('✅ Bulk send test successful!');
    console.log('Response:', response.data);
  } catch (error) {
    console.error('❌ Bulk send test failed:');
    console.error('Status:', error.response?.status);
    console.error('Data:', error.response?.data);
    console.error('Message:', error.message);
  }
}

testBulkSend();
