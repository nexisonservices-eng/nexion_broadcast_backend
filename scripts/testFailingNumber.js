const axios = require('axios');

async function testFailingNumber() {
  try {
    console.log('🧪 Testing with the failing phone number...');
    
    const testData = {
      message_type: 'custom',
      custom_message: 'Test message for failing number',
      broadcast_name: 'Test Failing Number',
      recipients: [
        { phone: '919677973676', name: 'Nandha', variables: [] }
      ]
    };

    const response = await axios.post('http://localhost:3001/api/bulk/send', testData, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    console.log('✅ Test with failing number successful!');
    console.log('Response:', response.data);
  } catch (error) {
    console.error('❌ Test with failing number failed:');
    console.error('Status:', error.response?.status);
    console.error('Data:', error.response?.data);
    console.error('Message:', error.message);
  }
}

testFailingNumber();
