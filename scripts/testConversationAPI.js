const http = require('http');

// Test the conversation API endpoints
console.log('🧪 Testing Conversation API endpoints...\n');

// Test getConversationContacts endpoint
const options = {
  hostname: 'localhost',
  port: 3001,
  path: '/api/conversations/contacts/unique',
  method: 'GET',
  headers: {
    'Content-Type': 'application/json'
  }
};

const req = http.request(options, (res) => {
  let data = '';
  
  res.on('data', (chunk) => {
    data += chunk;
  });
  
  res.on('end', () => {
    try {
      const jsonData = JSON.parse(data);
      console.log('✅ Conversation Contacts API Response:');
      console.log(`Status: ${res.statusCode}`);
      console.log(`Success: ${jsonData.success}`);
      console.log(`Contacts found: ${jsonData.data?.length || 0}`);
      
      if (jsonData.data && jsonData.data.length > 0) {
        console.log('\nSample contacts:');
        jsonData.data.slice(0, 3).forEach((contact, index) => {
          console.log(`${index + 1}. ${contact.name} (${contact.phone})`);
        });
      }
    } catch (error) {
      console.log('❌ Error parsing response:', error.message);
      console.log('Raw response:', data);
    }
  });
});

req.on('error', (error) => {
  console.log('❌ API request error:', error.message);
});

req.end();

setTimeout(() => {
  console.log('\n🎯 API test completed!');
  process.exit(0);
}, 3000);
