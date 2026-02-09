const mongoose = require('mongoose');

async function connectToServerDatabase() {
  try {
    // Use exact same connection string as server
    const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/whatsapp_platform';
    console.log(`🔗 Connecting to: ${MONGODB_URI}`);
    
    await mongoose.connect(MONGODB_URI);
    console.log('✅ Connected to server database');
    console.log(`📍 Database: ${mongoose.connection.name}`);
    console.log(`📍 Host: ${mongoose.connection.host}`);
    
    // Get the Broadcast model (same as server)
    const BroadcastSchema = new mongoose.Schema({}, { strict: false });
    const Broadcast = mongoose.model('Broadcast', BroadcastSchema);
    
    // Count broadcasts
    const count = await Broadcast.countDocuments();
    console.log(`📊 Broadcasts found: ${count}`);
    
    if (count > 0) {
      console.log('\n⚠️  FOUND BROADCASTS! Deleting them now...');
      
      // Get sample before deletion
      const samples = await Broadcast.find({}).limit(3);
      console.log('Sample broadcasts to delete:');
      samples.forEach((b, i) => {
        console.log(`   ${i + 1}. ${b.name} (ID: ${b._id})`);
      });
      
      // Delete all
      const result = await Broadcast.deleteMany({});
      console.log(`🗑️  Deleted ${result.deletedCount} broadcasts`);
      
      // Verify
      const remaining = await Broadcast.countDocuments();
      console.log(`📊 Remaining broadcasts: ${remaining}`);
      
    } else {
      console.log('✅ No broadcasts found in server database');
    }
    
    // Check all collections
    const collections = await mongoose.connection.db.listCollections().toArray();
    console.log('\n📁 Collections in server database:');
    for (const collection of collections) {
      const count = await mongoose.connection.db.collection(collection.name).countDocuments();
      console.log(`   - ${collection.name}: ${count} documents`);
    }
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await mongoose.connection.close();
    console.log('\n🔌 Connection closed');
  }
}

connectToServerDatabase();
