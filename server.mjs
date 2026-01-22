import express from 'express';
import { MongoClient } from 'mongodb';

const app = express();

const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const DB_NAME = 'GoHighLevel';
const ALLOWED_COLLECTIONS = new Set([
    'contacts',
    'users',
    'opportunities',
    'calendars'
    ,
    'conversations',
    'customfields',
    'locations'
]);

let mongoClient;
let mongoDb;

async function getDb() {
    if (mongoDb) {
        return mongoDb;
    }
    if (!MONGO_URI) {
        throw new Error('MONGO_URI is not set');
    }
    if (!mongoClient) {
        mongoClient = new MongoClient(MONGO_URI);
    }
    if (!mongoClient.topology?.isConnected?.()) {
        await mongoClient.connect();
    }
    mongoDb = mongoClient.db(DB_NAME);
    return mongoDb;
}

app.use(express.json({ limit: '1mb' }));
app.use(express.static(new URL('./public', import.meta.url).pathname));

app.post('/api/:collection/query', async (req, res) => {
    const { collection } = req.params;
    if (!ALLOWED_COLLECTIONS.has(collection)) {
        return res.status(400).json({ error: 'Unknown collection' });
    }
    const query = req.body?.query || {};
    if (query && typeof query !== 'object') {
        return res.status(400).json({ error: 'Query must be an object' });
    }
    try {
        const db = await getDb();
        const docs = await db.collection(collection).find(query).limit(10).toArray();
        return res.json({ count: docs.length, items: docs });
    } catch (err) {
        return res.status(500).json({ error: err.message });
    }
});

app.get('/api/:collection/count', async (req, res) => {
    const { collection } = req.params;
    if (!ALLOWED_COLLECTIONS.has(collection)) {
        return res.status(400).json({ error: 'Unknown collection' });
    }
    try {
        const db = await getDb();
        const count = await db.collection(collection).countDocuments();
        return res.json({ count });
    } catch (err) {
        return res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
});
