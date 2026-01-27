import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import axios from 'axios';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { runUpdateUserMapping } from './hubspot/users/updateUserMapping.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();

const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const DB_NAME = 'GoHighLevel';
const HIGHLEVEL_API_URL = process.env.HIGHLEVEL_API_URL;
const HIGHLEVEL_API_KEY = process.env.HIGHLEVEL_API_KEY;
const HIGHLEVEL_LOCATION_ID = process.env.HIGHLEVEL_LOCATION_ID;

const ALLOWED_COLLECTIONS = new Set([
    'contacts',
    'users',
    'opportunities',
    'calendars',
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

function getGhlHeaders() {
    return {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
}

async function delay(time = 500) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

async function getGhlIds(collection) {
    const headers = getGhlHeaders();
    const ids = [];

    try {
        switch (collection) {
            case 'contacts': {
                let url = `${HIGHLEVEL_API_URL}/contacts?limit=100&locationId=${HIGHLEVEL_LOCATION_ID}`;
                url = url.replace('http:', 'https:');
                while (url) {
                    const response = await axios.get(url, { headers });
                    if (response?.data?.contacts && Array.isArray(response.data.contacts)) {
                        response.data.contacts.forEach((contact) => {
                            if (contact.id) ids.push(contact.id);
                        });
                        url = response?.data?.meta?.nextPageUrl;
                        if (url) {
                            url = url.replace('http:', 'https:');
                            await delay();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                break;
            }
            case 'users': {
                const url = `${HIGHLEVEL_API_URL}/users/?locationId=${HIGHLEVEL_LOCATION_ID}`;
                const response = await axios.get(url, { headers });
                if (response?.data?.users && Array.isArray(response.data.users)) {
                    response.data.users.forEach((user) => {
                        if (user.id) ids.push(user.id);
                    });
                }
                break;
            }
            case 'opportunities': {
                let page = 1;
                const pageSize = 100;
                while (true) {
                    const url = `${HIGHLEVEL_API_URL}/opportunities/search?location_id=${HIGHLEVEL_LOCATION_ID}&page=${page}&limit=${pageSize}`;
                    const response = await axios.get(url, { headers });
                    if (response.status === 200 && Array.isArray(response?.data?.opportunities)) {
                        if (response.data.opportunities.length === 0) break;
                        response.data.opportunities.forEach((opp) => {
                            if (opp.id) ids.push(opp.id);
                        });
                        page++;
                        await delay();
                    } else {
                        break;
                    }
                }
                break;
            }
            case 'conversations': {
                const baseUrl = `${HIGHLEVEL_API_URL || ''}`.replace(/\/+$/, '');
                const pageSize = 100;
                let page = 1;
                let total = -1;
                let emptyPageCount = 0; // Track consecutive empty pages
                const maxEmptyPages = 2; // Stop after 2 consecutive empty pages
                
                while (true) {
                    const url = `${baseUrl}/conversations/search?locationId=${encodeURIComponent(HIGHLEVEL_LOCATION_ID)}&page=${page}&limit=${pageSize}`;
                    try {
                        const response = await axios.get(url, { headers });
                        const data = response?.data || {};
                        const conversations = data?.conversations || data?.items || [];
                        
                        if (Array.isArray(conversations) && conversations.length > 0) {
                            emptyPageCount = 0; // Reset empty page counter
                            conversations.forEach((conv) => {
                                if (conv.id) ids.push(conv.id);
                            });
                            
                            // Get total from first response
                            if (total === -1) {
                                total = data.total || 0;
                                console.log(`[conversations] Total records from API: ${total}`);
                            }
                            
                            // Progress logging every 10 pages
                            if (page % 10 === 0) {
                                console.log(`[conversations] Progress: page ${page}, fetched ${ids.length} IDs so far...`);
                            }
                            
                            // If we have a total and we've fetched all, break
                            if (total > 0) {
                                total -= conversations.length;
                                if (total <= 0) {
                                    console.log(`[conversations] Fetched all records (total was ${data.total})`);
                                    break;
                                }
                            }
                            
                            // If we got fewer records than pageSize, we're likely at the end
                            if (conversations.length < pageSize) {
                                console.log(`[conversations] Last page (got ${conversations.length} records)`);
                                break;
                            }
                            
                            page++;
                            await delay();
                        } else {
                            // Empty page
                            emptyPageCount++;
                            if (emptyPageCount >= maxEmptyPages) {
                                console.log(`[conversations] Stopping after ${maxEmptyPages} empty pages`);
                                break;
                            }
                            page++;
                            await delay();
                        }
                    } catch (err) {
                        const status = err?.response?.status;
                        if (status === 404) {
                            console.log(`[conversations] 404 - no more pages`);
                            break;
                        }
                        console.error(`[conversations] Error on page ${page}:`, err.message);
                        break;
                    }
                }
                break;
            }
            case 'calendars': {
                const url = `${HIGHLEVEL_API_URL}/calendars/?locationId=${encodeURIComponent(HIGHLEVEL_LOCATION_ID)}`;
                const response = await axios.get(url, { headers });
                const calendars = response?.data?.calendars || response?.data || [];
                if (Array.isArray(calendars)) {
                    calendars.forEach((cal) => {
                        if (cal.id) ids.push(cal.id);
                    });
                }
                break;
            }
        }
    } catch (err) {
        console.error(`Error fetching ${collection} IDs from GHL:`, err.message);
    }

    return ids;
}

app.use(express.json({ limit: '1mb' }));
app.use(express.static(join(__dirname, 'public')));

app.post('/api/:collection/query', async (req, res) => {
    const { collection } = req.params;
    if (!ALLOWED_COLLECTIONS.has(collection)) {
        return res.status(400).json({ error: 'Unknown collection' });
    }
    const query = req.body?.query || {};
    const rawLimit = req.body?.limit;
    const limit = Number.isFinite(rawLimit) ? rawLimit : Number(rawLimit ?? 10);
    if (query && typeof query !== 'object') {
        return res.status(400).json({ error: 'Query must be an object' });
    }
    try {
        const db = await getDb();
        const cursor = db.collection(collection).find(query);
        if (Number.isFinite(limit) && limit > 0) {
            cursor.limit(limit);
        }
        const docs = await cursor.toArray();
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

app.post('/api/hubspot/users/delete', async (req, res) => {
    try {
        await runUpdateUserMapping({ deleteMode: true });
        return res.json({ status: 'ok', message: 'HubSpot user mapping deleted.' });
    } catch (err) {
        return res.status(500).json({ error: err.message });
    }
});

app.post('/api/hubspot/users/refresh', async (req, res) => {
    try {
        await runUpdateUserMapping({ merge: true });
        return res.json({ status: 'ok', message: 'HubSpot user mapping refreshed.' });
    } catch (err) {
        return res.status(500).json({ error: err.message });
    }
});

app.post('/api/customfields/destination', async (req, res) => {
    const { documentId, fieldId, model, destination } = req.body || {};
    if (!documentId || !fieldId) {
        return res.status(400).json({ error: 'documentId and fieldId are required' });
    }
    const updateModel = model || 'contact';
    const updatePath = `${updateModel}.${fieldId}.destination`;
    try {
        const db = await getDb();
        const filter = ObjectId.isValid(documentId)
            ? { _id: new ObjectId(documentId) }
            : { _id: documentId };
        const result = await db.collection('customfields').updateOne(
            filter,
            { $set: { [updatePath]: destination || null } }
        );
        if (!result.matchedCount) {
            return res.status(404).json({ error: 'Custom field document not found' });
        }
        return res.json({ status: 'ok' });
    } catch (err) {
        return res.status(500).json({ error: err.message });
    }
});

app.get('/api/:collection/compare', async (req, res) => {
    const { collection } = req.params;
    if (!ALLOWED_COLLECTIONS.has(collection)) {
        return res.status(400).json({ error: 'Unknown collection' });
    }

    if (!HIGHLEVEL_API_URL || !HIGHLEVEL_API_KEY || !HIGHLEVEL_LOCATION_ID) {
        return res.status(500).json({ error: 'GoHighLevel API credentials not configured (HIGHLEVEL_API_URL, HIGHLEVEL_API_KEY, HIGHLEVEL_LOCATION_ID)' });
    }

    try {
        const db = await getDb();
        console.log(`[${collection}] Starting comparison...`);
        
        // Get MongoDB count (fast)
        const mongoCount = await db.collection(collection).countDocuments();
        console.log(`[${collection}] MongoDB count: ${mongoCount}`);
        
        // Get GoHighLevel IDs (slow - paginated API calls)
        console.log(`[${collection}] Fetching IDs from GoHighLevel...`);
        const ghlIds = await getGhlIds(collection);
        const ghlCount = ghlIds.length;
        console.log(`[${collection}] GoHighLevel count: ${ghlCount}`);

        // If counts match, we can skip the detailed comparison
        if (mongoCount === ghlCount) {
            console.log(`[${collection}] Counts match! Skipping detailed ID comparison.`);
            return res.json({
                mongoCount,
                ghlCount,
                match: true,
                difference: 0,
                missingIds: [],
                missingCount: 0
            });
        }

        // Only fetch MongoDB IDs if counts don't match
        console.log(`[${collection}] Counts don't match. Fetching MongoDB IDs for comparison...`);
        const mongoDocs = await db.collection(collection).find({}, { projection: { id: 1 } }).toArray();
        const mongoIds = new Set(mongoDocs.map((doc) => doc.id).filter(Boolean));
        console.log(`[${collection}] MongoDB IDs fetched: ${mongoIds.size}`);

        const missingIds = ghlIds.filter((id) => !mongoIds.has(id));
        console.log(`[${collection}] Comparison complete. Missing: ${missingIds.length}`);

        return res.json({
            mongoCount,
            ghlCount,
            match: mongoCount === ghlCount,
            difference: ghlCount - mongoCount,
            missingIds: missingIds.slice(0, 100),
            missingCount: missingIds.length
        });
    } catch (err) {
        console.error(`[${collection}] Comparison error:`, err.message);
        return res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
});
