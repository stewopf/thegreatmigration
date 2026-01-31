import axios from 'axios';
import { MongoClient } from 'mongodb';
import logger from './src/log.mjs'
const log = logger(import.meta.url);

const HIGHLEVEL_API_URL = process.env.HIGHLEVEL_API_URL;
const HIGHLEVEL_API_KEY = process.env.HIGHLEVEL_API_KEY;
const HIGHLEVEL_LOCATION_ID = process.env.HIGHLEVEL_LOCATION_ID;
const HIGHLEVEL_DEFAULT_USER_ID = process.env.HIGHLEVEL_DEFAULT_USER_ID;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const MONGO_DB_NAME = 'GoHighLevel';

let mongoClient;
let mongoDb;

async function getMongoDb() {
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
    mongoDb = mongoClient.db(MONGO_DB_NAME);
    return mongoDb;
}

async function upsertById(collectionName, items) {
    if (!Array.isArray(items) || items.length === 0) {
        return 0;
    }
    const db = await getMongoDb();
    const collection = db.collection(collectionName);
    const operations = items
        .filter((item) => item && item.id)
        .map((item) => ({
            replaceOne: {
                filter: { id: item.id },
                replacement: item,
                upsert: true
            }
        }));
    if (operations.length === 0) {
        return 0;
    }
    const result = await collection.bulkWrite(operations, { ordered: false });
    return { upserted: result.upsertedCount, modified: result.modifiedCount, matched: result.matchedCount };
}

export async function storeGhlData({ contacts, users, opportunities, calendars, conversations, customFields }) {
    const [contactsCount, usersCount, opportunitiesCount, calendarsCount, conversationsCount, customFieldsCount] = await Promise.all([
        upsertById('contacts', contacts),
        upsertById('users', users),
        upsertById('opportunities', opportunities),
        upsertById('calendars', calendars),
        upsertById('conversations', conversations),
        upsertById('customfields', customFields)
    ]);
    return {
        contacts: contactsCount,
        users: usersCount,
        opportunities: opportunitiesCount,
        calendars: calendarsCount,
        conversations: conversationsCount,
        customFields: customFieldsCount
    };
}


export async function getAllNotes(contactId) {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const url = `${HIGHLEVEL_API_URL}/contacts/${encodeURIComponent(contactId)}/notes`;
    try {
        const response = await axios.get(url, { headers });
        return response?.data?.notes;
    } catch (err) {
        log.warn('retrieveHighlevelCustomFields url=%s, error=%s', url, err.toString());
    }
}

export async function importAllContactNotes({ delayMs = 200 } = {}) {
    const db = await getMongoDb();
    const contactsCollection = db.collection('contacts');
    const cursor = contactsCollection.find({ id: { $exists: true } }, { projection: { id: 1 } });
    let processedContacts = 0;
    let contactsWithNotes = 0;
    let totalNotesUpserted = 0;
    let totalNotesModified = 0;

    for await (const contact of cursor) {
        const contactId = contact?.id;
        if (!contactId) {
            continue;
        }
        processedContacts++;
        const notes = await getAllNotes(contactId);
        if (!Array.isArray(notes) || notes.length === 0) {
            continue;
        }
        const normalized = notes
            .filter((note) => note && note.id)
            .map((note) => ({
                ...note,
                contactId: note.contactId || contactId
            }));
        if (normalized.length > 0) {
            const result = await upsertById('notes', normalized);
            contactsWithNotes++;
            totalNotesUpserted += result?.upserted || 0;
            totalNotesModified += result?.modified || 0;
        }
        if (delayMs > 0) {
            await delay(delayMs);
        }
    }
    return { processedContacts, contactsWithNotes, totalNotesUpserted, totalNotesModified };
}
async function retrieveHighlevelCustomFields(model = 'contact') {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const url = `${HIGHLEVEL_API_URL}/locations/${encodeURIComponent(HIGHLEVEL_LOCATION_ID)}/customFields?model=${encodeURIComponent(model)}`;
    try {
        const response = await axios.get(url, { headers });
        if (response?.data?.customFields) {
            return (response.data.customFields.reduce((acc, el) => {
                acc[el.id] = el;
                return acc;
            }, {}));
        }
        return null;
    } catch (err) {
        log.warn('retrieveHighlevelCustomFields model=%s, url=%s, error=%s', model, url, err.toString());
    }
}

async function delay(time = 2 * 1000) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(true);
        }, time);
    })
}


async function getUsers() {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const url = `${HIGHLEVEL_API_URL}/users/?locationId=${HIGHLEVEL_LOCATION_ID}`;
    try {
        const response = await axios.get(url, { headers });
        return response?.data?.users;
    } catch (err) {
        log.warn('getUsers url=%s, error=%s', url, err.toString());
    }
    return null;
}

export async function getAllConversations(locationId = HIGHLEVEL_LOCATION_ID) {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const ALL_CONVERSATIONS = [];
    const baseUrl = `${HIGHLEVEL_API_URL || ''}`.replace(/\/+$/, '');
    const pageSize = 100;
    let total = -1;
    let startAfterDate = null; // next batch: value of sort from last document
    let batchNum = 0;

    while (true) {
        batchNum++;
        let nextUrl = `${baseUrl}/conversations/search?locationId=${encodeURIComponent(locationId)}&limit=${pageSize}&sort=asc`;
        if (startAfterDate != null) {
            nextUrl += `&startAfterDate=${encodeURIComponent(startAfterDate)}`;
        }
        nextUrl = nextUrl.replace('http:', 'https:');
        try {
            log.debug('getAllConversations url=%s, batch=%s, total=%s', nextUrl, batchNum, total);
            const response = await axios.get(nextUrl, { headers });
            const data = response?.data || {};
            const conversations = data?.conversations || data?.items || data?.data || data;
            if (!Array.isArray(conversations)) {
                return ALL_CONVERSATIONS;
            }
            ALL_CONVERSATIONS.push(...conversations);
            if (total === -1) {
                total = data.total ?? 0;
            }
            if (ALL_CONVERSATIONS.length >= total) {
                log.debug('getAllConversations fetched all, total=%s', total);
                break;
            }
            if (conversations.length < pageSize) {
                log.debug('getAllConversations last batch, count=%s', conversations.length);
                break;
            }
            const lastDoc = conversations[conversations.length - 1];
            if (!Array.isArray(lastDoc.sort) || lastDoc.sort.length === 0) {
                throw new Error(`Conversation ${lastDoc.id} missing required "sort" field for pagination`);
            }
            startAfterDate = lastDoc.sort[0];
            await delay();
        } catch (err) {
            const status = err?.response?.status;
            if (status !== 404) {
                log.warn('getAllConversations url=%s, status=%s, error=%s', nextUrl, status, err.toString());
            }
            break;
        }
    }
    return ALL_CONVERSATIONS;
}

/**
 * Fetches all messages for a single conversation via pagination.
 * GET .../conversations/{conversationId}/messages?limit=20&lastMessageId=...
 * Embeds are not included for call/voicemail; use separate "get call recording" API if needed.
 */
async function getAllMessagesForConversation(conversationId) {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-04-15'
    };
    const baseUrl = `${HIGHLEVEL_API_URL || ''}`.replace(/\/+$/, '').replace('http:', 'https:');
    const path = `/conversations/${encodeURIComponent(conversationId)}/messages`;
    const limit = 20;
    const seenIds = new Set();
    const allMessages = [];
    let lastMessageId = '';

    while (true) {
        let url = `${baseUrl}${path}?limit=${limit}`;
        if (lastMessageId) {
            url += `&lastMessageId=${encodeURIComponent(lastMessageId)}`;
        }
        try {
            const response = await axios.get(url, { headers });
            const data = response?.data || {};
            const messagesWrapper = data?.messages;
            const list = Array.isArray(messagesWrapper?.messages)
                ? messagesWrapper.messages
                : Array.isArray(data?.items)
                    ? data.items
                    : Array.isArray(data?.data)
                        ? data.data
                        : [];
            for (const msg of list) {
                if (msg?.id && !seenIds.has(msg.id)) {
                    seenIds.add(msg.id);
                    allMessages.push(msg);
                }
            }
            const nextPage = messagesWrapper?.nextPage === true || messagesWrapper?.nextPage === 'true' ||
                data?.nextPage === true || data?.nextPage === 'true';
            const nextLastId = messagesWrapper?.lastMessageId ?? data?.lastMessageId ??
                (list.length > 0 ? list[list.length - 1]?.id : null);
            if (!nextPage || !nextLastId) {
                break;
            }
            lastMessageId = nextLastId;
            await delay(300);
        } catch (err) {
            const status = err?.response?.status;
            log.warn('getAllMessagesForConversation conversationId=%s, status=%s, error=%s', conversationId, status, err.toString());
            break;
        }
    }
    return allMessages;
}

async function getOpportunities() {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    let page = 1;
    const pageSize = 100;
    let opportunities = [];
    while (true) {
        const url = `${HIGHLEVEL_API_URL}/opportunities/search?location_id=${HIGHLEVEL_LOCATION_ID}&page=${page}&limit=${pageSize}`;
        try {
            const response = await axios.get(url, { headers });
            if (response.status === 200) {
                if (!Array.isArray(response?.data?.opportunities)) {
                    return opportunities;
                }
                if (response?.data?.opportunities?.length === 0) {
                    return opportunities;
                }
                opportunities = opportunities.concat(response?.data?.opportunities || []);
                log.debug('getOpportunities page=%s, total=%s', page, opportunities.length);
                await delay();
            } else {
                log.warn('getOpportunities url=%s, status=%s, error=%s', url, response.status, response.data);
                return opportunities;
            }
            page++;
        } catch (err) {
            log.warn('getOpportunities url=%s, error=%s', url, err.toString());
        }
    }
    return null;
}


export async function getCalendars(locationId = HIGHLEVEL_LOCATION_ID) {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const url = `${HIGHLEVEL_API_URL}/calendars/?locationId=${encodeURIComponent(locationId)}`
    try {
        log.debug('getCalendars url=%s', url);
        const response = await axios.get(url, { headers });
        const calendars = response?.data?.calendars || response?.data;
        if (!Array.isArray(calendars)) {
            return calendars;
        }
        const calendarsWithAppointments = await Promise.all(
            calendars.map(async (calendar) => {
                const appointments = await getAppointmentsForCalendar(calendar?.id, locationId);
                return { ...calendar, appointments };
            })
        );
        return calendarsWithAppointments;
    } catch (err) {
        const status = err?.response?.status;
        if (status !== 404) {
            log.warn('getCalendars url=%s, status=%s, error=%s', url, status, err.toString());
            return null;
        }
    }
    log.warn('getCalendars locationId=%s returned 404', locationId);
    return null;
}

async function getAppointmentsForCalendar(
    calendarId,
    locationId = HIGHLEVEL_LOCATION_ID,
    startTime,
    endTime
) {
    if (!calendarId) {
        return [];
    }
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const baseUrl = `${HIGHLEVEL_API_URL || ''}`.replace(/\/+$/, '');
    const params = new URLSearchParams();
    if (locationId) {
        params.set('locationId', locationId);
    }
    if (calendarId) {
        params.set('calendarId', calendarId);
    }
    if (!startTime || !endTime) {
        const now = Date.now();
        const defaultStart = new Date(now - 30 * 24 * 60 * 60 * 1000);
        const defaultEnd = new Date(now + 120 * 24 * 60 * 60 * 1000);
        startTime = startTime || defaultStart;
        endTime = endTime || defaultEnd;
    }
    params.set('startTime', startTime.getTime());
    params.set('endTime', endTime.getTime());
    const query = params.toString();
    const url = `${baseUrl}/calendars/events${query ? `?${query}` : ''}`;
    try {
        const response = await axios.get(url, { headers });
        const events = response?.data?.events || response?.data?.calendar_events || response?.data || [];
        return filterAppointmentEvents(events);
    } catch (err) {
        const status = err?.response?.status;
        if (status !== 404) {
            log.warn(
                'getAppointmentsForCalendar calendarId=%s, url=%s, status=%s, error=%s, response=%s',
                calendarId,
                url,
                status,
                err.toString(),
                JSON.stringify(err?.response?.data)
            );
            return [];
        }
    }
    log.warn('getAppointmentsForCalendar calendarId=%s returned 404 for all known URLs', calendarId);
    return [];
}

function filterAppointmentEvents(events) {
    if (!Array.isArray(events)) {
        return [];
    }
    return events;
    // return events.filter((event) => {
    //     const type = `${event?.type || event?.eventType || event?.calendarEventType || ''}`.toLowerCase();
    //     if (type) {
    //         return type.includes('appointment');
    //     }
    //     if (typeof event?.isAppointment === 'boolean') {
    //         return event.isAppointment;
    //     }
    //     return Boolean(event?.appointmentStatus);
    // });
}

function shapeOpportunityCustomFields(opportunity, customFieldMap) {
    if (!Array.isArray(opportunity?.customFields)) {
        return [];
    }
    opportunity?.customFields.forEach(customField => {
        const fieldDef = customFieldMap?.[customField.id];
        const key = fieldDef?.name || customField.id;
        customField.name = key;
    });
    return opportunity?.customFields || [];
}

function shapeContactCustomFields(contact, customFieldMap) {
    const values = contact?.customFields || {};
    if (!values || typeof values !== 'object') {
        return {};
    }
    return Object.entries(values).reduce((acc, [fieldId, fieldValue]) => {
        const fieldDef = customFieldMap?.[fieldId];
        const key = fieldDef?.name || fieldId;
        acc[key] = fieldValue;
        return acc;
    }, {});
}

async function getOpportunitiesWithCustomFields() {
    const [opportunities, customFieldMap] = await Promise.all([
        getOpportunities(),
        retrieveHighlevelCustomFields('opportunity')
    ]);
    if (!Array.isArray(opportunities)) {
        return { opportunities, customFieldMap: customFieldMap || {} };
    }
    const opportunitiesWithFields = opportunities.map((opportunity) => ({
        ...opportunity,
        customFields: shapeOpportunityCustomFields(opportunity, customFieldMap)
    }));
    return { opportunities: opportunitiesWithFields, opportunityCustomFieldMap: customFieldMap || {} };
}

export async function getContactsWithCustomFields(url) {
    const [contacts, customFieldMap] = await Promise.all([
        importAllHighLevelContacts(url),
        retrieveHighlevelCustomFields('contact')
    ]);
    if (!Array.isArray(contacts)) {
        return { contacts, customFieldMap: customFieldMap || {} };
    }
    const contactsWithFields = contacts.map((contact) => ({
        ...contact,
        customFieldsNamed: shapeContactCustomFields(contact, customFieldMap)
    }));
    return { contacts: contactsWithFields, customFieldMap: customFieldMap || {} };
}

export async function importAllHighLevelContacts(url, ALL_CONTACTS, customFieldMap) {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    ALL_CONTACTS = ALL_CONTACTS || [];
    customFieldMap = customFieldMap || await retrieveHighlevelCustomFields('contact');
    url = url || `${HIGHLEVEL_API_URL}/contacts?limit=100&locationId=${HIGHLEVEL_LOCATION_ID}`;
    url = url.replace('http:', 'https:');
    try {
        let response = await axios.get(url, { headers });
        if (response.status === 200) {
            const contacts = response?.data?.contacts;
            if (Array.isArray(contacts) && contacts.length > 0) {
                contacts.forEach(contact => {
                    (contact.customFields || []).forEach(customField => {
                        customField.name = customFieldMap[customField.id]?.name || customField.id;
                    });
                });
                ALL_CONTACTS.push(...contacts);
                log.debug('importAllHighLevelContacts totalLoaded=%s', ALL_CONTACTS.length);
                if (response?.data?.meta?.nextPageUrl) {
                    await delay();
                    await importAllHighLevelContacts(response?.data?.meta?.nextPageUrl, ALL_CONTACTS, customFieldMap);
                }
            }
        }
    } catch (err) {
        log.warn('importAllHighLevelContacts url=%s, error=%s', url, err.toString());
    }
    return {customFieldMap, contacts: ALL_CONTACTS};
}

const ENTITY_ALIASES = {
    contacts: 'contacts',
    users: 'users',
    opportunities: 'opportunities',
    calendars: 'calendars',
    conversations: 'conversations',
    customFields: 'customFields',
    notes: 'notes',
    all: 'all'
};

function parseEntitiesFromArgs(args) {
    const entities = [];
    for (const arg of args) {
        if (arg.startsWith('-')) {
            continue;
        }
        const normalized = arg.trim();
        if (!normalized) {
            continue;
        }
        const key = ENTITY_ALIASES[normalized];
        if (key) {
            entities.push(key);
        }
    }
    return entities;
}

function printUsage() {
    log.info('Usage: node highlevel.mjs [all|contacts|users|opportunities|calendars|conversations|customFields|notes]');
    log.info('Examples:');
    log.info('  node highlevel.mjs all');
    log.info('  node highlevel.mjs contacts users notes');
}

async function fetchAllEntities() {
    const { opportunities, opportunityCustomFieldMap } = await getOpportunitiesWithCustomFields();
    const { contacts, customFieldMap } = await importAllHighLevelContacts();
    const users = await getUsers();
    const calendars = await getCalendars();
    const conversations = await getAllConversations();
    for (let i = 0; i < conversations.length; i++) {
        const conv = conversations[i];
        if (!conv?.id) continue;
        try {
            conv.messages = await getAllMessagesForConversation(conv.id);
            log.debug('conversation %s: embedded %s messages', conv.id, conv.messages?.length ?? 0);
        } catch (err) {
            log.warn('embed messages conversationId=%s error=%s', conv.id, err.toString());
            conv.messages = [];
        }
        if (i < conversations.length - 1) {
            await delay(400);
        }
    }
    const stored = await storeGhlData({
        contacts,
        users,
        opportunities,
        calendars,
        conversations,
        customFields: [{ id: 'contact', contact: customFieldMap }, { id: 'opportunity', opportunity: opportunityCustomFieldMap }]
    });
    log.info('Stored GHL data %o', stored);
    const notesSummary = await importAllContactNotes();
    log.info('Stored GHL notes %o', notesSummary);
}

async function fetchSelectedEntities(entities) {
    const summary = {};

    if (entities.includes('opportunities') || entities.includes('customFields')) {
        const { opportunities, opportunityCustomFieldMap } = await getOpportunitiesWithCustomFields();
        if (entities.includes('opportunities')) {
            summary.opportunities = await upsertById('opportunities', opportunities);
        }
        if (entities.includes('customFields')) {
            summary.customFields = summary.customFields || [];
            summary.customFields.push({ id: 'opportunity', opportunity: opportunityCustomFieldMap });
        }
    }

    if (entities.includes('contacts') || entities.includes('customFields') || entities.includes('notes')) {
        const { contacts, customFieldMap } = await importAllHighLevelContacts();
        if (entities.includes('contacts')) {
            summary.contacts = await upsertById('contacts', contacts);
        }
        if (entities.includes('customFields')) {
            summary.customFields = summary.customFields || [];
            summary.customFields.push({ id: 'contact', contact: customFieldMap });
        }
        if (entities.includes('notes')) {
            summary.notes = await importAllContactNotes();
        }
    }

    if (entities.includes('users')) {
        const users = await getUsers();
        summary.users = await upsertById('users', users);
    }

    if (entities.includes('calendars')) {
        const calendars = await getCalendars();
        summary.calendars = await upsertById('calendars', calendars);
    }

    if (entities.includes('conversations')) {
        const conversations = await getAllConversations();
        summary.conversations = await upsertById('conversations', conversations);
    }

    if (Array.isArray(summary.customFields) && summary.customFields.length > 0) {
        summary.customFields = await upsertById('customfields', summary.customFields);
    }

    log.info('Stored GHL selected entities %o', summary);
}

async function run() {
    const args = process.argv.slice(2);
    if (args.includes('-h') || args.includes('--help')) {
        printUsage();
        return;
    }
    if (args.length === 0) {
        printUsage();
        return;
    }
    const entities = parseEntitiesFromArgs(args);
    if (entities.length === 0 || entities.includes('all')) {
        await fetchAllEntities();
        return;
    }
    await fetchSelectedEntities(entities);
}

setTimeout(async () => {
    try {
        await run();
    } catch (err) {
        log.error('Error storing GHL data %o', err);
    } finally {
        process.exit(0);
    }
}, 10)
