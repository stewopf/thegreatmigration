import axios from 'axios';
import logger from './src/log.mjs'
const log = logger(import.meta.url);

const HIGHLEVEL_API_URL = process.env.HIGHLEVEL_API_URL;
const HIGHLEVEL_API_KEY = process.env.HIGHLEVEL_API_KEY;
const HIGHLEVEL_LOCATION_ID = process.env.HIGHLEVEL_LOCATION_ID;
const HIGHLEVEL_DEFAULT_USER_ID = process.env.HIGHLEVEL_DEFAULT_USER_ID;

export async function syncNotes(notes) {
    for await (const note of (notes || [])) {
        if (note.wasChanged && note.foreignContactId) {
            if (note.foreignId) {
                await updateNote(note.foreignUserId, note.foreignContactId, note.foreignId, note.text);
            } else {
                await createNote(note.foreignUserId, note.foreignContactId, note.text);
            }
        }
    }
}
async function updateNote(userId, contactId, noteId, text) {
    log.debug('updateNode userId=%s, contactId=%s, noteId=%s, text=%s', userId, contactId, noteId, text);
    userId = userId || HIGHLEVEL_DEFAULT_USER_ID;
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };

    const url = `${HIGHLEVEL_API_URL}/contacts/${encodeURIComponent(contactId)}/notes/${noteId}`;
    try {
        const payload = { userId, body: text }
        const response = await axios.put(url, payload, { headers });
        console.log(response);

    } catch (err) {
        log.warn('updateNote url=%s, error=%s', url, err.toString());
    }

}
async function createNote(userId, contactId, text) {
    log.debug('createNote userId=%s, contactId=%s, text=%s', userId, contactId, text);

    userId = userId || HIGHLEVEL_DEFAULT_USER_ID;

    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };

    const url = `${HIGHLEVEL_API_URL}/contacts/${encodeURIComponent(contactId)}/notes`;
    try {
        const payload = { userId, body: text }
        const response = await axios.post(url, payload, { headers });
        console.log(response);

    } catch (err) {
        log.warn('createNote url=%s, error=%s', url, err.toString());
    }
}

function shapeNotes(notes) {
    return (notes || []).map(note => {
        return {
            text: note.body,
            createdAt: new Date(note.dateAdded),
            foreignUserId: note.userId,
            foreignContactId: note.contactId,
            foreignId: note.id
        }
    });
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
        return shapeNotes(response?.data?.notes);
    } catch (err) {
        log.warn('retrieveHighlevelCustomFields url=%s, error=%s', url, err.toString());
    }
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
export async function searchForAndAdoptHighlevelTenants(email, phone, refresh) {
    log.debug('searchForAndAdoptHighlevelTenants email=%s, phone=%s', email, phone);
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const payload = { locationId: HIGHLEVEL_LOCATION_ID, page: 1, pageLimit: 20 }
    let url = `${HIGHLEVEL_API_URL}/contacts/search`;
    if (email) {
        payload.query = email;
    }
    if (phone) {
        payload.query = phone;
    }
    try {
        let response = await axios.post(url, payload, { headers });
        if (response.status === 200) {
            const contacts = response?.data?.contacts;
            if (Array.isArray(contacts) && contacts.length > 0) {
                const fields = await retrieveHighlevelCustomFields('contact');
                return adoptHighlevelTenants(contacts, fields, refresh);
            }
        } else {
            log.debug('searchForAndAdoptHighlevelTenants email=%s, phone=%s, response.status=%s, headers=%o', email, phone, response.status, headers);
        }
    } catch (err) {
        log.warn('searchForAndAdoptHighlevelTenants email=%s, phone=%s, header=%o, error=%s', email, phone, headers, err.toString());
    }
    return null;
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

async function getOpportunities() {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const url = `${HIGHLEVEL_API_URL}/opportunities/search?location_id=${HIGHLEVEL_LOCATION_ID}`;
    try {
        const response = await axios.get(url, { headers });
        return response?.data?.opportunities;
    } catch (err) {
        log.warn('getOpportunities url=%s, error=%s', url, err.toString());
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
    const url =  `${baseUrl}/calendars/events${query ? `?${query}` : ''}`;
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
    return events.filter((event) => {
        console.log(event);
        const type = `${event?.type || event?.eventType || event?.calendarEventType || ''}`.toLowerCase();
        if (type) {
            return type.includes('appointment');
        }
        if (typeof event?.isAppointment === 'boolean') {
            return event.isAppointment;
        }
        return Boolean(event?.appointmentStatus);
    });
}

function shapeOpportunityCustomFields(opportunity, customFieldMap) {
    const values = opportunity?.customFields || {};
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
        customFieldsNamed: shapeOpportunityCustomFields(opportunity, customFieldMap)
    }));
    return { opportunities: opportunitiesWithFields, customFieldMap: customFieldMap || {} };
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

let totalLoaded = 0;
export async function importAllHighLevelContacts(url) {
    const headers = {
        Authorization: `Bearer ${HIGHLEVEL_API_KEY}`,
        'Content-Type': 'application/json',
        Version: '2021-07-28'
    };
    const ALL_CONTACTS = [];
    url = url || `${HIGHLEVEL_API_URL}/contacts?limit=100&locationId=${HIGHLEVEL_LOCATION_ID}`;
    url = url.replace('http:', 'https:');
    try {
        let response = await axios.get(url, { headers });
        if (response.status === 200) {
            const contacts = response?.data?.contacts;
            if (Array.isArray(contacts) && contacts.length > 0) {
                totalLoaded += contacts.length;
                console.log(totalLoaded);
                if (response?.data?.meta?.nextPageUrl) {
                    await delay();
                    await importAllHighLevelContacts(response?.data?.meta?.nextPageUrl);
                }
                ALL_CONTACTS.push(...contacts);
            }
        }
    } catch (err) {
        console.log(err);
    }
    return ALL_CONTACTS;
}

setTimeout(async () => {
    // console.log(await importAllHighLevelContacts());
    // const users = await getUsers();
    // const opportunities = await getOpportunitiesWithCustomFields();
    // console.log(users.length);
    console.log((await getCalendars())[0].appointments);
}, 2000);

