import { Client } from "@hubspot/api-client";
import { MongoClient, ObjectId } from "mongodb";

const DEFAULT_DB_NAME = "GoHighLevel";
const DEFAULT_COLLECTION = "conversations";
const DEFAULT_MAP_COLLECTION = "GHLHubspotIdMap";
const DEFAULT_CHECKPOINT_ID = "hubspot_conversations";
const DEFAULT_CONTACT_OBJECT_TYPE_ID = "contact";
const DEFAULT_OPPORTUNITY_OBJECT_TYPE_ID = "opportunity";

function buildHubspotClient(accessToken = process.env.HUBSPOT_ACCESS_TOKEN) {
    if (!accessToken) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    return new Client({ accessToken });
}

function toCamelCase(input) {
    return input.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
}

function parseCliArgs(argv = []) {
    const options = {};
    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        if (!arg.startsWith("--")) {
            continue;
        }
        const raw = arg.slice(2);
        if (!raw) {
            continue;
        }
        if (raw === "help") {
            options.help = true;
            continue;
        }
        if (raw.startsWith("no-")) {
            const key = toCamelCase(raw.slice(3));
            options[key] = false;
            continue;
        }
        const [keyPart, valuePart] = raw.split("=", 2);
        const key = toCamelCase(keyPart);
        if (valuePart !== undefined) {
            options[key] = valuePart;
            continue;
        }
        if (key === "dryRun" || key === "resume" || key === "deleteImportTag") {
            options[key] = true;
            continue;
        }
        const next = argv[i + 1];
        if (next && !next.startsWith("--")) {
            options[key] = next;
            i += 1;
        }
    }
    return options;
}

function printUsage() {
    console.log(`
Usage: node hubspot/conversations/migrateConversions.mjs [options]

Options:
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name (default: GoHighLevel)
  --collection <name>      Mongo conversations collection (default: conversations)
  --map-collection <name>  Mapping collection (default: GHLHubspotIdMap)
  --delete-import-tag [tag]  Delete engagements with import_tag (default: GHL_MIGRATION)
  --limit <number>         Max conversations to migrate
  --checkpoint-id <id>     Checkpoint document id
  --resume                 Resume from checkpoint (default)
  --no-resume              Start from the beginning
  --dry-run                Log actions without calling HubSpot
  --hubspot-access-token <token>  HubSpot private app token
  --help                   Show this help message
`);
}

async function getDb(mongoUri, dbName) {
    const resolvedUri = mongoUri || process.env.MONGO_URI || "mongodb://localhost:27017";
    const client = new MongoClient(resolvedUri);
    await client.connect();
    return { client, db: client.db(dbName) };
}

async function loadCheckpoint(db, checkpointId) {
    return db.collection("hubspot_transfer_checkpoints").findOne({ _id: checkpointId });
}

async function saveCheckpoint(db, checkpointId, data) {
    const payload = { ...data, updatedAt: new Date() };
    await db.collection("hubspot_transfer_checkpoints").updateOne(
        { _id: checkpointId },
        { $set: payload, $setOnInsert: { createdAt: new Date() } },
        { upsert: true }
    );
}

async function recordFailedMigration(db, {
    entityType,
    ghlId,
    reason,
    meta
} = {}) {
    if (!db || !entityType || !ghlId) {
        return;
    }
    await db.collection("hubspot_failed_migrations").updateOne(
        { entityType, ghlId },
        {
            $set: { reason, meta, updatedAt: new Date() },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
}

async function upsertGhlHubspotIdMap(db, { ghlId, hubspotId, objectTypeId } = {}) {
    if (!db || !ghlId || !hubspotId || !objectTypeId) {
        return;
    }
    await db.collection("GHLHubspotIdMap").updateOne(
        { ghlId, objectTypeId },
        {
            $set: { ghlId, hubspotId, objectTypeId, updatedAt: new Date() },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
}

function toTimestamp(value) {
    if (!value) {
        return Date.now();
    }
    if (value instanceof Date) {
        return value.getTime();
    }
    if (typeof value === "number") {
        return value < 1e12 ? value * 1000 : value;
    }
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? Date.now() : parsed;
}

function normalizeMessageText(message) {
    return (
        message?.body ||
        message?.text ||
        message?.message ||
        message?.content ||
        ""
    );
}
function normalizeMessageDirection(message) {
    const raw = String(message?.direction || message?.messageDirection || message?.status || "").toLowerCase();
    if (raw.includes("inbound") || raw.includes("incoming")) {
        return "inbound";
    }
    if (raw.includes("outbound") || raw.includes("outgoing")) {
        return "outbound";
    }
    if (message?.inbound === true) {
        return "inbound";
    }
    if (message?.outbound === true) {
        return "outbound";
    }
    return "unknown";
}

function normalizeDispositionKey(value) {
    return String(value || "")
        .trim()
        .toLowerCase()
        .replace(/[_\s-]+/g, " ");
}

function extractCallStatus(message) {
    return (
        message?.status ||
        message?.callStatus ||
        message?.meta?.call?.status ||
        message?.meta?.status ||
        message?.call?.status ||
        null
    );
}

function mapCallStatusToLabel(rawStatus) {
    const normalized = normalizeDispositionKey(rawStatus);
    if (!normalized) {
        return null;
    }
    if (normalized.includes("voicemail")) {
        return "Left voicemail";
    }
    if (normalized.includes("no answer") || normalized.includes("noanswer") || normalized.includes("unanswered")) {
        return "No answer";
    }
    if (normalized.includes("busy")) {
        return "Busy";
    }
    if (normalized.includes("wrong number")) {
        return "Wrong number";
    }
    if (normalized.includes("bad number")) {
        return "Bad number";
    }
    if (normalized.includes("connected") || normalized.includes("answered") || normalized.includes("completed")) {
        return "Connected";
    }
    if (normalized.includes("failed") || normalized.includes("missed") || normalized.includes("declined")) {
        return "No answer";
    }
    return null;
}

function buildDispositionLookup(options = []) {
    const lookup = {};
    options.forEach((option) => {
        const value = option?.value ? String(option.value) : "";
        const label = option?.label ? String(option.label) : "";
        if (value) {
            lookup[normalizeDispositionKey(value)] = value;
        }
        if (label) {
            lookup[normalizeDispositionKey(label)] = value || label;
        }
    });
    return lookup;
}

async function getCallDispositionLookup(hubspotClient) {
    if (!hubspotClient) {
        return null;
    }
    try {
        const property = await hubspotClient.crm.properties.coreApi.getByName("calls", "hs_call_disposition");
        const options = Array.isArray(property?.options) ? property.options : [];
        return buildDispositionLookup(options);
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        console.warn("failed to load call dispositions", status || "", err?.message || err);
        return null;
    }
}

function resolveCallDispositionValue(message, dispositionLookup) {
    if (!dispositionLookup) {
        return null;
    }
    const rawStatus = extractCallStatus(message);
    if (!rawStatus) {
        return null;
    }
    const rawKey = normalizeDispositionKey(rawStatus);
    if (dispositionLookup[rawKey]) {
        return dispositionLookup[rawKey];
    }
    const label = mapCallStatusToLabel(rawStatus);
    if (!label) {
        return null;
    }
    const labelKey = normalizeDispositionKey(label);
    return dispositionLookup[labelKey] || null;
}

function normalizeOwnerId(userRecord) {
    const raw = userRecord?.hubSpot?.id ?? userRecord?.hubSpot?.ownerId ?? userRecord?.hubSpot?.userId;
    if (raw === undefined || raw === null) {
        return null;
    }
    const trimmed = String(raw).trim();
    if (!/^\d+$/.test(trimmed)) {
        return null;
    }
    return trimmed;
}

async function findAssignedToUser(db, message, conversation) {
    if (!db) {
        return null;
    }
    const userId = message?.userId || conversation?.assignedTo;
    if (!userId) {
        return null;
    }
    return db.collection("users").findOne({ id: userId });
}

function isSmsMessage(message) {
    const raw = String(message?.type || message?.channel || message?.messageType || "").toLowerCase();
    if (raw.includes("sms") || raw.includes("text")) {
        return true;
    }
    const typeNum = typeof message?.type === "number" ? message.type : null;
    const msgType = String(message?.messageType || "").toUpperCase();
    return typeNum === 2 || msgType === "TYPE_SMS";
}

function isEmailMessage(message) {
    const raw = String(message?.type || message?.channel || message?.messageType || "").toLowerCase();
    if (raw.includes("email") || raw.includes("mail")) {
        return true;
    }
    const typeNum = typeof message?.type === "number" ? message.type : null;
    const msgType = String(message?.messageType || "").toUpperCase();
    return typeNum === 3 || msgType === "TYPE_EMAIL";
}

function isCallMessage(message) {
    const raw = String(message?.type || message?.channel || message?.messageType || "").toLowerCase();
    if (raw.includes("call") || raw.includes("voicemail")) {
        return true;
    }
    const typeNum = typeof message?.type === "number" ? message.type : null;
    const msgType = String(message?.messageType || "").toUpperCase();
    return typeNum === 1 || msgType === "TYPE_CALL";
}

function isOpportunityActivityMessage(message) {
    const msgType = String(message?.messageType || "").toUpperCase();
    return msgType === "TYPE_ACTIVITY_OPPORTUNITY";
}

function isAppointmentActivityMessage(message) {
    const msgType = String(message?.messageType || "").toUpperCase();
    return msgType === "TYPE_ACTIVITY_APPOINTMENT";
}

function isContactActivityMessage(message) {
    const msgType = String(message?.messageType || "").toUpperCase();
    return msgType === "TYPE_ACTIVITY_CONTACT";
}

function isInternalCommentMessage(message) {
    const msgType = String(message?.messageType || "").toUpperCase();
    return msgType === "TYPE_INTERNAL_COMMENT";
}

function buildAttachmentHtml(message) {
    const attachments = Array.isArray(message?.attachments) ? message.attachments : [];
    const items = attachments
        .filter(Boolean)
        .map((url) => `<li><a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a></li>`);
    if (items.length === 0) {
        return "";
    }
    return `<p><strong>Attachments</strong></p><ul>${items.join("")}</ul>`;
}

function buildNoteProperties(message, { ownerId } = {}) {
    const direction = normalizeMessageDirection(message);
    const text = normalizeMessageText(message);
    const label = direction === "unknown" ? "SMS" : `SMS (${direction})`;
    const attachmentHtml = buildAttachmentHtml(message);
    const body = `<p><strong>${label}</strong></p><p>${text}</p>${attachmentHtml}`;
    const properties = {
        hs_note_body: body,
        hs_timestamp: toTimestamp(message?.lastMessageAt || message?.dateAdded || message?.createdAt || message?.timestamp),
        import_tag: "GHL_MIGRATION",
        ghl_id: message?.id,
        ghl_conversation_id: message?.conversationId,
        ghl_channel: message?.channel ? message.channel.toLowerCase() : null,
        ghl_direction: direction === "unknown" ? "mixed" : direction,
        ghl_conversation_updated_at: toTimestamp(message?.updatedAt || message?.updated),
        ghl_conversation_created_at: toTimestamp(message?.createdAt || message?.timestamp),
        ghl_source: message?.source ? message.source.toLowerCase() : null,
        ghl_direction: message?.direction ? message.direction.toLowerCase() : null,
    };
    if (ownerId) {
        properties.hubspot_owner_id = ownerId;
    }
    return properties;
}

function buildActivityNoteProperties(message, { ownerId } = {}) {
    const direction = normalizeMessageDirection(message);
    const lines = [
        `<p><strong>Opportunity Activity</strong></p>`,
        `<p><strong>Body:</strong> ${normalizeMessageText(message) || ""}</p>`,
        `<p><strong>Source:</strong> ${message?.source || ""}</p>`,
        `<p><strong>From:</strong> ${message?.from || ""}</p>`,
        `<p><strong>To:</strong> ${message?.to || ""}</p>`,
        `<p><strong>Direction:</strong> ${direction}</p>`,
        `<p><strong>Status:</strong> ${message?.status || ""}</p>`
    ];
    const attachmentHtml = buildAttachmentHtml(message);
    if (attachmentHtml) {
        lines.push(attachmentHtml);
    }
    const properties = {
        hs_note_body: lines.join(""),
        hs_timestamp: toTimestamp(message?.dateAdded || message?.createdAt || message?.timestamp),
        import_tag: "GHL_MIGRATION",
        ghl_id: message?.id
    };
    if (ownerId) {
        properties.hubspot_owner_id = ownerId;
    }
    return properties;
}

function buildEmailProperties(message, { ownerId } = {}) {
    const direction = normalizeMessageDirection(message);
    const subject = message?.subject || message?.meta?.email?.subject || message?.title || "Email";
    const body = message?.contentType === "text/html" ? (message?.body || "") : normalizeMessageText(message);
    const toList = Array.isArray(message?.to) ? message.to : [];
    const headerTo = toList
        .map((entry) => {
            if (!entry) {
                return null;
            }
            if (typeof entry === "string") {
                return { email: entry };
            }
            const email = entry?.email || entry?.address || entry?.value || entry?.to || null;
            const firstName = entry?.firstName || entry?.first_name || entry?.givenName || null;
            const lastName = entry?.lastName || entry?.last_name || entry?.familyName || null;
            if (!email) {
                return null;
            }
            const recipient = { email };
            if (firstName) {
                recipient.firstName = firstName;
            }
            if (lastName) {
                recipient.lastName = lastName;
            }
            return recipient;
        })
        .filter(Boolean);
    const headerFromEmail = message?.from || message?.fromEmail || message?.meta?.email?.from;
    const headerFrom = headerFromEmail ? { email: headerFromEmail } : null;
    const headers = {};
    if (headerTo.length > 0) {
        headers.to = headerTo;
    }
    if (headerFrom) {
        headers.from = headerFrom;
    }
    if (subject) {
        headers.subject = subject;
    }
    const emailDirection = direction === "inbound"
        ? "INCOMING_EMAIL"
        : direction === "outbound"
            ? "OUTGOING"
            : "INCOMING_EMAIL";
    const properties = {
        hs_email_subject: subject,
        hs_email_text: message?.contentType === "text/html" ? undefined : body,
        hs_email_html: message?.contentType === "text/html" ? body : undefined,
        hs_email_direction: emailDirection,
        hs_timestamp: toTimestamp(message?.dateAdded || message?.createdAt || message?.timestamp),
        import_tag: "GHL_MIGRATION",
        ghl_id: message?.id,
        ghl_source: message?.source ? message.source.toLowerCase() : null,
        ghl_status: message?.status ? message.status.toLowerCase() : null,
        ghl_direction: message?.direction ? message.direction.toLowerCase() : null,
        ghl_conversation_id: message?.conversationId,
        ghl_conversation_updated_at: toTimestamp(message?.updatedAt || message?.updated),
    };
    if (Object.keys(headers).length > 0) {
        properties.hs_email_headers = JSON.stringify(headers);
    }
    if (ownerId) {
        properties.hubspot_owner_id = ownerId;
    }
    return properties;
}

function buildCallProperties(message, { ownerId, dispositionValue } = {}) {
    const direction = normalizeMessageDirection(message);
    let body = normalizeMessageText(message);
    const voicemailUrl = message?.voicemailUrl || message?.meta?.call?.voicemailUrl;
    const recordingUrl = message?.recordingUrl || message?.meta?.call?.recordingUrl;
    const urlLines = [];
    if (voicemailUrl) {
        urlLines.push(`Voicemail: ${voicemailUrl}`);
    }
    if (recordingUrl) {
        urlLines.push(`Recording: ${recordingUrl}`);
    }
    if (urlLines.length > 0) {
        body = [body, urlLines.join("\n")].filter(Boolean).join("\n");
    }
    const duration = message?.duration || message?.callDuration || message?.meta?.call?.duration;
    const callDirection = direction === "inbound"
        ? "INBOUND"
        : direction === "outbound"
            ? "OUTBOUND"
            : "INBOUND";
    const properties = {
        hs_call_body: body,
        hs_call_direction: callDirection,
        ghl_conversation_id: message?.conversationId,
        hs_timestamp: toTimestamp(message?.dateAdded || message?.createdAt || message?.timestamp),
        ghl_conversation_updated_at: toTimestamp(message?.updatedAt || message?.updated),
        import_tag: "GHL_MIGRATION",
        ghl_id: message?.id,
        ghl_source: message?.source ? message.source.toLowerCase() : null,
    };
    if (dispositionValue) {
        properties.hs_call_disposition = dispositionValue;
    }
    if (ownerId) {
        properties.hubspot_owner_id = ownerId;
    }
    if (duration != null) {
        properties.hs_call_duration = duration;
    }
    return properties;
}

async function getAssociationType(hubspotClient, fromObject, toObject, label) {
    const response = await hubspotClient.crm.associations.v4.schema.definitionsApi.getAll(fromObject, toObject);
    const results = Array.isArray(response?.results) ? response.results : response;
    let match = results?.find((item) => (item?.category || item?.associationCategory) === "HUBSPOT_DEFINED");
    if (label) {
        match = results?.find((item) => String(item?.label || "").toLowerCase() === label.toLowerCase()) || match;
    }
    if (!match?.typeId) {
        throw new Error(`Unable to resolve association type for ${fromObject} -> ${toObject}`);
    }
    return {
        associationCategory: match?.category || match?.associationCategory || "HUBSPOT_DEFINED",
        associationTypeId: match.typeId
    };
}

async function deleteEngagementsByImportTag(
    hubspotClient,
    objectType,
    {
        importTag = "GHL_MIGRATION",
        dryRun = false,
        db,
        mapCollection = DEFAULT_MAP_COLLECTION,
        mapObjectTypeId
    } = {}
) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    const results = { scanned: 0, deleted: 0, mapDeleted: 0 };
    const shouldDeleteMap = objectType === "notes" && db && !dryRun;
    const resolvedMapObjectTypeId = mapObjectTypeId || (objectType === "notes" ? "note" : objectType);
    let after;
    do {
        const searchRequest = {
            filterGroups: [
                {
                    filters: [
                        {
                            propertyName: "import_tag",
                            operator: "EQ",
                            value: importTag
                        }
                    ]
                }
            ],
            limit: 100,
            after
        };
        const searchResponse = await hubspotClient.crm.objects.searchApi.doSearch(objectType, searchRequest);
        const records = Array.isArray(searchResponse?.results) ? searchResponse.results : [];
        results.scanned += records.length;
        if (!dryRun && records.length > 0) {
            const inputs = records.map((record) => ({ id: record.id }));
            const ids = records.map((record) => record.id).filter(Boolean);
            const batchApi = hubspotClient.crm.objects.batchApi;
            if (batchApi?.archive) {
                await batchApi.archive(objectType, { inputs });
                results.deleted += inputs.length;
            } else {
                for (const record of records) {
                    await hubspotClient.crm.objects.basicApi.archive(objectType, record.id);
                    results.deleted += 1;
                }
            }
            if (shouldDeleteMap && ids.length > 0) {
                const deleteResult = await db.collection(mapCollection).deleteMany({
                    hubspotId: { $in: ids },
                    objectTypeId: resolvedMapObjectTypeId
                });
                results.mapDeleted += deleteResult?.deletedCount || 0;
            }
        }
        after = searchResponse?.paging?.next?.after;
    } while (after);
    return results;
}

async function getNoteToContactAssociationType(hubspotClient) {
    return getAssociationType(hubspotClient, "notes", "contacts");
}

async function getNoteToDealAssociationType(hubspotClient) {
    return getAssociationType(hubspotClient, "notes", "deals");
}

export async function migrateConversationsToHubspot({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = DEFAULT_DB_NAME,
    collectionName = DEFAULT_COLLECTION,
    mapCollection = DEFAULT_MAP_COLLECTION,
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    checkpointId = DEFAULT_CHECKPOINT_ID,
    resume = true,
    dryRun = false,
    limit
} = {}) {
    if (!hubspotAccessToken && !dryRun) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    const { client, db } = await getDb(mongoUri, dbName);
    try {
        const collection = db.collection(collectionName);
        const mapColl = db.collection(mapCollection);
        let query = {};
        let startConversationId = null;
        let startMessageId = null;
        if (resume) {
            const checkpoint = await loadCheckpoint(db, checkpointId);
            if (checkpoint?.lastConversationId) {
                startConversationId = checkpoint.lastConversationId;
                startMessageId = checkpoint.lastMessageId || null;
                query = { _id: { $gte: new ObjectId(startConversationId) } };
            }
        }
        let cursor = collection.find(query).sort({ _id: 1 });
        if (limit) {
            cursor = cursor.limit(Number(limit));
        }

        const summary = {
            processedConversations: 0,
            processedMessages: 0,
            createdNotes: 0,
            createdActivityNotes: 0,
            createdEmails: 0,
            createdCalls: 0,
            skippedNonSms: 0,
            skippedMissingMappings: 0,
            errors: 0
        };

        const hubspotClient = dryRun ? null : buildHubspotClient(hubspotAccessToken);
        let contactAssociationType = null;
        let dealAssociationType = null;
        let emailContactAssociationType = null;
        let emailDealAssociationType = null;
        let callContactAssociationType = null;
        let callDealAssociationType = null;
        let callDispositionLookup = null;
        if (!dryRun) {
            contactAssociationType = await getNoteToContactAssociationType(hubspotClient);
            dealAssociationType = await getNoteToDealAssociationType(hubspotClient);
            emailContactAssociationType = await getAssociationType(hubspotClient, "emails", "contacts");
            emailDealAssociationType = await getAssociationType(hubspotClient, "emails", "deals");
            callContactAssociationType = await getAssociationType(hubspotClient, "calls", "contacts");
            callDealAssociationType = await getAssociationType(hubspotClient, "calls", "deals");
            callDispositionLookup = await getCallDispositionLookup(hubspotClient);
        }

        let lastProgressAt = Date.now();
        const progressIntervalMs = 5000;
        while (await cursor.hasNext()) {
            const conversation = await cursor.next();
            if (!conversation) {
                break;
            }
            summary.processedConversations += 1;
            const conversationId = conversation?._id ? String(conversation._id) : null;
            const messages = Array.isArray(conversation?.messages) ? conversation.messages : [];

            let startIndex = 0;
            if (startConversationId && conversationId === startConversationId && startMessageId) {
                startIndex = Math.max(0, messages.findIndex((msg) => msg?.id === startMessageId));
            }

            const contactId = conversation?.contactId || conversation?.contact?.id;
            const opportunityId = conversation?.opportunityId || conversation?.opportunity?.id;
            let hubspotContactId = null;
            let hubspotDealId = null;
            if (contactId) {
                const mapping = await mapColl.findOne({ ghlId: contactId, objectTypeId: DEFAULT_CONTACT_OBJECT_TYPE_ID });
                hubspotContactId = mapping?.hubspotId || null;
            }
            if (opportunityId) {
                const mapping = await mapColl.findOne({ ghlId: opportunityId, objectTypeId: DEFAULT_OPPORTUNITY_OBJECT_TYPE_ID });
                hubspotDealId = mapping?.hubspotId || null;
            }
            const now = Date.now();
            if (now - lastProgressAt >= progressIntervalMs) {
                console.log(`processing conversation: ${summary.processedConversations}: ${conversationId}`);
                lastProgressAt = now;
            }
            for (let i = startIndex; i < messages.length; i += 1) {
                const message = messages[i];
                if (!message) {
                    continue;
                }
                summary.processedMessages += 1;
                if (Date.now() - lastProgressAt >= progressIntervalMs) {
                    console.log(`processed messages: ${summary.processedMessages}`);
                    lastProgressAt = Date.now();
                }
                const isActivity = isOpportunityActivityMessage(message)
                    || isAppointmentActivityMessage(message)
                    || isContactActivityMessage(message)
                    || isInternalCommentMessage(message);
                const isSms = isSmsMessage(message);
                const isEmail = isEmailMessage(message);
                const isCall = isCallMessage(message);
                if (!isActivity && !isSms && !isEmail && !isCall) {
                    console.log(`skipped non sms: ${JSON.stringify(message, null, 4)}`);
                    summary.skippedNonSms += 1;
                    continue;
                }

                if (!hubspotContactId && !hubspotDealId) {
                    summary.skippedMissingMappings += 1;
                    continue;
                }

                const associations = [];
                if (hubspotContactId) {
                    associations.push({
                        to: { id: hubspotContactId }
                    });
                }
                if (hubspotDealId) {
                    associations.push({
                        to: { id: hubspotDealId }
                    });
                }

                const assignedToUser = await findAssignedToUser(db, message, conversation);
                const ownerId = normalizeOwnerId(assignedToUser);
                if (assignedToUser && (Date.now() - lastProgressAt >= progressIntervalMs)) {
                    console.log(assignedToUser.email, assignedToUser.firstName, assignedToUser.lastName);
                    lastProgressAt = Date.now();
                }

                if (isActivity) {
                    const properties = buildActivityNoteProperties(message, { ownerId });
                    const activityAssociations = associations.map((association) => ({
                        ...association,
                        types: [association.to.id === hubspotContactId ? contactAssociationType : dealAssociationType]
                    }));
                    if (dryRun) {
                        console.log("[dry-run] create opportunity activity note", {
                            conversationId,
                            messageId: message?.id
                        });
                        summary.createdActivityNotes += 1;
                    } else {
                        try {
                            const created = await hubspotClient.crm.objects.notes.basicApi.create({
                                properties,
                                associations: activityAssociations
                            });
                            await upsertGhlHubspotIdMap(db, {
                                ghlId: message?.id,
                                hubspotId: created?.id,
                                objectTypeId: "note"
                            });
                            summary.createdActivityNotes += 1;
                        } catch (err) {
                            const status = err?.code || err?.response?.statusCode || err?.response?.status;
                            console.error("failed to create activity note", status || "", err?.message || err);
                            await recordFailedMigration(db, {
                                entityType: "conversation_message",
                                ghlId: message?.id,
                                reason: err?.message || String(err),
                                meta: {
                                    conversationId,
                                    kind: "activity",
                                    messageType: message?.messageType || message?.type || message?.channel
                                }
                            });
                            summary.errors += 1;
                        }
                    }
                } else if (isSms) {
                    const properties = buildNoteProperties(message, { ownerId });
                    const smsAssociations = associations.map((association) => ({
                        ...association,
                        types: [association.to.id === hubspotContactId ? contactAssociationType : dealAssociationType]
                    }));
                    if (dryRun) {
                        console.log("[dry-run] create sms note", {
                            conversationId,
                            messageId: message?.id,
                            direction: normalizeMessageDirection(message)
                        });
                        summary.createdNotes += 1;
                    } else {
                        try {
                        const created = await hubspotClient.crm.objects.notes.basicApi.create({
                                properties,
                                associations: smsAssociations
                            });
                        await upsertGhlHubspotIdMap(db, {
                            ghlId: message?.id,
                            hubspotId: created?.id,
                            objectTypeId: "note"
                        });
                            summary.createdNotes += 1;
                        } catch (err) {
                            const status = err?.code || err?.response?.statusCode || err?.response?.status;
                            console.error("failed to create sms note", status || "", err?.message || err);
                            await recordFailedMigration(db, {
                                entityType: "conversation_message",
                                ghlId: message?.id,
                                reason: err?.message || String(err),
                                meta: {
                                    conversationId,
                                    kind: "sms",
                                    messageType: message?.messageType || message?.type || message?.channel
                                }
                            });
                            summary.errors += 1;
                        }
                    }
                } else if (isEmail) {
                    const properties = buildEmailProperties(message, { ownerId });
                    const emailAssociations = associations.map((association) => ({
                        ...association,
                        types: [association.to.id === hubspotContactId ? emailContactAssociationType : emailDealAssociationType]
                    }));
                    if (dryRun) {
                        console.log("[dry-run] create email", {
                            conversationId,
                            messageId: message?.id,
                            direction: normalizeMessageDirection(message)
                        });
                        summary.createdEmails += 1;
                    } else {
                        try {
                            const created = await hubspotClient.crm.objects.emails.basicApi.create({
                                properties,
                                associations: emailAssociations
                            });
                            await upsertGhlHubspotIdMap(db, {
                                ghlId: message?.id,
                                hubspotId: created?.id,
                                objectTypeId: "email"
                            });
                            summary.createdEmails += 1;
                        } catch (err) {
                            const status = err?.code || err?.response?.statusCode || err?.response?.status;
                            console.error("failed to create email", status || "", err?.message || err);
                            await recordFailedMigration(db, {
                                entityType: "conversation_message",
                                ghlId: message?.id,
                                reason: err?.message || String(err),
                                meta: {
                                    conversationId,
                                    kind: "email",
                                    messageType: message?.messageType || message?.type || message?.channel
                                }
                            });
                            summary.errors += 1;
                        }
                    }
                } else if (isCall) {
                    const callDispositionValue = resolveCallDispositionValue(message, callDispositionLookup);
                    const properties = buildCallProperties(message, { ownerId, dispositionValue: callDispositionValue });
                    const callAssociations = associations.map((association) => ({
                        ...association,
                        types: [association.to.id === hubspotContactId ? callContactAssociationType : callDealAssociationType]
                    }));
                    if (dryRun) {
                        console.log("[dry-run] create call", {
                            conversationId,
                            messageId: message?.id,
                            direction: normalizeMessageDirection(message)
                        });
                        summary.createdCalls += 1;
                    } else {
                        try {
                            const created = await hubspotClient.crm.objects.calls.basicApi.create({
                                properties,
                                associations: callAssociations
                            });
                            await upsertGhlHubspotIdMap(db, {
                                ghlId: message?.id,
                                hubspotId: created?.id,
                                objectTypeId: "call"
                            });
                            summary.createdCalls += 1;
                        } catch (err) {
                            const status = err?.code || err?.response?.statusCode || err?.response?.status;
                            console.error("failed to create call", status || "", err?.message || err);
                            await recordFailedMigration(db, {
                                entityType: "conversation_message",
                                ghlId: message?.id,
                                reason: err?.message || String(err),
                                meta: {
                                    conversationId,
                                    kind: "call",
                                    messageType: message?.messageType || message?.type || message?.channel
                                }
                            });
                            summary.errors += 1;
                        }
                    }
                }
                if (conversationId) {
                    await saveCheckpoint(db, checkpointId, {
                        lastConversationId: conversationId,
                        lastMessageId: message?.id || null
                    });
                }
            }
        }
        return summary;
    } finally {
        await client.close();
    }
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
    const cli = parseCliArgs(process.argv.slice(2));
    if (cli.help || process.argv.length <= 2) {
        printUsage();
        process.exit(0);
    }
    const parsedLimit = cli.limit ? Number(cli.limit) : undefined;
    const limit = Number.isInteger(parsedLimit) && parsedLimit > 0 ? parsedLimit : undefined;

    const run = async () => {
        if (cli.deleteImportTag !== undefined) {
            const importTag = cli.deleteImportTag === true ? "GHL_MIGRATION" : cli.deleteImportTag;
            const hubspotClient = buildHubspotClient(cli.hubspotAccessToken);
            const { client, db } = await getDb(cli.mongoUri || process.env.MONGO_URI, cli.dbName || DEFAULT_DB_NAME);
            try {
                const notes = await deleteEngagementsByImportTag(hubspotClient, "notes", {
                    importTag,
                    dryRun: cli.dryRun,
                    db,
                    mapCollection: cli.mapCollection || DEFAULT_MAP_COLLECTION,
                    mapObjectTypeId: "note"
                });
                const emails = await deleteEngagementsByImportTag(hubspotClient, "emails", { importTag, dryRun: cli.dryRun });
                const calls = await deleteEngagementsByImportTag(hubspotClient, "calls", { importTag, dryRun: cli.dryRun });
                console.log("delete complete:", { notes, emails, calls });
            } finally {
                await client.close();
            }
            return;
        }
        const summary = await migrateConversationsToHubspot({
            mongoUri: cli.mongoUri,
            dbName: cli.dbName,
            collectionName: cli.collection,
            mapCollection: cli.mapCollection,
            hubspotAccessToken: cli.hubspotAccessToken,
            checkpointId: cli.checkpointId,
            resume: cli.resume !== false,
            dryRun: cli.dryRun,
            limit
        });
        console.log("conversation migration complete:", summary);
    };

    run().catch((err) => {
        console.error("migrateConversations failed:", err?.message || err);
        process.exit(1);
    });
}
