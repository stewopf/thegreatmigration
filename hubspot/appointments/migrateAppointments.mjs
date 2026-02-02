import { Client } from "@hubspot/api-client";
import { MongoClient, ObjectId } from "mongodb";

const DEFAULT_DB_NAME = "GoHighLevel";
const DEFAULT_COLLECTION = "appointments";
const DEFAULT_MAP_COLLECTION = "GHLHubspotIdMap";
const DEFAULT_CHECKPOINT_ID = "hubspot_appointments";
const DEFAULT_OBJECT_TYPE_ID = "meetings";
const DEFAULT_APPOINTMENT_OBJECT_TYPE_ID = "meeting";

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
Usage: node hubspot/appointments/migrateAppointments.mjs [options]

Options:
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name (default: GoHighLevel)
  --collection <name>      Mongo appointments collection (default: appointments)
  --map-collection <name>  Mapping collection (default: GHLHubspotIdMap)
  --object-type-id <id>    HubSpot meetings object type id (default: meetings)
  --delete-import-tag [tag]  Delete appointments with import_tag (default: GHL_MIGRATION)
  --limit <number>         Max appointments to migrate
  --checkpoint-id <id>     Checkpoint document id
  --resume                 Resume from checkpoint (default)
  --no-resume              Start from the beginning
  --dry-run                Log actions without calling HubSpot
  --hubspot-access-token <token>  HubSpot private app token
  --help                   Show this help message
`);
}

async function getDb(mongoUri, dbName) {
    if (!mongoUri) {
        throw new Error("MONGO_URI is not set");
    }
    const client = new MongoClient(mongoUri);
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
    reason
} = {}) {
    if (!db || !entityType || !ghlId) {
        return;
    }
    await db.collection("hubspot_failed_migrations").updateOne(
        { entityType, ghlId },
        {
            $set: { reason, updatedAt: new Date() },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
}

async function upsertGhlHubspotIdMap(db, { ghlId, hubspotId, objectTypeId } = {}) {
    if (!ghlId || !hubspotId || !objectTypeId) {
        return;
    }
    await db.collection(DEFAULT_MAP_COLLECTION).updateOne(
        { ghlId, objectTypeId },
        {
            $set: { ghlId, hubspotId, objectTypeId, updatedAt: new Date() },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
}

function normalizePropertyValue(value) {
    if (value === undefined || value === null || value === "") {
        return undefined;
    }
    if (typeof value === "boolean") {
        return value ? "true" : "false";
    }
    return value;
}

function toTimestamp(value) {
    if (!value) {
        return undefined;
    }
    if (value instanceof Date) {
        return value.getTime();
    }
    if (typeof value === "number") {
        return value < 1e12 ? value * 1000 : value;
    }
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? undefined : parsed;
}

function normalizeMeetingOutcome(value) {
    if (!value) {
        return undefined;
    }
    const raw = String(value).trim().toLowerCase();
    if (!raw) {
        return undefined;
    }
    if (["showed", "show", "attended", "kept", "completed", "done", "held"].includes(raw)) {
        return "COMPLETED";
    }
    if (["no show", "noshow", "no_show", "no-show"].includes(raw)) {
        return "NO_SHOW";
    }
    if (["canceled", "cancelled", "canceled_by_contact", "cancelled_by_contact", "canceled by contact"].includes(raw)) {
        return "CANCELED";
    }
    if (["rescheduled", "reschedule"].includes(raw)) {
        return "RESCHEDULED";
    }
    if (["scheduled", "confirmed", "booked", "pending"].includes(raw)) {
        return "SCHEDULED";
    }
    return undefined;
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

function buildAppointmentProperties(appointment, { ownerId } = {}) {
    const title =
        normalizePropertyValue(appointment?.title) ||
        normalizePropertyValue(appointment?.name) ||
        normalizePropertyValue(appointment?.appointmentTitle) ||
        `GHL Appointment ${appointment?.id || ""}`.trim();
    const startTime = toTimestamp(appointment?.startTime || appointment?.startAt || appointment?.startDate);
    const endTime = toTimestamp(appointment?.endTime || appointment?.endAt || appointment?.endDate);
    const body = normalizePropertyValue(appointment?.notes || appointment?.description);
    const address = normalizePropertyValue(appointment?.address);
    const outcome = normalizeMeetingOutcome(appointment?.appointmentStatus || appointment?.status);
    const properties = {
        hs_meeting_title: title,
        hs_meeting_body: body,
        hs_meeting_start_time: startTime,
        hs_meeting_end_time: endTime,
        hs_meeting_location: address,
        hs_meeting_outcome: outcome,
        hs_timestamp: toTimestamp(appointment?.dateAdded || appointment?.createdAt || startTime),
        import_tag: "GHL_MIGRATION",
        ghl_id: appointment?.id
    };
    if (ownerId) {
        properties.hubspot_owner_id = ownerId;
    }
    return properties;
}

async function findAssignedToUser(db, appointment) {
    if (!db || !appointment) {
        return null;
    }
    const userId = appointment?.userId
        || appointment?.assignedTo
        || appointment?.assignedUserId
        || appointment?.calendarUserId
        || appointment?.staffId;
    if (!userId) {
        return null;
    }
    return db.collection("users").findOne({ id: userId });
}

async function getDefaultAssociationType(hubspotClient, fromObject, toObject) {
    const response = await hubspotClient.crm.associations.v4.schema.definitionsApi.getAll(fromObject, toObject);
    const results = Array.isArray(response?.results) ? response.results : response;
    const match = results?.find((item) => (item?.category || item?.associationCategory) === "HUBSPOT_DEFINED");
    if (!match?.typeId) {
        return null;
    }
    return {
        associationCategory: match?.category || match?.associationCategory || "HUBSPOT_DEFINED",
        associationTypeId: match.typeId
    };
}

async function deleteAppointmentsByImportTag(
    hubspotClient,
    objectTypeId,
    { importTag = "GHL_MIGRATION", dryRun = false } = {}
) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    const results = { scanned: 0, deleted: 0 };
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
        const searchResponse = await hubspotClient.crm.objects.searchApi.doSearch(objectTypeId, searchRequest);
        const records = Array.isArray(searchResponse?.results) ? searchResponse.results : [];
        results.scanned += records.length;
        if (!dryRun && records.length > 0) {
            const inputs = records.map((record) => ({ id: record.id }));
            const batchApi = hubspotClient.crm.objects.batchApi;
            if (batchApi?.archive) {
                await batchApi.archive(objectTypeId, { inputs });
                results.deleted += inputs.length;
            } else {
                for (const record of records) {
                    await hubspotClient.crm.objects.basicApi.archive(objectTypeId, record.id);
                    results.deleted += 1;
                }
            }
        }
        after = searchResponse?.paging?.next?.after;
    } while (after);
    return results;
}

export async function migrateAppointmentsToHubspot({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = DEFAULT_DB_NAME,
    collectionName = DEFAULT_COLLECTION,
    mapCollection = DEFAULT_MAP_COLLECTION,
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    checkpointId = DEFAULT_CHECKPOINT_ID,
    resume = true,
    dryRun = false,
    limit,
    objectTypeId = DEFAULT_OBJECT_TYPE_ID,
    appointmentMapObjectTypeId = DEFAULT_APPOINTMENT_OBJECT_TYPE_ID
} = {}) {
    if (!hubspotAccessToken && !dryRun) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    if (!objectTypeId) {
        throw new Error("objectTypeId is required");
    }

    const { client, db } = await getDb(mongoUri, dbName);
    try {
        const collection = db.collection(collectionName);
        const mapColl = db.collection(mapCollection);
        let query = { id: { $exists: true, $ne: null } };
        if (resume) {
            const checkpoint = await loadCheckpoint(db, checkpointId);
            const lastId = checkpoint?.lastId;
            if (lastId) {
                query._id = { $gt: new ObjectId(lastId) };
            }
        }
        let cursor = collection.find(query);
        if (limit) {
            cursor = cursor.limit(Number(limit));
        }

        const summary = {
            processed: 0,
            created: 0,
            skippedAlreadyMapped: 0,
            errors: 0
        };

        const hubspotClient = dryRun ? null : buildHubspotClient(hubspotAccessToken);
        let meetingContactAssociationType = null;
        if (!dryRun) {
            try {
                meetingContactAssociationType = await getDefaultAssociationType(hubspotClient, objectTypeId, "contacts");
            } catch (err) {
                console.warn("failed to load meeting-contact association type", err?.message || err);
            }
        }

        for await (const appointment of cursor) {
            const lastProcessedId = appointment?._id ? String(appointment._id) : undefined;
            const ghlId = appointment?.id;
            if (!ghlId) {
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }
            summary.processed += 1;

            const existingMap = await mapColl.findOne({ ghlId, objectTypeId: appointmentMapObjectTypeId });
            if (existingMap?.hubspotId) {
                summary.skippedAlreadyMapped += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            const assignedToUser = await findAssignedToUser(db, appointment);
            const contactId = appointment?.contactId || appointment?.contact?.id || appointment?.ghlContactId;
            let hubspotContactId = null;
            if (contactId) {
                const contactMap = await mapColl.findOne({ ghlId: contactId, objectTypeId: "contact" });
                hubspotContactId = contactMap?.hubspotId || null;
            }
            const ownerId = normalizeOwnerId(assignedToUser);
            if (!ownerId && assignedToUser?.hubSpot) {
                console.warn("invalid hubspot_owner_id for appointment", {
                    appointmentId: appointment?.id,
                    hubSpot: assignedToUser?.hubSpot
                });
            }
            const properties = buildAppointmentProperties(appointment, { ownerId });
            if (dryRun) {
                console.log("[dry-run] create meeting", ghlId, properties.hs_meeting_title);
                summary.created += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            try {
                const associations = [];
                if (hubspotContactId) {
                    associations.push({
                        to: { id: hubspotContactId },
                        types: meetingContactAssociationType ? [meetingContactAssociationType] : []
                    });
                }
                const response = await hubspotClient.crm.objects.basicApi.create(objectTypeId, {
                    properties,
                    associations
                });
                const hubspotId = response?.id;
                if (hubspotId) {
                    await upsertGhlHubspotIdMap(db, { ghlId, hubspotId, objectTypeId: appointmentMapObjectTypeId });
                }
                summary.created += 1;
            } catch (err) {
                const status = err?.code || err?.response?.statusCode || err?.response?.status;
                console.error("failed to create appointment", status || "", err?.message || err);
                await recordFailedMigration(db, {
                    entityType: "appointment",
                    ghlId,
                    reason: err?.message || String(err)
                });
                summary.errors += 1;
            } finally {
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
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
            const result = await deleteAppointmentsByImportTag(hubspotClient, cli.objectTypeId || DEFAULT_OBJECT_TYPE_ID, {
                importTag,
                dryRun: cli.dryRun
            });
            console.log(`delete complete: scanned ${result.scanned}, deleted ${result.deleted}`);
            return;
        }
        const summary = await migrateAppointmentsToHubspot({
            mongoUri: cli.mongoUri,
            dbName: cli.dbName,
            collectionName: cli.collection,
            mapCollection: cli.mapCollection,
            hubspotAccessToken: cli.hubspotAccessToken,
            checkpointId: cli.checkpointId,
            resume: cli.resume !== false,
            dryRun: cli.dryRun,
            limit,
            objectTypeId: cli.objectTypeId,
            appointmentMapObjectTypeId: cli.appointmentMapObjectTypeId
        });
        console.log("appointment migration complete:", summary);
    };

    run().catch((err) => {
        console.error("migrateAppointments failed:", err?.message || err);
        process.exit(1);
    });
}
