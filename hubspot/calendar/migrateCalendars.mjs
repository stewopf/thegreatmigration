import { Client } from "@hubspot/api-client";
import { MongoClient, ObjectId } from "mongodb";

const DEFAULT_DB_NAME = "GoHighLevel";
const DEFAULT_COLLECTION = "calendars";
const DEFAULT_MAP_COLLECTION = "GHLHubspotIdMap";
const DEFAULT_CHECKPOINT_ID = "hubspot_calendars";
const DEFAULT_OBJECT_TYPE_ID = "calendars";
const DEFAULT_NAME_PROPERTY = "name";

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
        if (key === "dryRun" || key === "resume") {
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
Usage: node hubspot/calendar/migrateCalendars.mjs [options]

Options:
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name (default: GoHighLevel)
  --collection <name>      Mongo calendars collection (default: calendars)
  --map-collection <name>  Mapping collection (default: GHLHubspotIdMap)
  --object-type-id <id>    HubSpot object type id (default: calendars)
  --name-property <name>   HubSpot property to store name (default: name)
  --limit <number>         Max calendars to migrate
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

function normalizePropertyValue(value) {
    if (value === undefined || value === null || value === "") {
        return undefined;
    }
    if (typeof value === "boolean") {
        return value ? "true" : "false";
    }
    return value;
}

function buildCalendarProperties(calendar, { nameProperty = DEFAULT_NAME_PROPERTY } = {}) {
    const name =
        normalizePropertyValue(calendar?.name) ||
        normalizePropertyValue(calendar?.title) ||
        normalizePropertyValue(calendar?.calendarName) ||
        `GHL Calendar ${calendar?.id || ""}`.trim();
    return {
        [nameProperty]: name
    };
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

export async function migrateCalendarsToHubspot({
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
    nameProperty = DEFAULT_NAME_PROPERTY
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

        for await (const calendar of cursor) {
            const lastProcessedId = calendar?._id ? String(calendar._id) : undefined;
            const ghlId = calendar?.id;
            if (!ghlId) {
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }
            summary.processed += 1;

            const existingMap = await mapColl.findOne({ ghlId, objectTypeId });
            if (existingMap?.hubspotId) {
                summary.skippedAlreadyMapped += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            const properties = buildCalendarProperties(calendar, { nameProperty });
            if (dryRun) {
                console.log("[dry-run] create calendar", ghlId, properties[nameProperty]);
                summary.created += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            try {
                const response = await hubspotClient.crm.objects.basicApi.create(objectTypeId, { properties });
                const hubspotId = response?.id;
                if (hubspotId) {
                    await upsertGhlHubspotIdMap(db, { ghlId, hubspotId, objectTypeId });
                }
                summary.created += 1;
            } catch (err) {
                const status = err?.code || err?.response?.statusCode || err?.response?.status;
                console.error("failed to create calendar", status || "", err?.message || err);
                await recordFailedMigration(db, {
                    entityType: "calendar",
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
        const summary = await migrateCalendarsToHubspot({
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
            nameProperty: cli.nameProperty
        });
        console.log("calendar migration complete:", summary);
    };

    run().catch((err) => {
        console.error("migrateCalendars failed:", err?.message || err);
        process.exit(1);
    });
}
