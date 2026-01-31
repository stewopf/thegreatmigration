import { Client } from "@hubspot/api-client";
import { MongoClient, ObjectId } from "mongodb";

const DEFAULT_DB_NAME = "GoHighLevel";
const DEFAULT_COLLECTION = "opportunities";
const DEFAULT_MAP_COLLECTION = "GHLHubspotIdMap";
const DEFAULT_CHECKPOINT_ID = "hubspot_opportunities";
const OBJECT_TYPE_ID = "opportunity";

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
Usage: node hubspot/opportunties/migrateOpportunities.mjs [options]

Options:
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name (default: GoHighLevel)
  --collection <name>      Mongo opportunities collection (default: opportunities)
  --map-collection <name>  Mapping collection (default: GHLHubspotIdMap)
  --limit <number>         Max opportunities to migrate
  --checkpoint-id <id>     Checkpoint document id
  --resume                 Resume from checkpoint (default)
  --no-resume              Start from the beginning
  --dealstage <id>         HubSpot dealstage id (required if not in data)
  --pipeline <id>          HubSpot pipeline id (optional)
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

async function upsertGhlHubspotIdMap(db, { ghlId, hubspotId } = {}) {
    if (!ghlId || !hubspotId) {
        return;
    }
    await db.collection(DEFAULT_MAP_COLLECTION).updateOne(
        { ghlId, objectTypeId: OBJECT_TYPE_ID },
        {
            $set: { ghlId, hubspotId, objectTypeId: OBJECT_TYPE_ID, updatedAt: new Date() },
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

function buildDealProperties(opportunity, { defaultDealstage, defaultPipeline } = {}) {
    const dealname =
        normalizePropertyValue(opportunity?.name) ||
        normalizePropertyValue(opportunity?.title) ||
        normalizePropertyValue(opportunity?.displayName) ||
        `GHL Opportunity ${opportunity?.id || ""}`.trim();
    const dealstage = normalizePropertyValue(opportunity?.pipelineStageId || opportunity?.stageId) || defaultDealstage;
    const pipeline = normalizePropertyValue(opportunity?.pipelineId) || defaultPipeline;
    const amount = normalizePropertyValue(
        opportunity?.monetaryValue ?? opportunity?.amount ?? opportunity?.value ?? opportunity?.price
    );
    const closedate = toTimestamp(opportunity?.closedAt || opportunity?.closeDate || opportunity?.closedOn);

    const properties = {
        dealname,
        dealstage
    };
    if (pipeline) {
        properties.pipeline = pipeline;
    }
    if (amount !== undefined) {
        properties.amount = amount;
    }
    if (closedate !== undefined) {
        properties.closedate = closedate;
    }
    return properties;
}

export async function migrateOpportunitiesToHubspot({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = DEFAULT_DB_NAME,
    collectionName = DEFAULT_COLLECTION,
    mapCollection = DEFAULT_MAP_COLLECTION,
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    checkpointId = DEFAULT_CHECKPOINT_ID,
    resume = true,
    dryRun = false,
    limit,
    defaultDealstage,
    defaultPipeline
} = {}) {
    if (!hubspotAccessToken && !dryRun) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    if (!defaultDealstage && !dryRun) {
        console.warn("dealstage not provided; provide --dealstage if opportunities lack a stage");
    }

    const { client, db } = await getDb(mongoUri, dbName);
    try {
        const collection = db.collection(collectionName);
        const mapColl = db.collection(mapCollection);
        const query = { id: { $exists: true, $ne: null } };
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
            skippedMissingStage: 0,
            skippedAlreadyMapped: 0,
            errors: 0
        };

        const hubspotClient = dryRun ? null : buildHubspotClient(hubspotAccessToken);

        for await (const opportunity of cursor) {
            const lastProcessedId = opportunity?._id ? String(opportunity._id) : undefined;
            const ghlId = opportunity?.id;
            if (!ghlId) {
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }
            summary.processed += 1;

            const existingMap = await mapColl.findOne({ ghlId, objectTypeId: OBJECT_TYPE_ID });
            if (existingMap?.hubspotId) {
                summary.skippedAlreadyMapped += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            const properties = buildDealProperties(opportunity, { defaultDealstage, defaultPipeline });
            if (!properties.dealstage) {
                summary.skippedMissingStage += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            if (dryRun) {
                console.log("[dry-run] create deal", ghlId, properties.dealname);
                summary.created += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            try {
                const response = await hubspotClient.crm.deals.basicApi.create({ properties });
                const hubspotId = response?.id;
                if (hubspotId) {
                    await upsertGhlHubspotIdMap(db, { ghlId, hubspotId });
                }
                summary.created += 1;
            } catch (err) {
                const status = err?.code || err?.response?.statusCode || err?.response?.status;
                console.error("failed to create deal", status || "", err?.message || err);
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
    if (cli.help) {
        printUsage();
        process.exit(0);
    }
    migrateOpportunitiesToHubspot({
        mongoUri: cli.mongoUri,
        dbName: cli.dbName,
        collectionName: cli.collection,
        mapCollection: cli.mapCollection,
        hubspotAccessToken: cli.hubspotAccessToken,
        checkpointId: cli.checkpointId,
        resume: cli.resume !== false,
        limit: cli.limit,
        defaultDealstage: cli.dealstage,
        defaultPipeline: cli.pipeline,
        dryRun: cli.dryRun
    }).then((summary) => {
        console.log("opportunity migration complete:", summary);
    }).catch((err) => {
        console.error("opportunity migration failed:", err?.message || err);
        process.exit(1);
    });
}
