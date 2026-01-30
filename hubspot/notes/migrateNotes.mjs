import { Client } from "@hubspot/api-client";
import { MongoClient, ObjectId } from "mongodb";

const DEFAULT_DB_NAME = "GoHighLevel";
const DEFAULT_NOTES_COLLECTION = "notes";
const DEFAULT_MAP_COLLECTION = "GHLHubspotIdMap";
const DEFAULT_OBJECT_TYPE = "contact";
const DEFAULT_CHECKPOINT_ID = "hubspot_notes";

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
        if (key === "dryRun" || key === "delete" || key === "resume") {
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
Usage: node hubspot/notes/migrateNotes.mjs [options]

Options:
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name (default: GoHighLevel)
  --notes-collection <name>  Mongo notes collection (default: notes)
  --map-collection <name>  Mapping collection (default: GHLHubspotIdMap)
  --limit <number>         Max notes to migrate
  --checkpoint-id <id>     Checkpoint document id
  --resume                 Resume from checkpoint (default)
  --no-resume              Start from the beginning
  --delete                 Delete all HubSpot notes
  --batch-size <number>    Page size for delete (default: 100)
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

function normalizeNoteBody(value) {
    if (value === undefined || value === null) {
        return undefined;
    }
    const normalized = String(value).trim();
    return normalized ? normalized : undefined;
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

function buildNoteProperties(note, { includePlainText = false } = {}) {
    const htmlBody = normalizeNoteBody(note?.body);
    const textBody = normalizeNoteBody(note?.bodyText);
    if (!htmlBody && !textBody) {
        return null;
    }
    const timestamp = toTimestamp(
        note?.createdAt || note?.created || note?.dateAdded || note?.dateCreated || note?.updatedAt
    );
    const properties = {
        hs_note_body: htmlBody || textBody,
        hs_timestamp: timestamp
    };
    if (includePlainText && textBody && textBody !== htmlBody) {
        properties.hs_note_body_plain_text = textBody;
    }
    return properties;
}

async function getNoteToContactAssociationType(hubspotClient) {
    const response = await hubspotClient.crm.associations.v4.schema.definitionsApi.getAll("notes", "contacts");
    const results = Array.isArray(response?.results) ? response.results : response;
    const match = results?.find((item) => (item?.category || item?.associationCategory) === "HUBSPOT_DEFINED");
    if (!match?.typeId) {
        throw new Error("Unable to resolve note-to-contact association type id");
    }
    return {
        associationCategory: match?.category || match?.associationCategory || "HUBSPOT_DEFINED",
        associationTypeId: match.typeId
    };
}

async function checkPlainTextProperty(hubspotClient) {
    try {
        await hubspotClient.crm.properties.coreApi.getByName("notes", "hs_note_body_plain_text");
        return true;
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 404) {
            return false;
        }
        throw err;
    }
}

export async function migrateNotesToHubspot({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = DEFAULT_DB_NAME,
    notesCollection = DEFAULT_NOTES_COLLECTION,
    mapCollection = DEFAULT_MAP_COLLECTION,
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    limit,
    checkpointId = DEFAULT_CHECKPOINT_ID,
    resume = true,
    dryRun = false
} = {}) {
    if (!hubspotAccessToken && !dryRun) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }

    const { client, db } = await getDb(mongoUri, dbName);
    try {
        const notesColl = db.collection(notesCollection);
        const mapColl = db.collection(mapCollection);
        const query = { contactId: { $exists: true, $ne: null } };
        if (resume) {
            const checkpoint = await loadCheckpoint(db, checkpointId);
            const lastId = checkpoint?.lastId;
            if (lastId) {
                query._id = { $gt: new ObjectId(lastId) };
            }
        }
        let cursor = notesColl.find(query);
        if (limit) {
            cursor = cursor.limit(Number(limit));
        }
        const progressInterval = 100;

        const summary = {
            processed: 0,
            created: 0,
            skippedMissingContactId: 0,
            skippedMissingHubspotId: 0,
            skippedMissingBody: 0,
            errors: 0
        };

        let hubspotClient;
        let associationType;
        let includePlainText = false;

        if (!dryRun) {
            hubspotClient = buildHubspotClient(hubspotAccessToken);
            associationType = await getNoteToContactAssociationType(hubspotClient);
            includePlainText = await checkPlainTextProperty(hubspotClient);
        }

        for await (const note of cursor) {
            let lastProcessedId = note?._id ? String(note._id) : undefined;
            const contactId = note?.contactId;
            if (!contactId) {
                summary.skippedMissingContactId += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }
            summary.processed += 1;
            const mapping = await mapColl.findOne({ ghlId: contactId, objectTypeId: DEFAULT_OBJECT_TYPE });
            const hubspotContactId = mapping?.hubspotId;
            if (!hubspotContactId) {
                summary.skippedMissingHubspotId += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            const properties = buildNoteProperties(note, { includePlainText });
            if (!properties) {
                summary.skippedMissingBody += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            if (dryRun) {
                console.log("[dry-run] create note for contact", contactId, "->", hubspotContactId);
                summary.created += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                continue;
            }

            try {
                await hubspotClient.crm.objects.notes.basicApi.create({
                    properties,
                    associations: [
                        {
                            to: { id: hubspotContactId },
                            types: [associationType]
                        }
                    ]
                });
                summary.created += 1;
                if (lastProcessedId) {
                    await saveCheckpoint(db, checkpointId, { lastId: lastProcessedId });
                }
                if (summary.processed % progressInterval === 0) {
                    console.log(
                        `notes progress: processed=${summary.processed}, created=${summary.created}, skippedMissingContactId=${summary.skippedMissingContactId}, skippedMissingHubspotId=${summary.skippedMissingHubspotId}, skippedMissingBody=${summary.skippedMissingBody}, errors=${summary.errors}`
                    );
                }
            } catch (err) {
                const status = err?.code || err?.response?.statusCode || err?.response?.status;
                console.error("failed to create note", status || "", err?.message || err);
                summary.errors += 1;
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

export async function deleteHubspotNotes({
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    dryRun = false,
    batchSize = 100
} = {}) {
    if (!hubspotAccessToken) {
        if (dryRun) {
            return { scanned: 0, deleted: 0, skipped: true };
        }
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    const hubspotClient = buildHubspotClient(hubspotAccessToken);
    const result = { scanned: 0, deleted: 0 };
    let after;

    do {
        const page = await hubspotClient.crm.objects.notes.basicApi.getPage(batchSize, after);
        const notes = Array.isArray(page?.results) ? page.results : [];
        result.scanned += notes.length;
        if (!dryRun && notes.length > 0) {
            const inputs = notes.map((note) => ({ id: note.id }));
            const batchApi = hubspotClient.crm.objects.notes.batchApi;
            if (batchApi?.archive) {
                await batchApi.archive({ inputs });
                result.deleted += inputs.length;
            } else {
                for (const note of notes) {
                    await hubspotClient.crm.objects.notes.basicApi.archive(note.id);
                    result.deleted += 1;
                }
            }
        }
        after = page?.paging?.next?.after;
    } while (after);

    return result;
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
    const cli = parseCliArgs(process.argv.slice(2));
    if (cli.help) {
        printUsage();
        process.exit(0);
    }
    if (cli.delete) {
        deleteHubspotNotes({
            hubspotAccessToken: cli.hubspotAccessToken,
            dryRun: cli.dryRun,
            batchSize: cli.batchSize
        }).then((result) => {
            console.log(`delete complete: scanned ${result.scanned}, deleted ${result.deleted}`);
        }).catch((err) => {
            console.error("note delete failed:", err?.message || err);
            process.exit(1);
        });
    } else {
        migrateNotesToHubspot({
            mongoUri: cli.mongoUri,
            dbName: cli.dbName,
            notesCollection: cli.notesCollection,
            mapCollection: cli.mapCollection,
            hubspotAccessToken: cli.hubspotAccessToken,
            checkpointId: cli.checkpointId,
            resume: cli.resume !== false,
            limit: cli.limit,
            dryRun: cli.dryRun
        }).then((summary) => {
            console.log("note migration complete:", summary);
        }).catch((err) => {
            console.error("note migration failed:", err?.message || err);
            process.exit(1);
        });
    }
}
