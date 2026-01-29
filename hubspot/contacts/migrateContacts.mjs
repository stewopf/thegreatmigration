import { Client } from "@hubspot/api-client";
import { MongoClient } from "mongodb";

function buildHubspotClient(accessToken = process.env.HUBSPOT_ACCESS_TOKEN) {
    if (!accessToken) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    return new Client({ accessToken });
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

function toHubspotPropertyName(name) {
    return `${name || ""}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 100);
}

function buildBaseContactProperties(properties = {}) {
    const normalized = {};
    Object.entries(properties).forEach(([key, value]) => {
        const cleaned = normalizePropertyValue(value);
        if (cleaned !== undefined) {
            normalized[key] = cleaned;
        }
    });
    return normalized;
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

async function findAssignedToUser(db, ghlContact) {
    if (!db || !ghlContact) {
        return null;
    }
    const userId = ghlContact.assignedTo;
    if (!userId) {
        return null;
    }
    return db.collection("users").findOne({ id: userId });
}

/**
 * Create a base HubSpot contact.
 * Requires at least an email address.
 */
export async function createBaseHubspotContact(
    ghlContact = {},
    hubspotClient = null,
    { dryRun = false, db = null } = {}
) {
    const {
        email,
        firstName,
        lastName,
        companyName: company,
        phone,
        address1: address,
        city,
        state,
        postalCode: zip,
        country,
        dateOfBirth,
        dnd
    } = ghlContact || {};

    if (!email) {
        await recordFailedMigration(db, {
            entityType: "contact",
            ghlId: ghlContact?.id,
            reason: "missing email"
        });
        throw new Error(`email is required to create a HubSpot contact ghlContact=${JSON.stringify(ghlContact?.id)}`);
    }

    const baseProperties = {
        email,
        firstname: firstName,
        lastname: lastName,
        phone,
        company,
        address,
        city,
        state,
        zip,
        country,
        dateOfBirth,
        dnd,
        ghl_contact_id: ghlContact.id
    };
    const assignedToUser = await findAssignedToUser(db, ghlContact);
    if (assignedToUser) {
        baseProperties.assignedTo = assignedToUser?.hubSpot?.id;
    }
    if (baseProperties.assignedTo) {
        console.log("assignedTo", baseProperties.assignedTo);
    }
    ghlContact.customFields?.forEach((customField) => {
        switch (customField.id) {
            case "g6tSBxPatzAwTtIhHVvx": // "SF Account ID"
                baseProperties[toHubspotPropertyName("sfAccountId")] = customField.value;
                break;
            case "kWZU041gXmtUwmuuKkv0":
                baseProperties[toHubspotPropertyName("contactLanguage")] = customField.value === "Spanish" ? "es" : "en";
                break;
            case "gyxG6J8U3DjXt4yfOd1R":
                baseProperties.ghl_created_date = new Date(customField.value).toISOString();
                break;
            case "4xAtW6G5ay7NcXlw3LTJ":
                baseProperties[toHubspotPropertyName("smsOptIn")] = customField.value;
                break;
            case "qUtfAwv63pRApArTvSjp":
                baseProperties.secondaryemail = customField.value;
                break;
            case "vGf2kz1P3ZH10TQx7U9N":
                baseProperties.secondaryphone = customField.value;
                break;
            case "dn0Lzqfp4S5jukPm1DKg":
                baseProperties.address2 = customField.value;
                break;
            default:
                break;
        }
    });
    const properties = buildBaseContactProperties(baseProperties);

    if (dryRun) {
        // console.log(JSON.stringify({ ghlContact, baseProperties: properties, assignedToUser }, null, 2));
        return { id: null, properties };
    }
    process.exit(0);
    const client = hubspotClient || buildHubspotClient();
    const created = await client.crm.contacts.basicApi.create({
        properties
    });
    return { id: created.id, properties: created.properties };
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

async function upsertGhlHubspotIdMap(db, {
    ghlId,
    hubspotId,
    objectTypeId = "contact"
} = {}) {
    if (!ghlId || !hubspotId) {
        return;
    }
    await db.collection("GHLHubspotIdMap").updateOne(
        { ghlId, objectTypeId },
        {
            $set: {
                ghlId,
                hubspotId,
                objectTypeId,
                updatedAt: new Date()
            },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
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
Usage: node hubspot/contacts/migrateContacts.mjs [options]

Options:
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name (default: GoHighLevel)
  --collection <name>      Mongo contacts collection (default: contacts)
  --limit <number>         Max contacts to migrate
  --checkpoint-id <id>     Checkpoint document id
  --reset <entity>         Clear failed maps + checkpoints
  --resume                 Resume from checkpoint (default)
  --no-resume              Start from the beginning
  --dry-run                Log actions without calling HubSpot
  --hubspot-access-token <token>  HubSpot private app token
  --help                   Show this help message
`);
}

async function resetMigrationState(db, {
    entity,
    checkpointId
} = {}) {
    if (!entity) {
        throw new Error("reset entity is required");
    }
    const normalized = String(entity).trim().toLowerCase();
    await db.collection("hubspot_failed_migrations").deleteMany({ entityType: normalized });
    await db.collection("GHLHubspotIdMap").deleteMany({ objectTypeId: normalized });
    if (checkpointId) {
        await db.collection("hubspot_transfer_checkpoints").deleteOne({ _id: checkpointId });
    } else {
        await db.collection("hubspot_transfer_checkpoints").deleteMany({});
    }
    console.log(`reset complete for entity: ${normalized}`);
}

export async function migrateContactsToHubspot({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = "GoHighLevel",
    collectionName = "contacts",
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    checkpointId = "hubspot_contacts",
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
        let query = {};
        if (resume) {
            const checkpoint = await loadCheckpoint(db, checkpointId);
            if (checkpoint?.lastId) {
                query = { _id: { $gt: checkpoint.lastId } };
            }
        }

        const hubspotClient = dryRun ? null : new Client({ accessToken: hubspotAccessToken });
        const cursor = collection.find(query).sort({ _id: 1 });
        let processed = 0;
        let failed = 0;
        let assignedTo = 0;
        let noCompanyNameCount = 0;
        while (await cursor.hasNext()) {
            if (Number.isInteger(limit) && processed >= limit) {
                break;
            }
            const contact = await cursor.next();
            if (!contact) {
                break;
            }
            if (dryRun) {
                console.log(`[dry-run] migrate contact ${contact._id}`);
                processed += 1;
            }
            try {
                const created = await createBaseHubspotContact(contact, hubspotClient, { dryRun, db });
                if (! dryRun) {
                    await upsertGhlHubspotIdMap(db, {
                        ghlId: contact.id,
                        hubspotId: created?.id,
                        objectTypeId: "contact"
                    });
                }
                processed += 1;
                if (created?.properties?.assignedTo) {
                    assignedTo += 1;
                }
                if (! created?.properties?.company) {
                    noCompanyNameCount += 1;
                }
            } catch (err) {
                failed += 1;
                console.error("contact migration failed", contact?._id, err?.message || err);
            }
            await saveCheckpoint(db, checkpointId, {
                lastId: contact._id,
                processedCount: processed,
                failedCount: failed,
                assignedToCount: assignedTo,
                noCompanyNameCount: noCompanyNameCount
            });
        }

        console.log(`migration complete: processed ${processed}, failed ${failed}, assignedTo ${assignedTo}, noCompanyNameCount ${noCompanyNameCount}`);
    } finally {
        await client.close();
    }
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
    const args = process.argv.slice(2);
    if (args.length === 0) {
        printUsage();
        process.exit(0);
    }
    const cli = parseCliArgs(args);
    if (cli.help) {
        printUsage();
        process.exit(0);
    }
    const parsedLimit = cli.limit ? Number(cli.limit) : undefined;
    const limit = Number.isInteger(parsedLimit) && parsedLimit > 0 ? parsedLimit : undefined;
    if (cli.reset) {
        const { client, db } = await getDb(
            cli.mongoUri || process.env.MONGO_URI || "mongodb://localhost:27017",
            cli.dbName || "GoHighLevel"
        );
        try {
            await resetMigrationState(db, {
                entity: cli.reset,
                checkpointId: cli.checkpointId
            });
        } finally {
            await client.close();
        }
        process.exit(0);
    }
    migrateContactsToHubspot({
        mongoUri: cli.mongoUri,
        dbName: cli.dbName,
        collectionName: cli.collection,
        hubspotAccessToken: cli.hubspotAccessToken,
        checkpointId: cli.checkpointId,
        resume: cli.resume,
        dryRun: cli.dryRun,
        limit
    }).catch((err) => {
        console.error("migrateContacts failed:", err?.message || err);
        process.exit(1);
    });
}
