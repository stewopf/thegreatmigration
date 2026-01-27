import { MongoClient } from "mongodb";
import { Client } from "@hubspot/api-client";

const DEFAULT_DB_NAME = "GoHighLevel";
const DEFAULT_COLLECTION = "users";

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
        if (key === "delete" || key === "merge" || key === "dryRun") {
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
Usage: node hubspot/users/updateUserMapping.mjs [options]

Options:
  --merge                  Upsert HubSpot users into Mongo (default)
  --no-merge               Insert only if missing (no updates)
  --delete                 Remove hubSpot field and delete docs without id
  --dry-run                Log actions without writing
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name
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

function normalizeEmail(email) {
    return String(email || "").trim().toLowerCase();
}

async function loadAllHubspotUsers(hubspotClient, pageSize = 100) {
    const users = [];
    let after = undefined;
    while (true) {
        const response = await hubspotClient.settings.users.usersApi.getPage(pageSize, after);
        const results = response?.results || [];
        users.push(...results);
        const nextAfter = response?.paging?.next?.after;
        if (!nextAfter) {
            break;
        }
        after = nextAfter;
    }
    return users;
}

export async function upsertHubspotUsersByEmail(db, users, { merge = true, dryRun = false } = {}) {
    const collection = db.collection(DEFAULT_COLLECTION);
    let processed = 0;
    for (const user of users) {
        const email = normalizeEmail(user?.email);
        if (!email) {
            continue;
        }
        if (dryRun) {
            console.log("[dry-run] upsert user", email);
            processed += 1;
            continue;
        }
        const update = merge
            ? {
                $set: { email, hubSpot: user },
                $setOnInsert: { createdAt: new Date() }
            }
            : {
                $setOnInsert: { email, hubSpot: user, createdAt: new Date() }
            };
        await collection.updateOne({ email }, update, { upsert: true });
        processed += 1;
    }
    return processed;
}

export async function deleteHubspotUsers(db, { dryRun = false } = {}) {
    const collection = db.collection(DEFAULT_COLLECTION);
    if (dryRun) {
        const withHubspot = await collection.countDocuments({ hubSpot: { $exists: true } });
        const withoutId = await collection.countDocuments({
            $or: [{ id: { $exists: false } }, { id: null }, { id: "" }]
        });
        console.log(`[dry-run] unset hubSpot on ${withHubspot} docs`);
        console.log(`[dry-run] delete ${withoutId} docs without id`);
        return { unsetCount: withHubspot, deletedCount: withoutId };
    }

    const unsetResult = await collection.updateMany(
        { hubSpot: { $exists: true } },
        { $unset: { hubSpot: "" } }
    );
    const deleteResult = await collection.deleteMany({
        $or: [{ id: { $exists: false } }, { id: null }, { id: "" }]
    });
    return { unsetCount: unsetResult.modifiedCount || 0, deletedCount: deleteResult.deletedCount || 0 };
}

export async function runUpdateUserMapping({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = DEFAULT_DB_NAME,
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    merge = true,
    deleteMode = false,
    dryRun = false
} = {}) {
    if (!deleteMode && !hubspotAccessToken && !dryRun) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }

    const { client, db } = await getDb(mongoUri, dbName);
    try {
        if (deleteMode) {
            const result = await deleteHubspotUsers(db, { dryRun });
            console.log(`delete complete: removed hubSpot from ${result.unsetCount} docs, deleted ${result.deletedCount} docs without id`);
            return;
        }

        let users = [];
        if (!hubspotAccessToken) {
            console.log("[dry-run] HUBSPOT_ACCESS_TOKEN not set; skipping HubSpot fetch");
        } else {
            const hubspotClient = new Client({ accessToken: hubspotAccessToken });
            try {
                users = await loadAllHubspotUsers(hubspotClient);
                console.log(`loaded ${users.length} HubSpot users`);
            } catch (err) {
                const status = err?.code || err?.response?.statusCode || err?.response?.status;
                console.error("failed to load HubSpot users", status || "", err?.message || err);
                throw err;
            }
        }
        const processed = await upsertHubspotUsersByEmail(db, users, { merge, dryRun });
        console.log(`merge complete: processed ${processed} HubSpot users`);
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
    runUpdateUserMapping({
        mongoUri: cli.mongoUri,
        dbName: cli.dbName,
        hubspotAccessToken: cli.hubspotAccessToken,
        merge: cli.merge,
        deleteMode: cli.delete,
        dryRun: cli.dryRun
    }).catch((err) => {
        console.error("updateUserMapping failed:", err?.message || err);
        process.exit(1);
    });
}
