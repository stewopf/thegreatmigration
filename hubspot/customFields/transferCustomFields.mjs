import { MongoClient } from "mongodb";
import { Client } from "@hubspot/api-client";

const DEFAULT_CONTACT_OBJECT_TYPE = "contacts";
const GHL_TO_HUBSPOT_OBJECT_TYPE = {
    contact: "contacts",
    opportunity: "deals",
    company: "companies",
    ticket: "tickets"
};

export function toHubspotPropertyName(name) {
    return `${name}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 50);
}

export function mapGhlModelToHubspotObjectType(model) {
    if (!model) {
        return null;
    }
    return GHL_TO_HUBSPOT_OBJECT_TYPE[String(model).toLowerCase()] || null;
}

function toHubspotEnumValue(value, fallback) {
    const normalized = toHubspotPropertyName(value || "");
    return normalized || fallback || "option";
}

export function mapGhlFieldToHubspotProperty(ghlField) {
    const dataType = String(ghlField?.dataType || "TEXT").toUpperCase();
    const baseName = ghlField?.fieldKey || ghlField?.name || "custom";
    const name = toHubspotPropertyName(baseName) || "custom";
    const label = ghlField?.name || ghlField?.fieldKey || "GHL Custom Field";
    let type = "string";
    let fieldType = "text";
    let options;

    switch (dataType) {
        case "CHECKBOX":
            type = "bool";
            fieldType = "booleancheckbox";
            break;
        case "LARGE_TEXT":
            type = "string";
            fieldType = "textarea";
            break;
        case "NUMBER":
        case "NUMERIC":
        case "NUMERICAL":
        case "MONETORY":
            type = "number";
            fieldType = "number";
            break;
        case "DATE":
            type = "date";
            fieldType = "date";
            break;
        case "DATETIME":
            type = "datetime";
            fieldType = "datetime";
            break;
        case "DROPDOWN":
        case "SINGLE_SELECT":
        case "SINGLE_OPTIONS":
        case "RADIO":
            type = "enumeration";
            fieldType = "select";
            break;
        case "MULTI_SELECT":
        case "MULTIPLE_OPTIONS":
            type = "enumeration";
            fieldType = "checkbox";
            break;
        case "PHONE":
            type = "string";
            fieldType = "phone";
            break;
        case "EMAIL":
            type = "string";
            fieldType = "email";
            break;
        case "WEBSITE":
            type = "string";
            fieldType = "text";
            break;
        case "FILE_UPLOAD":
            type = "string";
            fieldType = "text";
            break;
        default:
            type = "string";
            fieldType = "text";
            break;
    }

    if (type === "enumeration") {
        const picklist = Array.isArray(ghlField?.picklistOptions) ? ghlField.picklistOptions : [];
        const usedValues = new Set();
        options = picklist.map((option, index) => {
            const labelText = String(option);
            let value = toHubspotEnumValue(labelText, `option_${index + 1}`);
            while (usedValues.has(value)) {
                value = `${value}_${usedValues.size + 1}`;
            }
            usedValues.add(value);
            return { label: labelText, value };
        });
    }

    return { name, label, type, fieldType, options };
}

export async function ensureObjectProperty(hubspotClient, objectType, { name, label, type = "string", fieldType = "text", options } = {}) {
    try {
        await hubspotClient.crm.properties.coreApi.getByName(objectType, name);
        return;
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status && status !== 404) {
            throw err;
        }
    }
    const payload = {
        name,
        label,
        type,
        fieldType
    };
    if (Array.isArray(options) && options.length > 0) {
        payload.options = options;
    }
    try {
        await hubspotClient.crm.properties.coreApi.create(objectType, payload);
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 409) {
            return;
        }
        throw err;
    }
}

async function deleteObjectProperty(hubspotClient, objectType, propertyName) {
    try {
        await hubspotClient.crm.properties.coreApi.archive(objectType, propertyName);
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 404) {
            return;
        }
        throw err;
    }
}

export async function ensureGhlCustomFields(hubspotClient, customFields = []) {
    await ensureObjectProperty(hubspotClient, DEFAULT_CONTACT_OBJECT_TYPE, {
        name: "ghl_contact_id",
        label: "GHL Contact ID",
        type: "string",
        fieldType: "text"
    });
    for (const field of customFields) {
        const propName = toHubspotPropertyName(field?.name || "custom");
        const label = field?.name || "GHL Custom Field";
        await ensureObjectProperty(hubspotClient, DEFAULT_CONTACT_OBJECT_TYPE, {
            name: propName,
            label,
            type: "string",
            fieldType: "text"
        });
    }
}

export async function createHubspotPropertyFromGhlField(hubspotClient, ghlField, hubspotObjectType) {
    const objectType = hubspotObjectType
        || mapGhlModelToHubspotObjectType(ghlField?.model)
        || DEFAULT_CONTACT_OBJECT_TYPE;
    const property = mapGhlFieldToHubspotProperty(ghlField);
    await ensureObjectProperty(hubspotClient, objectType, property);
    return { objectType, property };
}

function getFieldKey(model, field) {
    const fieldId = field?.id || "";
    return `${model}:${fieldId}`;
}

function getFieldSortKey(model, field) {
    const fieldKey = field?.fieldKey || "";
    const name = field?.name || "";
    const fieldId = field?.id || "";
    return `${model}::${fieldKey}::${name}::${fieldId}`;
}

async function getDb(mongoUri, dbName) {
    if (!mongoUri) {
        throw new Error("MONGO_URI is not set");
    }
    const client = new MongoClient(mongoUri);
    await client.connect();
    return { client, db: client.db(dbName) };
}

async function loadCustomFieldsFromDb(db) {
    const docs = await db.collection("customfields").find({}).toArray();
    const fields = [];
    for (const doc of docs) {
        const model = doc?.id || doc?.model;
        if (!model) {
            continue;
        }
        const fieldMap = doc?.[model] || doc?.customFields || doc?.fields;
        if (!fieldMap || typeof fieldMap !== "object") {
            continue;
        }
        for (const [fieldId, field] of Object.entries(fieldMap)) {
            const normalized = {
                ...field,
                id: field?.id || fieldId,
                model: field?.model || model
            };
            fields.push({ model, field: normalized });
        }
    }
    return fields;
}

async function loadCheckpoint(db, checkpointId) {
    return db.collection("hubspot_transfer_checkpoints").findOne({ _id: checkpointId });
}

async function saveCheckpoint(db, checkpointId, data) {
    const payload = {
        ...data,
        updatedAt: new Date()
    };
    await db.collection("hubspot_transfer_checkpoints").updateOne(
        { _id: checkpointId },
        { $set: payload, $setOnInsert: { createdAt: new Date() } },
        { upsert: true }
    );
}

export async function transferCustomFields({
    mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017",
    dbName = "GoHighLevel",
    hubspotAccessToken = process.env.HUBSPOT_ACCESS_TOKEN,
    checkpointId = "hubspot_custom_fields",
    resume = true,
    dryRun = false,
    deleteMode = false
} = {}) {
    if (!hubspotAccessToken && !dryRun) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }

    const { client, db } = await getDb(mongoUri, dbName);
    try {
        const fields = await loadCustomFieldsFromDb(db);
        const sorted = fields
            .sort((a, b) => getFieldSortKey(a.model, a.field).localeCompare(getFieldSortKey(b.model, b.field)));

        let startIndex = 0;
        if (resume) {
            const checkpoint = await loadCheckpoint(db, checkpointId);
            if (checkpoint?.lastKey) {
                const index = sorted.findIndex((item) => getFieldKey(item.model, item.field) === checkpoint.lastKey);
                if (index >= 0) {
                    startIndex = index + 1;
                }
            }
        }

        const hubspotClient = dryRun ? null : new Client({ accessToken: hubspotAccessToken });
        let processed = 0;

        for (let i = startIndex; i < sorted.length; i += 1) {
            const { model, field } = sorted[i];
            const key = getFieldKey(model, field);
            const property = mapGhlFieldToHubspotProperty(field);
            if (dryRun) {
                const action = deleteMode ? "delete" : "create";
                console.log(`[dry-run] ${action} property for`, key, property?.name);
                processed += 1;
                await saveCheckpoint(db, checkpointId, { lastKey: key, processedCount: processed });
                continue;
            }
            try {
                if (deleteMode) {
                    const objectType = mapGhlModelToHubspotObjectType(field?.model) || DEFAULT_CONTACT_OBJECT_TYPE;
                    await deleteObjectProperty(hubspotClient, objectType, property.name);
                } else {
                    await createHubspotPropertyFromGhlField(hubspotClient, field);
                }
                processed += 1;
                await saveCheckpoint(db, checkpointId, { lastKey: key, processedCount: processed });
            } catch (err) {
                console.error("transfer failed for", key, err?.message || err);
            }
        }

        const actionWord = deleteMode ? "deleted" : "processed";
        console.log(`transfer complete: ${actionWord} ${processed} fields`);
    } finally {
        await client.close();
    }
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
Usage: node hubspot/transferCustomFields.mjs [options]

Options:
  --dry-run                Log actions without calling HubSpot
  --delete                 Delete HubSpot properties created from GHL
  --resume                 Resume from checkpoint (default)
  --no-resume              Start from the beginning
  --checkpoint-id <id>     Checkpoint document id
  --mongo-uri <uri>        Mongo connection string
  --db-name <name>         Mongo database name
  --hubspot-access-token <token>  HubSpot private app token
  --help                   Show this help message
`);
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
    const cli = parseCliArgs(process.argv.slice(2));
    if (cli.help) {
        printUsage();
        process.exit(0);
    }
    transferCustomFields({
        mongoUri: cli.mongoUri,
        dbName: cli.dbName,
        hubspotAccessToken: cli.hubspotAccessToken,
        checkpointId: cli.checkpointId,
        resume: cli.resume,
        dryRun: cli.dryRun,
        deleteMode: cli.delete
    })
        .catch((err) => {
            console.error("transferCustomFields failed:", err?.message || err);
            process.exit(1);
        });
}
