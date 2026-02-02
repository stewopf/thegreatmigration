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

export async function deleteHubspotObjectsByImportTag(
    hubspotClient,
    { importTag = "GHL_MIGRATION", dryRun = false } = {}
) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    const results = {
        contacts: { scanned: 0, deleted: 0 },
        companies: { scanned: 0, deleted: 0 }
    };
    await deleteByImportTagForObjectType(
        hubspotClient,
        "contacts",
        importTag,
        results.contacts,
        dryRun
    );
    await deleteByImportTagForObjectType(
        hubspotClient,
        "companies",
        importTag,
        results.companies,
        dryRun
    );
    return results;
}

async function deleteByImportTagForObjectType(
    hubspotClient,
    objectType,
    importTag,
    counters,
    dryRun
) {
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
        const searchResponse = await hubspotClient.crm[objectType].searchApi.doSearch(searchRequest);
        const results = Array.isArray(searchResponse?.results) ? searchResponse.results : [];
        counters.scanned += results.length;
        if (!dryRun && results.length > 0) {
            const inputs = results.map((record) => ({ id: record.id }));
            const batchApi = hubspotClient.crm[objectType]?.batchApi;
            if (batchApi?.archive) {
                await batchApi.archive({ inputs });
                counters.deleted += inputs.length;
            } else {
                for (const record of results) {
                    await hubspotClient.crm[objectType].basicApi.archive(record.id);
                    counters.deleted += 1;
                }
            }
        }
        after = searchResponse?.paging?.next?.after;
    } while (after);
}

async function getPrimaryCompanyContactAssociationType(hubspotClient) {
    const response = await hubspotClient.crm.associations.v4.schema.definitionsApi.getAll("companies", "contacts");
    const results = Array.isArray(response?.results) ? response.results : response;
    const primary = results?.find((item) => {
        const label = String(item?.label || "").toLowerCase();
        return label === "primary";
    });
    if (!primary) {
        return null;
    }
    return {
        associationCategory: primary?.category || "HUBSPOT_DEFINED",
        associationTypeId: primary?.typeId
    };
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
function normalizeEnumerationValue(value) {
    if (! value) return value;
    value = value.toString().trim();
    value = value
        .replace(/[^a-z0-9]+/gi, "_")
        .replace(/^_+|_+$/g, "")
        .toLowerCase();
    return value;
}

function capitalizeFirstCharacter(value) {
    if (!value) return value;
    const trimmed = value.toString().trim();
    if (!trimmed) return value;
    return trimmed.split(' ').map(v =>`${v.charAt(0).toUpperCase()}${v.slice(1)}`).join(' ');
}
function extractCompanyProperties(company, ghlContact) {
    const result = {name:company, website_url:ghlContact.website, import_tag: "GHL_MIGRATION"};
    ghlContact.customFields?.forEach((customField) => {
        switch (customField.id) {
            case "gyxG6J8U3DjXt4yfOd1R": 
                result.ghl_created_date = customField.value ? new Date(customField.value).toISOString() : undefined;
                break;
            case 'Iq99flttTMyx0RKzlYY5': 
                result.services_offered = customField.value;
                break;
            case '8fOwdZOm0bylfiaqOOkt':
                result.hours_of_operation = customField.value;
                break;
            case 'G5kwvVM0yZotFHDzTtU0':
                result.sf_reason_lost = customField.value;
                break;
            case 'zGWKpJ5VQTyE2CY2lIGX':
                result.contracted_services = Array.isArray(customField.value) ? customField.value.map(normalizeEnumerationValue).join(';') : normalizeEnumerationValue(customField.value);
                break;
            case 'cbD2OOhWDXDep6Bjw799':
                result.monthly_gross_sales = customField.value;
                break;
            case '9eHyavp1jfJTT7IJGRp8':
                result.time_in_business_at_creation = customField.value;
                break;
            case '5yPFgdkdeqyTDefOtsZs':
                result.apex_new_ach_request_url = customField.value;
                break;
            case 'DlGjoPpF3vQzaav0Ui6u':
                result.apex_new_rcc_request_url = customField.value;
                break;
            case 'xgqyCCP2GeDdydHfElRJ':
                result.apex_new_card_request_url = customField.value;
                break;
            case 'T8Hpdsu1W5gErAwbXJH6':
                result.existing_logo = customField.value === 'Yes';
                break;
            case 'LC2oRMOXgxnGYI6yKcth':
                result.preferred_website_language = customField.value === 'Spanish' ? 'es' : 'en';
                break;
            case 'YW4GIuaBiGVbDJo7aPae': 
                result.website_features = Array.isArray(customField.value) ? customField.value.map(normalizeEnumerationValue).join(';') : normalizeEnumerationValue(customField.value);
                break;
            case 'ZTCs1zCNZ19KZNkTx1hW':
                result.cancellation_reason = customField.value;
                break;
            case '509l4Gl65IKHoi8KpHMw':
                result.sf_industry = customField.value;
                break;
            case '4wAAp50m1fwku7MwfiRU':
                result.via_industry = normalizeEnumerationValue(customField.value);
                break;
            case 'Y52V3yr8R70CnDpfCEPJ':
                result.apex_id = customField.value;
                break;
            default:
                break;
        }
    });
    return result;
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
    let {
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
    email = email.toLowerCase();
    if (! company) {
        company = `NO-COMPANY NAME - ${firstName || ''} ${lastName || ''}`.trim();
    }
    const baseProperties = {
        email,
        firstname: capitalizeFirstCharacter(firstName),
        lastname: capitalizeFirstCharacter(lastName),
        phone,
        company,
        address,
        city,
        state,
        zip,
        country,
        dateOfBirth,
        dnd,
        ghl_contact_id: ghlContact.id,
        import_tag: "GHL_MIGRATION"
    };
    const assignedToUser = await findAssignedToUser(db, ghlContact);
    if (assignedToUser) {
        baseProperties.hubspot_owner_id = assignedToUser?.hubSpot?.id;
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
                baseProperties.secondary_email = customField.value;
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
        console.log(JSON.stringify({ ghlContact, baseProperties: properties, assignedToUser, companyProps }, null, 2));
        return { id: null, properties, companyProps };
    }
    const client = hubspotClient || buildHubspotClient();
    let created = null;
    try {
        created = await client.crm.contacts.basicApi.create({
            properties
        });
    } catch (err) {
        console.log(JSON.stringify({ ghlContact, baseProperties: properties, assignedToUser, err }, null, 4));
        throw err;
    }
    const companyProps = extractCompanyProperties(properties?.company, ghlContact);

    let companyCreated = null;
    if (created.id) {
        companyProps.hubspot_owner_id = assignedToUser?.hubSpot?.id;
        companyCreated = await createBaseHubspotCompany(companyProps, hubspotClient, { dryRun, db, contactId: created.id });
    }
    return { id: created.id, properties: created.properties, companyId: companyCreated?.id, companyProperties: companyCreated?.properties };
}

export async function createBaseHubspotCompany(
    ghlCompany = {},
    hubspotClient = null,
    { dryRun = false, db = null, contactId = null } = {}
) {


    const companyLabel = ghlCompany.name;
    if (!companyLabel) {
        await recordFailedMigration(db, {
            entityType: "company",
            ghlId: ghlCompany?.id,
            reason: "missing company name"
        });
        throw new Error(`company name is required to create a HubSpot company ghlCompany=${JSON.stringify(ghlCompany?.id)}`);
    }

    const baseProperties = buildBaseContactProperties(ghlCompany);

    if (dryRun) {
        console.log(JSON.stringify({ ghlCompany, baseProperties, contactId: contactId ? Number(contactId) : null }, null, 2));
        return { id: null, properties: baseProperties };
    }

    const client = hubspotClient || buildHubspotClient();
    const created = await client.crm.companies.basicApi.create({
        properties: baseProperties
    });

    await upsertGhlHubspotIdMap(db, {
        ghlId: ghlCompany?.id,
        hubspotId: created?.id,
        objectTypeId: "company"
    });

    if (contactId) {
        let associations = [];
        try {
            const primaryType = await getPrimaryCompanyContactAssociationType(client);
            if (primaryType?.associationTypeId) {
                associations = [primaryType];
            }
        } catch (err) {
            console.warn("failed to load primary association type, using default association", err?.message || err);
        }
        await client.crm.associations.v4.basicApi.create(
            "companies",
            created.id,
            "contacts",
            contactId,
            associations
        );
    }

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
  --delete-import-tag [tag]  Delete contacts/companies with import_tag (default: GHL_MIGRATION)
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
            if ((processed % 50) === 0) {
                console.log(`processed ${processed} contacts`);
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
                await recordFailedMigration(db, {
                    entityType: "contact",
                    ghlId: contact?.id ?? contact?._id,
                    reason: `migration error: ${err?.message || err}`
                });
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
    if (cli.deleteImportTag !== undefined) {
        const importTag = cli.deleteImportTag === true ? "GHL_MIGRATION" : cli.deleteImportTag;
        const hubspotClient = buildHubspotClient(cli.hubspotAccessToken);
        const results = await deleteHubspotObjectsByImportTag(hubspotClient, {
            importTag,
            dryRun: cli.dryRun
        });
        console.log("delete import tag complete:", results);
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
