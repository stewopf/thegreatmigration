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
  --delete-import-tag [tag]  Delete deals with import_tag (default: GHL_MIGRATION)
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

async function getDefaultDealAssociationType(hubspotClient, fromObject, toObject) {
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

async function getPrimaryCompanyIdForContact(hubspotClient, contactId) {
    if (!contactId) {
        return null;
    }
    let primaryType = null;
    try {
        primaryType = await getPrimaryCompanyContactAssociationType(hubspotClient);
    } catch (err) {
        console.warn("failed to load primary company association type", err?.message || err);
    }
    const response = await hubspotClient.crm.associations.v4.basicApi.getPage(
        "contacts",
        contactId,
        "companies",
        undefined,
        100
    );
    const results = Array.isArray(response?.results) ? response.results : [];
    if (results.length === 0) {
        return null;
    }
    if (primaryType?.associationTypeId) {
        const primary = results.find((association) => {
            const types = Array.isArray(association?.associationTypes) ? association.associationTypes : [];
            return types.some((type) => type?.associationTypeId === primaryType.associationTypeId);
        });
        if (primary?.toObjectId) {
            return primary.toObjectId;
        }
    }
    return results[0]?.toObjectId || null;
}

async function getCompanyApexId(hubspotClient, companyId, cache) {
    if (!hubspotClient || !companyId) {
        return null;
    }
    if (cache.has(companyId)) {
        return cache.get(companyId);
    }
    const response = await hubspotClient.crm.companies.basicApi.getById(
        companyId,
        ["apex_id"]
    );
    const apexId = response?.properties?.apex_id || null;
    cache.set(companyId, apexId);
    return apexId;
}

export async function deleteDealsByImportTag(
    hubspotClient,
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
        const searchResponse = await hubspotClient.crm.deals.searchApi.doSearch(searchRequest);
        const records = Array.isArray(searchResponse?.results) ? searchResponse.results : [];
        results.scanned += records.length;
        if (!dryRun && records.length > 0) {
            const inputs = records.map((record) => ({ id: record.id }));
            const batchApi = hubspotClient.crm.deals?.batchApi;
            if (batchApi?.archive) {
                await batchApi.archive({ inputs });
                results.deleted += inputs.length;
            } else {
                for (const record of records) {
                    await hubspotClient.crm.deals.basicApi.archive(record.id);
                    results.deleted += 1;
                }
            }
        }
        after = searchResponse?.paging?.next?.after;
    } while (after);
    return results;
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

function normalizeTagName(name) {
    const trimmed = String(name || "").trim();
    return trimmed ? trimmed : null;
}

function buildTagCacheKey(name) {
    return String(name || "").trim().toLowerCase();
}

async function getTagNameProperty(hubspotClient, state) {
    if (state?.tagNameProperty) {
        return state.tagNameProperty;
    }
    try {
        const response = await hubspotClient.crm.properties.coreApi.getAll("tags");
        const properties = response?.results || response || [];
        const preferred = properties.find((prop) => prop?.name === "name")
            || properties.find((prop) => String(prop?.name || "").toLowerCase().includes("name"));
        const fallback = properties.find((prop) => prop?.type === "string" && prop?.fieldType === "text");
        const selected = preferred?.name || fallback?.name || "name";
        state.tagNameProperty = selected;
        return selected;
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 404) {
            state.unavailable = true;
            return null;
        }
        throw err;
    }
}

async function searchHubspotTagByName(hubspotClient, name, propertyName) {
    const payload = {
        filterGroups: [
            {
                filters: [
                    {
                        propertyName,
                        operator: "EQ",
                        value: name
                    }
                ]
            }
        ],
        properties: [propertyName],
        limit: 1
    };
    const response = await hubspotClient.apiRequest({
        method: "POST",
        path: "/crm/v3/objects/tags/search",
        body: payload
    });
    const results = response?.body?.results || response?.results || [];
    return results[0] || null;
}

async function createHubspotTag(hubspotClient, name, propertyName) {
    const response = await hubspotClient.apiRequest({
        method: "POST",
        path: "/crm/v3/objects/tags",
        body: { properties: { [propertyName]: name } }
    });
    return response?.body || response || null;
}

function isTagEndpointUnsupported(err) {
    const status = err?.code || err?.response?.statusCode || err?.response?.status;
    const message = err?.response?.data?.message || err?.message || "";
    if (status === 404) {
        return true;
    }
    return status === 400 && String(message).includes("infer object type");
}

async function resolveHubspotTagIds(hubspotClient, tagNames, cache, state) {
    if (!hubspotClient || !Array.isArray(tagNames) || tagNames.length === 0) {
        return null;
    }
    if (state?.unavailable) {
        return null;
    }
    const ids = [];
    let propertyName = null;
    try {
        propertyName = await getTagNameProperty(hubspotClient, state);
    } catch (err) {
        if (isTagEndpointUnsupported(err)) {
            state.unavailable = true;
            return null;
        }
        throw err;
    }
    if (!propertyName) {
        return null;
    }
    for (const rawName of tagNames) {
        const normalized = normalizeTagName(rawName);
        if (!normalized) {
            continue;
        }
        const cacheKey = buildTagCacheKey(normalized);
        if (cache.has(cacheKey)) {
            const cachedId = cache.get(cacheKey);
            if (cachedId) {
                ids.push(cachedId);
            }
            continue;
        }
        let tag = null;
        try {
            tag = await searchHubspotTagByName(hubspotClient, normalized, propertyName);
        } catch (err) {
            if (isTagEndpointUnsupported(err)) {
                state.unavailable = true;
                return null;
            }
            throw err;
        }
        if (!tag) {
            try {
                tag = await createHubspotTag(hubspotClient, normalized, propertyName);
            } catch (err) {
                if (isTagEndpointUnsupported(err)) {
                    state.unavailable = true;
                    return null;
                }
                throw err;
            }
        }
        const tagId = tag?.id;
        cache.set(cacheKey, tagId || null);
        if (tagId) {
            ids.push(tagId);
        }
    }
    return ids.length > 0 ? ids.join(";") : null;
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
async function buildDealProperties(opportunity, { defaultDealstage, defaultPipeline, hsTagIds, db, companyApexId } = {}) {
    const dealname =
        normalizePropertyValue(opportunity?.name) ||
        normalizePropertyValue(opportunity?.title) ||
        normalizePropertyValue(opportunity?.displayName) ||
        `GHL Opportunity ${opportunity?.id || ""}`.trim();
    const dealstage = defaultDealstage;
    const pipeline = defaultPipeline;
    const amount = normalizePropertyValue(
        opportunity?.monetaryValue ?? opportunity?.amount ?? opportunity?.value ?? opportunity?.price
    );
    const closedate = toTimestamp(opportunity?.closedAt || opportunity?.closeDate || opportunity?.closedOn);

    const properties = {
        dealname,
        dealstage,
        import_tag: "GHL_MIGRATION",
        ghl_id: opportunity?.id,
        apex_id: companyApexId || opportunity?.apexId,
        phone: opportunity?.contact?.phone,
        email: opportunity?.contact?.email,
        status: normalizeEnumerationValue(opportunity?.status),
        ghl_pipeline_id: opportunity?.pipelineId,
    };
    if (hsTagIds) {
        properties.hs_tag_ids = hsTagIds;
    }

    (opportunity?.customFields || []).forEach((customField) => {
        switch (customField.id) {
            case "gyxG6J8U3DjXt4yfOd1R": // "SF Account ID"
                properties['ghl_created_on'] = new Date(customField.value).toISOString();
                break;
            case 'fG1SzEF1g2BFKEiWJnxW': 
                properties['ghl_notes'] = customField.fieldValueString;
                break;
            case 'C6ibnKRfYinqxJirahN6':
                properties['product_status'] = normalizeEnumerationValue(customField.fieldValueString);
                break;
            case 'pktIJsey3UvYPAn2BjPE':
                properties['installment_start_date'] = new Date(customField.fieldValueDatee).toISOString();
                break;
            case 'd6LQnhGA6VWI6HCyXral':
                properties['consultation_status'] = normalizeEnumerationValue(customField.fieldValueString);
                break;
        }
    });
    const assignedToUser = await findAssignedToUser(db, opportunity);
    if (assignedToUser && assignedToUser?.hubSpot?.id) {
        properties.hubspot_owner_id = assignedToUser?.hubSpot?.id;
    }

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

function resolveDefaultPipeline(pipelines = []) {
    if (!Array.isArray(pipelines) || pipelines.length === 0) {
        return null;
    }
    const explicitDefault = pipelines.find((pipeline) => pipeline?.default || pipeline?.isDefault);
    if (explicitDefault) {
        return explicitDefault;
    }
    const byDisplayOrder = [...pipelines].sort((a, b) => (a?.displayOrder ?? 0) - (b?.displayOrder ?? 0));
    return byDisplayOrder[0] || null;
}

function resolveDefaultStage(pipeline) {
    const stages = Array.isArray(pipeline?.stages) ? pipeline.stages : [];
    if (stages.length === 0) {
        return null;
    }
    const openStages = stages.filter((stage) => stage?.metadata?.isClosed !== "true");
    const source = openStages.length > 0 ? openStages : stages;
    const byDisplayOrder = [...source].sort((a, b) => (a?.displayOrder ?? 0) - (b?.displayOrder ?? 0));
    return byDisplayOrder[0] || null;
}

async function getDefaultPipelineAndStage(hubspotClient) {
    const response = await hubspotClient.crm.pipelines.pipelinesApi.getAll("deals");
    const pipelines = response?.results || response || [];
    const pipeline = resolveDefaultPipeline(pipelines);
    if (!pipeline) {
        return { pipelineId: null, stageId: null };
    }
    const stage = resolveDefaultStage(pipeline);
    return { pipelineId: pipeline?.id || null, stageId: stage?.id || null };
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
        const hubspotTagCache = new Map();
        const tagApiState = { unavailable: false };
        const companyApexIdCache = new Map();
        let dealToContactAssociationType = null;
        let dealToCompanyAssociationType = null;
        if (!dryRun) {
            try {
                dealToContactAssociationType = await getDefaultDealAssociationType(hubspotClient, "deals", "contacts");
                dealToCompanyAssociationType = await getDefaultDealAssociationType(hubspotClient, "deals", "companies");
            } catch (err) {
                console.warn("failed to load deal association types", err?.message || err);
            }
        }
        let resolvedDefaults = { pipelineId: defaultPipeline, stageId: defaultDealstage };
        if (!dryRun && (!defaultDealstage || !defaultPipeline)) {
            resolvedDefaults = await getDefaultPipelineAndStage(hubspotClient);
        }
        const fallbackPipeline = defaultPipeline || resolvedDefaults.pipelineId;
        const fallbackStage = defaultDealstage || resolvedDefaults.stageId;
        if (!fallbackStage && !dryRun) {
            console.warn("dealstage not provided and default stage not resolved; provide --dealstage");
        }

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

            let hsTagIds = null;
            if (!dryRun) {
                try {
                    const ghlTags = opportunity?.contact?.tags || [];
                    hsTagIds = await resolveHubspotTagIds(hubspotClient, ghlTags, hubspotTagCache, tagApiState);
                } catch (err) {
                    const status = err?.code || err?.response?.statusCode || err?.response?.status;
                    console.error("failed to resolve tags", status || "", err?.message || err);
                    summary.errors += 1;
                }
            }
            let hubspotContactId = null;
            let hubspotCompanyId = null;
            let companyApexId = null;
            if (!dryRun && opportunity?.contactId) {
                const contactMap = await mapColl.findOne({ ghlId: opportunity.contactId, objectTypeId: "contact" });
                hubspotContactId = contactMap?.hubspotId || null;
                if (hubspotContactId) {
                    try {
                        hubspotCompanyId = await getPrimaryCompanyIdForContact(hubspotClient, hubspotContactId);
                    } catch (err) {
                        console.warn("failed to resolve contact company", err?.message || err);
                    }
                }
            }
            if (!dryRun && hubspotCompanyId) {
                try {
                    companyApexId = await getCompanyApexId(hubspotClient, hubspotCompanyId, companyApexIdCache);
                } catch (err) {
                    console.warn("failed to resolve company apex_id", err?.message || err);
                }
            }
            if (!dryRun && opportunity?.contactId && !hubspotContactId) {
                await recordFailedMigration(db, {
                    entityType: "opportunity",
                    ghlId,
                    reason: "missing hubspot contact mapping"
                });
            }
            const properties = await buildDealProperties(opportunity, {
                defaultDealstage: fallbackStage,
                defaultPipeline: fallbackPipeline,
                hsTagIds,
                db,
                companyApexId
            });
            if (!properties.dealstage) {
                summary.skippedMissingStage += 1;
                await recordFailedMigration(db, {
                    entityType: "opportunity",
                    ghlId,
                    reason: "missing dealstage"
                });
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
                    if (hubspotContactId) {
                        const associations = dealToContactAssociationType ? [dealToContactAssociationType] : [];
                        await hubspotClient.crm.associations.v4.basicApi.create(
                            "deals",
                            hubspotId,
                            "contacts",
                            hubspotContactId,
                            associations
                        );
                    }
                    if (hubspotCompanyId) {
                        const associations = dealToCompanyAssociationType ? [dealToCompanyAssociationType] : [];
                        await hubspotClient.crm.associations.v4.basicApi.create(
                            "deals",
                            hubspotId,
                            "companies",
                            hubspotCompanyId,
                            associations
                        );
                    }
                }
                summary.created += 1;
            } catch (err) {
                const status = err?.code || err?.response?.statusCode || err?.response?.status;
                console.error("failed to create deal", status || "", err?.message || err);
                await recordFailedMigration(db, {
                    entityType: "opportunity",
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
    if (cli.help) {
        printUsage();
        process.exit(0);
    }
    const run = async () => {
        if (cli.deleteImportTag) {
            const hubspotClient = buildHubspotClient(cli.hubspotAccessToken);
            const result = await deleteDealsByImportTag(hubspotClient, {
                importTag: typeof cli.deleteImportTag === "string" ? cli.deleteImportTag : "GHL_MIGRATION",
                dryRun: cli.dryRun
            });
            console.log(`delete complete: scanned ${result.scanned}, deleted ${result.deleted}`);
            return;
        }
        const summary = await migrateOpportunitiesToHubspot({
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
        });
        console.log("opportunity migration complete:", summary);
    };

    run().catch((err) => {
        console.error("opportunity migration failed:", err?.message || err);
        process.exit(1);
    });
}
