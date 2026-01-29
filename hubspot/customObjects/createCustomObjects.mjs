import { Client } from "@hubspot/api-client";

const DEFAULT_LABEL_PREFIX = "GHL";

function buildHubspotClient(accessToken = process.env.HUBSPOT_ACCESS_TOKEN) {
    if (!accessToken) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    return new Client({ accessToken });
}

function toHubspotObjectName(name) {
    return `${name || ""}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 50);
}

function toHubspotPropertyName(name) {
    return `${name || ""}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 50);
}

function normalizeObjectDefinition(objectDefinition) {
    const normalizedProperties = (objectDefinition?.properties || []).map((property) => ({
        ...property,
        name: toHubspotPropertyName(property?.name)
    }));
    return {
        ...objectDefinition,
        name: toHubspotObjectName(objectDefinition?.name),
        primaryDisplayProperty: toHubspotPropertyName(objectDefinition?.primaryDisplayProperty),
        secondaryDisplayProperties: (objectDefinition?.secondaryDisplayProperties || [])
            .map((name) => toHubspotPropertyName(name)),
        searchableProperties: (objectDefinition?.searchableProperties || [])
            .map((name) => toHubspotPropertyName(name)),
        requiredProperties: (objectDefinition?.requiredProperties || [])
            .map((name) => toHubspotPropertyName(name)),
        properties: normalizedProperties
    };
}

async function createCustomObjectSchema(hubspotClient, objectDefinition) {
    const normalized = normalizeObjectDefinition(objectDefinition);
    const payload = {
        name: normalized.name,
        labels: normalized.labels || {},
        requiredProperties: normalized.requiredProperties || [],
        searchableProperties: normalized.searchableProperties || [],
        primaryDisplayProperty: normalized.primaryDisplayProperty,
        secondaryDisplayProperties: normalized.secondaryDisplayProperties || [],
        properties: normalized.properties || []
    };
    return hubspotClient.crm.schemas.coreApi.create(payload);
}

async function getCustomObjectSchemaByName(hubspotClient, objectName) {
    const schemas = await hubspotClient.crm.schemas.coreApi.getAll();
    const list = Array.isArray(schemas?.results) ? schemas.results : schemas;
    const normalized = toHubspotObjectName(objectName);
    const match = list?.find((schema) => {
        const schemaName = toHubspotObjectName(schema?.name);
        const schemaTypeId = toHubspotObjectName(schema?.objectTypeId);
        return schemaName === normalized
            || schemaTypeId === normalized
            || (schemaTypeId && schemaTypeId.endsWith(`_${normalized}`));
    });
    return match || null;
}

async function updateCustomObjectSchema(hubspotClient, objectType, objectDefinition) {
    const normalized = normalizeObjectDefinition(objectDefinition);
    const payload = {
        labels: normalized.labels || {},
        requiredProperties: normalized.requiredProperties || [],
        searchableProperties: normalized.searchableProperties || [],
        primaryDisplayProperty: normalized.primaryDisplayProperty,
        secondaryDisplayProperties: normalized.secondaryDisplayProperties || []
    };
    return hubspotClient.crm.schemas.coreApi.update(objectType, payload);
}

async function deleteCustomObjectSchema(hubspotClient, objectType) {
    return hubspotClient.crm.schemas.coreApi.archive(objectType);
}

async function ensureCustomObjectProperty(hubspotClient, objectTypeId, property) {
    const name = toHubspotPropertyName(property?.name);
    if (!name) {
        return;
    }
    try {
        await hubspotClient.crm.properties.coreApi.getByName(objectTypeId, name);
        return;
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status && status !== 404) {
            throw err;
        }
    }
    const payload = {
        ...property,
        name
    };
    await hubspotClient.crm.properties.coreApi.create(objectTypeId, payload);
}

async function ensureCustomObjectProperties(hubspotClient, objectTypeId, objectDefinition) {
    const normalized = normalizeObjectDefinition(objectDefinition);
    for (const property of normalized.properties || []) {
        await ensureCustomObjectProperty(hubspotClient, objectTypeId, property);
    }
}

function isInvalidPrimaryDisplayPropertyError(err) {
    const message = err?.response?.body?.message || err?.message || "";
    return message.includes("INVALID_PRIMARY_DISPLAY_PROPERTY")
        || message.includes("primaryDisplayProperty");
}

async function forcePrimaryDisplayProperty(hubspotClient, objectTypeId, fallbackPropertyName) {
    const payload = {
        primaryDisplayProperty: toHubspotPropertyName(fallbackPropertyName)
    };
    return hubspotClient.crm.schemas.coreApi.update(objectTypeId, payload);
}

export const BILLING_SUBSCRIPTION_OBJECT = {
    name: "billing_subscription",
    labels: {
        singular: `${DEFAULT_LABEL_PREFIX} Billing Subscription`,
        plural: `${DEFAULT_LABEL_PREFIX} Billing Subscriptions`
    },
    primaryDisplayProperty: "ghl_contact_id",
    searchableProperties: ["ghl_contact_id"],
    requiredProperties: ["ghl_contact_id"],
    properties: [

        {
            name: "ghl_contact_id",
            label: "GHL Contact ID",
            type: "string",
            fieldType: "text"
        },
        {
            name: "apex_account_status",
            label: "Status",
            type: "string",
            fieldType: "text"
        },
        {
            name: "deactivated_date",
            label: "Deactivated Date",
            type: "date",
            fieldType: "date"
        },
        {
            name: "paymentmethod",
            label: "Payment Method",
            type: "enumeration",
            fieldType: "select",
            options: [
                { label: "Card", value: "card" },
                { label: "ACH", value: "ach" },
                { label: "RCC", value: "rcc" }
            ]
        },
        {
            name: "upcoming_payment_date",
            label: "Next Payment Date",
            type: "date",
            fieldType: "date"
        },
        {
            name: "last_payment_status",
            label: "Last Payment Status",
            type: "enumeration",
            fieldType: "select",
            options: [
                { label: "Declined", value: "declined" },
                { label: "Settled", value: "settled" },
                { label: "Voided", value: "voided" },
                { label: "Refunded", value: "refunded" },
                { label: "Failed", value: "failed" },
                { label: "Pending", value: "pending" },
                { label: "NSF", value: "nsf" },
                { label: "Pushed", value: "pushed" }
            ]
        },
        {
            name: "last_payment_amount",
            label: "Last Payment Amount",
            type: "number",
            fieldType: "number"
        },
        {
            name: "last_payment_date",
            label: "Last Payment Date",
            type: "date",
            fieldType: "date"
        },
        {
            name: "apex_draft_status",
            label: "Apex Draft Status",
            type: "enumeration",
            fieldType: "select",
            options: [
                { label: "Active", value: "active" },
                { label: "Paused", value: "paused" }
            ]
        },
        {
            name: "apex_upcoming_payment_amount",
            label: "Next Payment Amount",
            type: "number",
            fieldType: "number"
        },
        {
            name: "apex_installment_frequency",
            label: "Installment Frequency",
            type: "enumeration",
            fieldType: "select",
            options: [
                { label: "Daily", value: "daily" },
                { label: "Weekly", value: "weekly" },
                { label: "Monthly", value: "monthly" },
                { label: "Annual", value: "annual" },
                { label: "One-Time", value: "one_time" },
            ]
        },
        {
            name: "apex_enrollment_date",
            label: "Enrollment Date",
            type: "date",
            fieldType: "date"
        },
        {
            name: "apex_contracted_services",
            label: "Contracted Services",
            type: "date",
            fieldType: "date"
        },
        {
            name: "first_payment_amount",
            label: "First Payment Amount",
            type: "number",
            fieldType: "number"
        },
        {
            name: "lead_value",
            label: "Lead value",
            type: "number",
            fieldType: "number"
        },
        {
            name: "installment_start_date",
            label: "Installment Date",
            type: "date",
            fieldType: "date"
        },



    ]
};

export async function createCustomObject(hubspotClient, objectDefinition) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    if (!objectDefinition?.name) {
        throw new Error("objectDefinition.name is required");
    }
    try {
        return await createCustomObjectSchema(hubspotClient, objectDefinition);
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 409) {
            const objectName = toHubspotObjectName(objectDefinition.name);
            const existing = await getCustomObjectSchemaByName(hubspotClient, objectName);
            if (!existing) {
                throw new Error(`Custom object not found: ${objectName}`);
            }
            const objectTypeId = existing?.objectTypeId;
            if (!objectTypeId) {
                throw new Error(`Custom object type id missing for: ${objectName}`);
            }
            try {
                await ensureCustomObjectProperties(hubspotClient, objectTypeId, objectDefinition);
            } catch (propertyErr) {
                if (isInvalidPrimaryDisplayPropertyError(propertyErr)) {
                    await forcePrimaryDisplayProperty(hubspotClient, objectTypeId, "hs_object_id");
                    await ensureCustomObjectProperties(hubspotClient, objectTypeId, objectDefinition);
                } else {
                    throw propertyErr;
                }
            }
            try {
                return await updateCustomObjectSchema(hubspotClient, objectTypeId, objectDefinition);
            } catch (updateErr) {
                if (isInvalidPrimaryDisplayPropertyError(updateErr)) {
                    await forcePrimaryDisplayProperty(hubspotClient, objectTypeId, "hs_object_id");
                    return updateCustomObjectSchema(hubspotClient, objectTypeId, objectDefinition);
                }
                throw updateErr;
            }
        }
        throw err;
    }
}

export async function deleteCustomObject(hubspotClient, objectType) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    if (!objectType) {
        throw new Error("objectType is required");
    }
    return deleteCustomObjectSchema(hubspotClient, objectType);
}

export async function createBillingSubscriptionObject(accessToken) {
    const hubspotClient = buildHubspotClient(accessToken);
    return createCustomObject(hubspotClient, BILLING_SUBSCRIPTION_OBJECT);
}

const CUSTOM_OBJECTS = [
    BILLING_SUBSCRIPTION_OBJECT
];

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
        if (key === "create" || key === "delete") {
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
Usage: node hubspot/customObjects/createCustomObjects.mjs [options]

Options:
  --entity <objectName>    Custom object name (e.g. billing_subscription)
  --create                 Create the custom object
  --delete                 Delete the custom object
  --help                   Show this help message
`);
}

function findObjectDefinition(entity) {
    const normalized = toHubspotObjectName(entity);
    return CUSTOM_OBJECTS.find((obj) => toHubspotObjectName(obj?.name) === normalized);
}

async function runCustomObjectsCli({ entity, create = false, deleteMode = false } = {}) {
    if (!entity) {
        throw new Error("entity is required");
    }
    if (!create && !deleteMode) {
        throw new Error("Specify --create or --delete");
    }
    const objectDefinition = findObjectDefinition(entity);
    if (!objectDefinition) {
        throw new Error(`No custom object configured for entity: ${entity}`);
    }
    const hubspotClient = buildHubspotClient();
    if (create) {
        const result = await createCustomObject(hubspotClient, objectDefinition);
        console.log("create complete:", result);
    }
    if (deleteMode) {
        const objectName = toHubspotObjectName(objectDefinition.name);
        const existing = await getCustomObjectSchemaByName(hubspotClient, objectName);
        if (!existing?.objectTypeId) {
            throw new Error(`Custom object type id missing for: ${objectName}`);
        }
        const objectTypeId = existing.objectTypeId;
        const result = await deleteCustomObject(hubspotClient, objectTypeId);
        console.log("delete complete:", result);
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
    runCustomObjectsCli({
        entity: cli.entity,
        create: cli.create,
        deleteMode: cli.delete
    }).catch((err) => {
        console.error("custom object runner failed:", err?.message || err);
        process.exit(1);
    });
}
