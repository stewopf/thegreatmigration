import { Client } from "@hubspot/api-client";

const DEFAULT_FIELD_TYPE = "text";
const DEFAULT_PROPERTY_TYPE = "string";

function toHubspotPropertyName(name) {
    return `${name || ""}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 50);
}

function normalizeOptions(options = []) {
    return options
        .filter((option) => option !== null && option !== undefined)
        .map((option) => {
            if (typeof option === "string") {
                const label = option;
                return { label, value: toHubspotPropertyName(label) || "option" };
            }
            const label = option?.label || option?.value || option?.name || "Option";
            const value = option?.value || toHubspotPropertyName(label) || "option";
            return { ...option, label, value };
        });
}

function normalizeFieldDefinition(field) {
    if (typeof field === "string") {
        return {
            name: toHubspotPropertyName(field),
            label: field,
            type: DEFAULT_PROPERTY_TYPE,
            fieldType: DEFAULT_FIELD_TYPE
        };
    }
    const label = field?.label || field?.name || field?.fieldKey || "Custom Field";
    const name = toHubspotPropertyName(field?.name || label) || "custom";
    const options = normalizeOptions(field?.options || []);
    let type = field?.type || (options.length > 0 ? "enumeration" : DEFAULT_PROPERTY_TYPE);
    if (typeof type === "string") {
        const normalizedType = type.toLowerCase();
        const typeAliases = {
            boolean: "bool",
            bool: "bool",
            string: "string",
            enumeration: "enumeration",
            number: "number",
            numeric: "number",
            datetime: "datetime",
            date: "date",
            phone: "phone_number",
            phone_number: "phone_number"
        };
        type = typeAliases[normalizedType] || type;
    }
    const fieldType = field?.fieldType
        || (type === "enumeration" ? "select" : DEFAULT_FIELD_TYPE);
    const normalizedOptions = [...options];
    if (type === "bool" && normalizedOptions.length === 0) {
        normalizedOptions.push(
            { label: "True", value: "true" },
            { label: "False", value: "false" }
        );
    }
    if (type === "bool") {
        // HubSpot booleancheckbox expects type: enumeration with true/false options.
        return {
            ...field,
            name,
            label,
            type: "enumeration",
            fieldType: "booleancheckbox",
            options: normalizedOptions
        };
    }
    return {
        ...field,
        name,
        label,
        type,
        fieldType,
        options: normalizedOptions
    };
}

async function createProperty(hubspotClient, objectType, field) {
    try {
        await hubspotClient.crm.properties.coreApi.create(objectType, field);
        return { name: field.name, action: "created" };
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 409) {
            return { name: field.name, action: "exists" };
        }
        throw err;
    }
}

async function deleteProperty(hubspotClient, objectType, name) {
    try {
        await hubspotClient.crm.properties.coreApi.archive(objectType, name);
        return { name, action: "deleted" };
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status === 404) {
            return { name, action: "missing" };
        }
        throw err;
    }
}

async function resolveDefaultGroupName(hubspotClient, objectType) {
    const groups = await hubspotClient.crm.properties.groupsApi.getAll(objectType);
    const fallback = Array.isArray(groups) ? groups[0] : groups?.results?.[0];
    const groupName = fallback?.name;
    if (!groupName) {
        throw new Error(`No property group found for ${objectType}`);
    }
    console.log(`Default group name: ${groupName}`);
    return groupName;
}

function toGroupLabel(groupName) {
    return String(groupName || "")
        .replace(/[_-]+/g, " ")
        .replace(/\b\w/g, (char) => char.toUpperCase())
        .trim() || "Custom Fields";
}

async function listGroupNames(hubspotClient, objectType) {
    const groups = await hubspotClient.crm.properties.groupsApi.getAll(objectType);
    const list = Array.isArray(groups) ? groups : groups?.results || [];
    return new Set(list.map((group) => group?.name).filter(Boolean));
}

async function ensurePropertyGroup(hubspotClient, objectType, groupName, knownGroups) {
    if (!groupName) {
        return;
    }
    if (knownGroups?.has(groupName)) {
        return;
    }
    const payload = {
        name: groupName,
        label: toGroupLabel(groupName)
    };
    try {
        await hubspotClient.crm.properties.groupsApi.create(objectType, payload);
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status !== 409) {
            throw err;
        }
    }
    if (knownGroups) {
        knownGroups.add(groupName);
    }
}

/**
 * Create custom fields on a HubSpot object type.
 */
export async function createHubspotCustomFieldsOnObjectType(
    hubspotClient,
    objectType,
    fields = [],
    { dryRun = false } = {}
) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    if (!objectType) {
        throw new Error("objectType is required");
    }

    let defaultGroupName;
    let knownGroups;
    const results = [];
    for (const field of fields) {
        const normalized = normalizeFieldDefinition(field);
        if (normalized.groupName) {
            if (!knownGroups) {
                knownGroups = await listGroupNames(hubspotClient, objectType);
            }
            await ensurePropertyGroup(hubspotClient, objectType, normalized.groupName, knownGroups);
        }
        if (!defaultGroupName && !normalized.groupName) {
            defaultGroupName = await resolveDefaultGroupName(hubspotClient, objectType);
        }
        const payload = {
            name: normalized.name,
            label: normalized.label,
            type: normalized.type,
            fieldType: normalized.fieldType,
            groupName: normalized.groupName || defaultGroupName
        };
        if (normalized.description) {
            payload.description = normalized.description;
        }
        if (normalized.defaultValue !== undefined) {
            payload.defaultValue = normalized.defaultValue;
        }
        if (Array.isArray(normalized.options) && normalized.options.length > 0) {
            payload.options = normalized.options;
        }
        if (dryRun) {
            console.log(`[dry-run] create property on ${objectType}`, payload.name);
            results.push({ name: payload.name, action: "dry-run" });
            continue;
        }
        const result = await createProperty(hubspotClient, objectType, payload);
        results.push(result);
    }
    return results;
}

export async function deleteHubspotCustomFieldsOnObjectType(
    hubspotClient,
    objectType,
    fields = [],
    { dryRun = false } = {}
) {
    if (!hubspotClient) {
        throw new Error("hubspotClient is required");
    }
    if (!objectType) {
        throw new Error("objectType is required");
    }

    const results = [];
    for (const field of fields) {
        const name = typeof field === "string"
            ? toHubspotPropertyName(field)
            : toHubspotPropertyName(field?.name || field?.propertyName || "");
        if (!name) {
            continue;
        }
        if (dryRun) {
            console.log(`[dry-run] delete property on ${objectType}`, name);
            results.push({ name, action: "dry-run" });
            continue;
        }
        const result = await deleteProperty(hubspotClient, objectType, name);
        results.push(result);
    }
    return results;
}

export function buildHubspotClient(accessToken = process.env.HUBSPOT_ACCESS_TOKEN) {
    if (!accessToken) {
        throw new Error("HUBSPOT_ACCESS_TOKEN is not set");
    }
    return new Client({ accessToken });
}

export async function createHubspotCustomFields(customFields = {}) {
    const hubspotClient = buildHubspotClient();
    return createHubspotCustomFieldsOnObjectType(hubspotClient, customFields.objectType, customFields.fields);
}

export async function deleteHubspotCustomFields(customFields = {}) {
    const hubspotClient = buildHubspotClient();
    return deleteHubspotCustomFieldsOnObjectType(hubspotClient, customFields.objectType, customFields.fields);
}
const CONTACT_GROUP_NAME = "ghl_contacts";
const COMPANY_GROUP_NAME = "via_business_details";
const WEBSITE_GROUP_NAME = "via_website_details";

export const CUSTOM_FIELDS = [
    {objectType: "contacts", fields: [
        {
            name: "ghl_contact_id",
            label: "GHL Contact ID",
            type: "string",
            fieldType: "text",
            groupName: CONTACT_GROUP_NAME,
        },
        {
            name: "ghl_created_date",
            label: "GHL Created Date",
            type: "date",
            fieldType: "date",
            groupName: CONTACT_GROUP_NAME,
        },
        {
            name: "contactlanguage",
            label: "Contact Language",
            type: "enumeration",
            fieldType: "select",
            options: [
                { label: "English", value: "en" },
                { label: "Spanish", value: "es" },
            ],
            defaultValue: "en",
            groupName: CONTACT_GROUP_NAME,
        },
        {
            name:"dateofbirth",
            label: "Date of Birth",
            type: "date",
            fieldType: "date",
            defaultValue: "1990-01-01",
            groupName: CONTACT_GROUP_NAME,
        },
        {
            name:"dnd",
            label: "DND",
            type: "boolean",
            fieldType: "checkbox",
            defaultValue: false,
            groupName: CONTACT_GROUP_NAME,
        },
        {
            name:"sfaccountid",
            label: "SF Account ID",
            type: "string",
            fieldType: "text",
            groupName: CONTACT_GROUP_NAME,
        },
        {
            name:"smsoptin",
            label: "SMS Opt In",
            type: "boolean",
            fieldType: "checkbox",
            defaultValue: false,
            groupName: CONTACT_GROUP_NAME,
        }
    ]},
    {objectType:"company", fields: [
        {
            name: "ghl_created_date",
            label: "Created Date",
            type: "date",
            fieldType: "date",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "services_offered",
            label: "Services Offered",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "hours_of_operation",
            label: "Hours of Operation",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "sf_reason_lost",
            label: "SF Reason Lost",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "contracted_services",
            label: "Contracted Services",
            type: "enumeration",
            fieldType: "checkbox",
            options: [
                "Listings Management",
                "Website Development",
                "Custom Domain",
                "Premium Domain",
                "Custom Logo",
                "Domain Integration",
                "Custom Email"
            ],
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "monthly_gross_sales",
            label: "Monthly Gross Sales",
            type: "number",
            fieldType: "number",
            groupName: COMPANY_GROUP_NAME,
        },
        // TODO: Creation of website? selection...?
        {
            name: "time_in_business_at_creation",
            label: "Time in Business at Creation (years)",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "apex_new_ach_request_url", // TODO: change to apex_new_ach_request
            label: "ACH Url",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "apex_new_rcc_request_url",
            label: "RCC Url",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },
        {
            name: "apex_new_card_request_url",
            label: "Card Url",
            type: "string",
            fieldType: "text",
            groupName: COMPANY_GROUP_NAME,
        },


    // Website Details Group
        {
            name: "existing_logo",
            label: "Existing Logo",
            type: "boolean",
            fieldType: "checkbox",
            groupName: WEBSITE_GROUP_NAME,
        },
        {
            name: "preferred_website_language",
            label: "Preferred Website Language",
            type: "enumeration",
            fieldType: "select",
            options: [
                { label: "English", value: "en" },
                { label: "Spanish", value: "es" },
            ],
            defaultValue: "en",
            groupName: WEBSITE_GROUP_NAME,
        },
        {
            name: "website_features",
            label: "Website Features",
            type: "enumeration",
            fieldType: "checkbox",
            options: [
                "Appointment/Reservations Tool",
                "Product Catalog"
            ],
            groupName: WEBSITE_GROUP_NAME,
        },

        {
            name: "cancellation_reason",
            label: "Cancellation Reason",
            type: "string",
            fieldType: "text",
            groupName: WEBSITE_GROUP_NAME,
        },
        {
            name: "sf_industry",
            label: "SF Industry",
            type: "string",
            fieldType: "text",
            groupName: WEBSITE_GROUP_NAME,
        },
        {
            name: "via_industry",
            label: "VIA Industry",
            type: "enumeration",
            fieldType: "select",
            options: [
                "Auto Repair/Shop",
                "Transportation",
                "Auto Wash/Detailing",
                "Restaurants & Bars",
                "Catering",
                "Construction & Remodeling",
                "Construction Materials",
                "Painting",
                "Solar Energy Contractor",
                "Metalwork",
                "Asphalt & Concrete",
                "Landscaping",
                "Distributors General",
                "Produce Wholesale",
                "Property Management",
                "Real Estate Agent/Agency",
                "Event Property",
                "Cleaning",
                "Security",
                "Plumbing",
                "Contractor General",
                "HVAC",
                "Electricians",
                "Store General",
                "Clothing Store",
                "Furniture Store",
                "Clinics & Hospitals",
                "Therapist",
                "Gym/Personal Trainer",
                "Pharmacy",
                "Lawyers",
                "Insurance",
                "Accountants & Tax",
                "Beauty Salon/Nails",
                "Barber Shop",
                "Spas",
                "Other",
                "Currier Service",
                "Junk Removal",
                "Beauty Salon",
                "Nails Center",
                "Food Truck"
            ],
            groupName: WEBSITE_GROUP_NAME,
        },
        {
            name: "website_url",
            label: "Website",
            type: "string",
            fieldType: "text",
            groupName: WEBSITE_GROUP_NAME,
        },

        // TODO: if the entity/contact
        {
            name: "apex_id",
            label: "Apex Id",
            type: "string",
            fieldType: "text",
            groupName: WEBSITE_GROUP_NAME,
        },
    ]},
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
Usage: node hubspot/customFields/createCustomObject.mjs [options]

Options:
  --entity <objectType>   HubSpot object type (e.g. contacts, deals)
  --create                Create custom fields for the entity
  --delete                Delete custom fields for the entity
  --help                  Show this help message
`);
}

function findCustomFieldsForEntity(entity) {
    const normalized = String(entity || "").trim().toLowerCase();
    return CUSTOM_FIELDS.find((item) => String(item?.objectType || "").toLowerCase() === normalized);
}

async function runCustomFieldsCli({
    entity,
    create = false,
    deleteMode = false
} = {}) {
    if (!entity) {
        throw new Error("entity is required");
    }
    if (!create && !deleteMode) {
        throw new Error("Specify --create or --delete");
    }
    const config = findCustomFieldsForEntity(entity);
    if (!config) {
        throw new Error(`No custom fields configured for entity: ${entity}`);
    }
    if (create) {
        const result = await createHubspotCustomFields(config);
        console.log("create complete:", result);
    }
    if (deleteMode) {
        const result = await deleteHubspotCustomFields(config);
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
    runCustomFieldsCli({
        entity: cli.entity,
        create: cli.create,
        deleteMode: cli.delete
    }).catch((err) => {
        console.error("custom fields runner failed:", err?.message || err);
        process.exit(1);
    });
}
