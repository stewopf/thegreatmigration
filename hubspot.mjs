import { Client } from "@hubspot/api-client";


const hubspotClient = new Client({
    accessToken: process.env.HUBSPOT_ACCESS_TOKEN,
});

const CONTACT_OBJECT_TYPE = "contacts";
const GHL_ID_PROPERTY = "ghl_contact_id";

function toHubspotPropertyName(name) {
    return `${name}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 50);
}

async function ensureContactProperty({ name, label, type = "string", fieldType = "text" }) {
    try {
        await hubspotClient.crm.properties.coreApi.getByName(CONTACT_OBJECT_TYPE, name);
        return;
    } catch (err) {
        const status = err?.code || err?.response?.statusCode || err?.response?.status;
        if (status && status !== 404) {
            throw err;
        }
    }
    await hubspotClient.crm.properties.coreApi.create(CONTACT_OBJECT_TYPE, {
        name,
        label,
        type,
        fieldType
    });
}

async function ensureGhlCustomFields(customFields = []) {
    await ensureContactProperty({
        name: GHL_ID_PROPERTY,
        label: "GHL Contact ID",
        type: "string",
        fieldType: "text"
    });
    for (const field of customFields) {
        const propName = toHubspotPropertyName(field?.name || "custom");
        const label = field?.name || "GHL Custom Field";
        await ensureContactProperty({
            name: propName,
            label,
            type: "string",
            fieldType: "text"
        });
    }
}

function mapGhlContactToHubspotProperties(ghlContact) {
    const properties = {
        email: ghlContact.email || undefined,
        firstname: ghlContact.firstNameRaw || ghlContact.firstName || undefined,
        lastname: ghlContact.lastNameRaw || ghlContact.lastName || undefined,
        phone: ghlContact.phone || undefined,
        company: ghlContact.companyName || undefined,
        address: ghlContact.address1 || undefined,
        city: ghlContact.city || undefined,
        state: ghlContact.state || undefined,
        zip: ghlContact.postalCode || undefined,
        country: ghlContact.country || undefined,
        website: ghlContact.website || undefined,
        [GHL_ID_PROPERTY]: ghlContact.id || undefined
    };
    for (const field of ghlContact.customFields || []) {
        const propName = toHubspotPropertyName(field?.name || "custom");
        let value = field?.value;
        if (Array.isArray(value)) {
            value = value.join(", ");
        }
        if (value !== undefined && value !== null) {
            properties[propName] = String(value);
        }
    }
    return properties;
}

export async function upsertGhlContact(ghlContact) {
    if (!ghlContact?.email) {
        throw new Error("GHL contact email is required to upsert in HubSpot");
    }
    await ensureGhlCustomFields(ghlContact.customFields || []);
    const properties = mapGhlContactToHubspotProperties(ghlContact);
    return upsertContactByEmail(ghlContact.email, properties);
}


/**
 * Upsert a HubSpot contact by email.
 * - If a contact exists with the email, updates it.
 * - Otherwise, creates it.
 */
async function upsertContactByEmail(email, properties = {}) {
    if (!email) throw new Error("email is required");

    // Ensure email is included (HubSpot contact "email" property)
    const mergedProps = { ...properties, email };

    // 1) Search for existing contact by email
    const searchRequest = {
        filterGroups: [
            {
                filters: [
                    {
                        propertyName: "email",
                        operator: "EQ",
                        value: email,
                    },
                ],
            },
        ],
        // Ask HubSpot to return these properties in the response (optional)
        properties: Object.keys(mergedProps),
        limit: 1,
    };

    const searchResponse = await hubspotClient.crm.contacts.searchApi.doSearch(
        searchRequest
    );

    const existing = (searchResponse.results && searchResponse.results[0]) || null;

    // 2) Update if found, else create
    if (existing) {
        const contactId = existing.id;
        const updated = await hubspotClient.crm.contacts.basicApi.update(contactId, {
            properties: mergedProps,
        });
        return { action: "updated", id: updated.id, properties: updated.properties };
    } else {
        const created = await hubspotClient.crm.contacts.basicApi.create({
            properties: mergedProps,
        });
        return { action: "created", id: created.id, properties: created.properties };
    }
}

// ---- Example usage ----
(async () => {
    try {
        const ghlContact = {
            id: "UTQ1xBkY1xiQBRF3LdlX",
            email: "maribelmontes1118@gmail.com",
            firstNameRaw: "Marybel",
            lastNameRaw: "Montes",
            companyName: "Carnitas La Fogata LLC",
            phone: "+15055544951",
            customFields: [
                { name: "SF Account ID", value: "001QP00000u0f1SYAQ" },
                { name: "Salesforce Lead ID", value: "00QPc0000093WPZMA2" }
            ]
        };
        const result = await upsertGhlContact(ghlContact);

        console.log(result.action, result.id);
        console.log(result.properties);
    } catch (e) {
        console.error("Upsert failed:", e.message);
        if (e.response?.body) console.error("HubSpot details:", e.response.body);
    }
})();

