import { Client } from "@hubspot/api-client";


const hubspotClient = new Client({
    accessToken: process.env.HUBSPOT_ACCESS_TOKEN,
});


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
        const result = await upsertContactByEmail("jane.doe@example.com", {
            firstname: "Jane",
            lastname: "Doe",
            phone: "+15125551212",
            company: "Acme Inc",
        });

        console.log(result.action, result.id);
        console.log(result.properties);
    } catch (e) {
        console.error("Upsert failed:", e.message);
        if (e.response?.body) console.error("HubSpot details:", e.response.body);
    }
})();

