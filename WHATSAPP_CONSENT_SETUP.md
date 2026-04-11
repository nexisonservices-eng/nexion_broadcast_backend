# WhatsApp Consent Setup

This backend now supports:

1. Manual proof-based opt-in from Contacts / Team Inbox
2. Public website or landing-page opt-in
3. Meta Lead Ads preview and consent sync

## Required Backend Environment Variables

Add these values in backend environment:

```env
WHATSAPP_OPTIN_PUBLIC_KEY=generate_a_long_random_public_opt_in_key
PUBLIC_BACKEND_URL=https://your-backend-domain.onrender.com
FRONTEND_URL=https://your-frontend-domain.vercel.app
META_APP_ID=your_meta_app_id
META_APP_SECRET=your_meta_app_secret
META_API_VERSION=v22.0
META_TOKEN_ENCRYPTION_KEY=32_characters_minimum_random_key
JWT_SECRET=generate_a_secure_random_secret
```

Notes:
- `WHATSAPP_OPTIN_PUBLIC_KEY` is required for `POST /api/public/whatsapp-opt-in`
- Meta lead sync also requires a valid Meta connection for the current user

## Manual Opt-In Flow

Operators can capture proof-based consent from:

- Contacts page
- Team Inbox contact panel

Required proof fields:
- source
- consent text
- proof type

Optional:
- proof ID
- proof URL
- page URL
- scope

Audit details can be viewed from the UI using the consent audit modal.

## Public Website / Landing Page Opt-In

Endpoint:

```http
POST /api/public/whatsapp-opt-in
```

Required:
- `x-opt-in-public-key` header or `publicKey` body field
- `userId`
- `phone`
- `consentChecked = true`
- `consentText`

Recommended fields:
- `companyId`
- `name`
- `email`
- `source`
- `scope`
- `pageUrl`
- `proofId`
- `proofUrl`
- `metadata`

Example:

```bash
curl -X POST "https://your-backend-domain/api/public/whatsapp-opt-in" \
  -H "Content-Type: application/json" \
  -H "x-opt-in-public-key: YOUR_PUBLIC_KEY" \
  -d '{
    "userId": "USER_ID_HERE",
    "companyId": "COMPANY_ID_HERE",
    "name": "John Doe",
    "phone": "919876543210",
    "consentChecked": true,
    "consentText": "I agree to receive WhatsApp updates from Technovohub.",
    "source": "website_form",
    "scope": "marketing",
    "pageUrl": "https://technovahub.in/demo",
    "proofId": "landing-form-001"
  }'
```

Frontend demo route:

```text
/whatsapp-opt-in-demo
```

## Meta Lead Ads Consent Sync

Available backend endpoints:

```http
GET  /api/meta-ads/leads/:leadId/preview
POST /api/meta-ads/leads/sync-consent
```

Use preview first:
- confirm phone field
- confirm name field
- confirm consent field
- confirm approved answer values

Then run sync.

Recommended mapping example:

```json
{
  "leadId": "1234567890",
  "mapping": {
    "phoneFieldKeys": ["phone number", "phone"],
    "nameFieldKeys": ["full name", "name"],
    "emailFieldKeys": ["email"],
    "consentFieldKeys": ["whatsapp consent", "receive whatsapp updates"],
    "consentApprovedValues": ["yes", "true", "checked"],
    "consentText": "Meta lead form consent for WhatsApp marketing updates.",
    "scope": "marketing"
  }
}
```

This flow is also available inside Meta Connect UI.

## Stored Consent Fields

Contacts can now store:

- `whatsappOptInStatus`
- `whatsappOptInAt`
- `whatsappOptInSource`
- `whatsappOptInScope`
- `whatsappOptInTextSnapshot`
- `whatsappOptInProofType`
- `whatsappOptInProofId`
- `whatsappOptInProofUrl`
- `whatsappOptInCapturedBy`
- `whatsappOptInPageUrl`
- `whatsappOptInIp`
- `whatsappOptInUserAgent`
- `whatsappOptInMetadata`
- `whatsappOptOutAt`
- `lastInboundMessageAt`
- `serviceWindowClosesAt`

## Policy Enforcement

Current behavior:

- Marketing templates require valid WhatsApp opt-in
- Opted-out contacts are blocked from marketing outreach
- `STOP`, `UNSUBSCRIBE`, `CANCEL`, `REMOVE` can mark contacts opted out
- 24-hour service window logic controls free-form vs template-only messaging

## Recommended Production Order

1. Set backend env vars
2. Deploy backend
3. Test manual opt-in in Contacts
4. Test public opt-in via `/whatsapp-opt-in-demo`
5. Test Meta lead preview with a real `leadId`
6. Run Meta consent sync only after preview confirms field mapping

## Important Safety Note

Do not blindly mark imported contacts as opted in unless you have valid prior consent.

Best practice:
- store proof
- store source
- store consent text snapshot
- use audit modal before sending marketing templates
