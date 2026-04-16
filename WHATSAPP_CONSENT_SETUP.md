# WhatsApp Consent Setup

This backend now supports:

1. Manual proof-based opt-in from Contacts / Team Inbox
2. Public website or landing-page opt-in
3. Meta Lead Ads preview, consent sync, and webhook auto-sync

## Required Backend Environment Variables

Add these values in backend environment:

```env
WHATSAPP_OPTIN_PUBLIC_KEY=generate_a_long_random_public_opt_in_key
WHATSAPP_MARKETING_TEMPLATE_MAX_PER_24H=1
WHATSAPP_MARKETING_TEMPLATE_WINDOW_HOURS=24
PUBLIC_BACKEND_URL=https://your-backend-domain.onrender.com
FRONTEND_URL=https://your-frontend-domain.vercel.app
META_APP_ID=your_meta_app_id
META_APP_SECRET=your_meta_app_secret
META_API_VERSION=v22.0
META_TOKEN_ENCRYPTION_KEY=32_characters_minimum_random_key
JWT_SECRET=generate_a_secure_random_secret
META_LEAD_WEBHOOK_VERIFY_TOKEN=your_meta_lead_webhook_verify_token
META_LEAD_PHONE_KEYS=phone number, phone, mobile, whatsapp number
META_LEAD_NAME_KEYS=full name, name
META_LEAD_EMAIL_KEYS=email, email address
META_LEAD_CONSENT_KEYS=whatsapp consent, receive whatsapp updates, consent
META_LEAD_APPROVED_VALUES=yes, true, checked
META_LEAD_CONSENT_TEXT=Meta lead form consent for WhatsApp marketing updates.
META_LEAD_SCOPE=marketing
CONSENT_EXPORT_EMAIL_ENABLED=false
CONSENT_EXPORT_EMAIL_FROM=no-reply@example.com
CONSENT_EXPORT_MAX_ROWS=5000
CONSENT_LOG_ARCHIVE_ENABLED=false
CONSENT_LOG_RETENTION_DAYS=365
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASS=
SMTP_SECURE=false
```

Notes:
- `WHATSAPP_OPTIN_PUBLIC_KEY` is required for `POST /api/public/whatsapp-opt-in`
- Marketing template throttles are configured via:
  - `WHATSAPP_MARKETING_TEMPLATE_MAX_PER_24H`
  - `WHATSAPP_MARKETING_TEMPLATE_WINDOW_HOURS`
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

Production landing route:

```text
/whatsapp-opt-in
```

## Meta Lead Ads Consent Sync

Available backend endpoints:

```http
GET  /api/meta-ads/leads/:leadId/preview
POST /api/meta-ads/leads/sync-consent
GET  /api/meta-ads/webhook
POST /api/meta-ads/webhook
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

## Meta Lead Ads Webhook (Auto Sync)

Configure a Meta leadgen webhook to:

```
https://your-backend-domain/api/meta-ads/webhook
```

Verification token:

```
META_LEAD_WEBHOOK_VERIFY_TOKEN
```

The webhook will:
- verify signature using `META_APP_SECRET`
- resolve the user by selected page ID
- apply consent only when approved mapping is detected

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
- `whatsappMarketingWindowStartedAt`
- `whatsappMarketingSendCount`
- `whatsappMarketingLastSentAt`
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

## Consent Export Email (Optional)

Endpoint:

```http
POST /api/consent/export-email
GET  /api/consent/export-jobs
```

Required:
- SMTP config
- `CONSENT_EXPORT_EMAIL_ENABLED=true`
- `email` in payload

Exports are processed by the scheduler and emailed as CSV attachments.

## Retention / Archive Policy (Optional)

Set:

```env
CONSENT_LOG_ARCHIVE_ENABLED=true
CONSENT_LOG_RETENTION_DAYS=365
```

Older logs are archived nightly and excluded from default views.

## Important Safety Note

Do not blindly mark imported contacts as opted in unless you have valid prior consent.

Best practice:
- store proof
- store source
- store consent text snapshot
- use audit modal before sending marketing templates
