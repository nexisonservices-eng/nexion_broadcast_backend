# WhatsApp Consent Deploy Checklist

Use this checklist when rolling out the consent system to production.

## Backend Environment

Set these environment variables in Render:

- `MONGODB_URI`
- `JWT_SECRET`
- `PUBLIC_BACKEND_URL`
- `FRONTEND_URL`
- `META_APP_ID`
- `META_APP_SECRET`
- `META_API_VERSION`
- `META_TOKEN_ENCRYPTION_KEY`
- `WHATSAPP_OPTIN_PUBLIC_KEY`
- `WHATSAPP_MARKETING_TEMPLATE_MAX_PER_24H`
- `WHATSAPP_MARKETING_TEMPLATE_WINDOW_HOURS`
- `META_LEAD_WEBHOOK_VERIFY_TOKEN`
- `META_LEAD_PHONE_KEYS`
- `META_LEAD_NAME_KEYS`
- `META_LEAD_EMAIL_KEYS`
- `META_LEAD_CONSENT_KEYS`
- `META_LEAD_APPROVED_VALUES`
- `META_LEAD_CONSENT_TEXT`
- `META_LEAD_SCOPE`
- `CONSENT_EXPORT_EMAIL_ENABLED`
- `CONSENT_EXPORT_EMAIL_FROM`
- `CONSENT_EXPORT_MAX_ROWS`
- `CONSENT_LOG_ARCHIVE_ENABLED`
- `CONSENT_LOG_RETENTION_DAYS`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `SMTP_SECURE`

Recommended:
- use a long random value for `WHATSAPP_OPTIN_PUBLIC_KEY`
- use a 32+ character random value for `META_TOKEN_ENCRYPTION_KEY`

## Frontend Environment

Make sure the frontend points to the correct backend:

- `VITE_API_URL`
- `VITE_API_BASE_URL`
- `VITE_SOCKET_URL`
- `VITE_WS_URL`
- `VITE_APP_BASE_PATH`
- `VITE_APP_BASENAME`

## Deploy Order

1. Deploy backend first
2. Verify backend health endpoint
3. Verify Render environment variables are present
4. Deploy frontend
5. Hard refresh browser after deploy

## Manual Verification

### Contacts / Team Inbox proof flow

1. Open Contacts
2. Choose a contact
3. Click `Mark Opted In`
4. Fill:
   - source
   - consent text
   - proof type
5. Save
6. Open `View Consent Audit`
7. Confirm audit values are visible

### Public opt-in

1. Open frontend route:
   - `/whatsapp-opt-in-demo`
   - `/whatsapp-opt-in` (production landing)
2. Enter:
   - backend URL
   - public key
   - userId
   - phone
   - consent text
3. Submit
4. Open the same contact in Contacts
5. Confirm `opted_in` and audit proof fields are saved

### Meta Lead consent sync

1. Open Meta Connect
2. Enter a real `leadId`
3. Click `Preview Lead`
4. Confirm:
   - phone field matches
   - consent field matches
   - approved answer values are correct
5. Click `Sync Lead Consent`
6. Open the contact and verify consent audit

### Meta Lead webhook

1. Configure Meta Lead Ads webhook URL:
   - `/api/meta-ads/webhook`
2. Set verify token to `META_LEAD_WEBHOOK_VERIFY_TOKEN`
3. Trigger a test lead submission
4. Confirm contact is created/updated with `meta_lead_ads` proof

### Consent export email

1. Set SMTP env values
2. Set `CONSENT_EXPORT_EMAIL_ENABLED=true`
3. POST `/api/consent/export-email` with an email
4. GET `/api/consent/export-jobs` to confirm job status
5. Verify CSV is delivered

### Consent retention

1. Set `CONSENT_LOG_ARCHIVE_ENABLED=true`
2. Set `CONSENT_LOG_RETENTION_DAYS`
3. Verify old logs are archived nightly

## Broadcast Safety Check

Before sending a marketing template:

1. Confirm contact is `opted_in`
2. Confirm contact is not `opted_out`
3. Confirm template category is correct
4. Confirm audience validation passes

## Known Safe Behavior

- marketing template send is blocked without valid opt-in
- marketing template sends are throttled per contact (default 1 per 24h)
- opted-out contacts are blocked from marketing outreach
- proof-backed opt-in is enforced server-side
- public forms require `WHATSAPP_OPTIN_PUBLIC_KEY`
- Meta lead sync is mapping-driven, not blind auto-sync

## Recommended Next Operations Step

If this deploy is successful, the next low-risk enhancement is:

- save reusable per-form Meta lead mappings

That will reduce manual mapping work while staying safe.
