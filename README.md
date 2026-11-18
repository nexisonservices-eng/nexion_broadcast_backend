"# broadcast-backend" 
"# nexion_broadcast_backend" 

## WhatsApp template creation

Set `WHATSAPP_TEMPLATE_CREATE_GRAPH_VERSION=v26.0` on the broadcast backend for
the template creation rollout. When the setting is absent, creation defaults to
`v20.0`. This setting covers creation and resubmission. WhatsApp messaging, media
upload, and template listing, synchronization, and deletion use their existing
Graph version.

Template writes load the latest WhatsApp credential set from the Admin API.
The selected WhatsApp Token must have `whatsapp_business_management` and access
to the client's WhatsApp Business Account. Meta rejections can be correlated
with `[WHATSAPP_TEMPLATE_CREATE]` logs using the local template ID and Meta
trace ID when available.
