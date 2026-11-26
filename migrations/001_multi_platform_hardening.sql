-- Multi-Platform Isolation, Outbox Webhooks & Runtime Agent Config Hardening
-- Target schema: whatsapp_infra (shared Supabase PostgreSQL instance)
-- IMPORTANT: Run role password setup using a freshly generated secret before running DDL.

-- 1. Least-privilege runtime role for the Go calling server
-- DO $$
-- BEGIN
--   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wacalls_engine') THEN
--     CREATE ROLE wacalls_engine LOGIN PASSWORD 'CHANGE_ME_STRONG_RANDOM_PASSWORD';
--   END IF;
-- END $$;
-- GRANT USAGE, CREATE ON SCHEMA whatsapp_infra TO wacalls_engine;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA whatsapp_infra TO wacalls_engine;
-- GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA whatsapp_infra TO wacalls_engine;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA whatsapp_infra GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO wacalls_engine;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA whatsapp_infra GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO wacalls_engine;
-- ALTER ROLE wacalls_engine SET search_path TO whatsapp_infra;

-- 2. Platform ownership column on sessions & agent_configs
ALTER TABLE whatsapp_infra.whatsapp_sessions
    ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'local';

CREATE UNIQUE INDEX IF NOT EXISTS idx_whatsapp_sessions_unique_business
    ON whatsapp_infra.whatsapp_sessions (business_id);

ALTER TABLE whatsapp_infra.agent_configs
    ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'local',
    ADD COLUMN IF NOT EXISTS voice_enabled BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS auto_answer BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS auto_callback BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS callback_greeting TEXT,
    ADD COLUMN IF NOT EXISTS max_call_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS timezone TEXT,
    ADD COLUMN IF NOT EXISTS callback_keywords JSONB,
    ADD COLUMN IF NOT EXISTS tools_json JSONB;

-- 3. Scheduled calls observability columns
ALTER TABLE whatsapp_infra.scheduled_calls
    ADD COLUMN IF NOT EXISTS call_id TEXT,
    ADD COLUMN IF NOT EXISTS error_message TEXT;

-- 4. Durable per-partition ordered webhook outbox for multi-platform event delivery
CREATE TABLE IF NOT EXISTS whatsapp_infra.webhook_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL,
    session_id TEXT NOT NULL,
    business_id UUID NOT NULL,
    partition_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_pending_partition
    ON whatsapp_infra.webhook_outbox (platform, partition_key, id)
    WHERE status IN ('pending', 'delivering');

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_cleanup
    ON whatsapp_infra.webhook_outbox (status, delivered_at);

-- 5. Enable RLS across all tables in whatsapp_infra
-- Service-role (used by Local CRM Supabase Edge Functions) bypasses RLS automatically.
-- Direct PostgREST anon/authenticated JWT roles receive empty results with zero policies;
-- wacalls_engine is granted full access within whatsapp_infra via explicit RLS policy.
DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'whatsapp_infra'
    LOOP
        EXECUTE format('ALTER TABLE whatsapp_infra.%I ENABLE ROW LEVEL SECURITY', tbl.tablename);
        EXECUTE format('DROP POLICY IF EXISTS wacalls_engine_all ON whatsapp_infra.%I', tbl.tablename);
        EXECUTE format(
            'CREATE POLICY wacalls_engine_all ON whatsapp_infra.%I FOR ALL TO wacalls_engine USING (true) WITH CHECK (true)',
            tbl.tablename
        );
    END LOOP;
END $$;

