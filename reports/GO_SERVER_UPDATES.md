# Go Engine & Local CRM Upgrade Plan for Multi-Platform Support (`GO_SERVER_UPDATES.md` — v2)

> **Target Repository**: `buildstart-calling-agent` (with coordinated minor updates in `buildstart-wacaller`)
> **Date**: 2026-11-26 (Revision 2 — incorporates evaluated Global developer review)
> **Inputs Reviewed**:
> - `calling-agent-global/reports/SYSTEM_ANALYSIS.md`
> - `calling-agent-global/reports/IMPLEMENTATION_PLAN.md`
> - Global Platform Developer's 15-point Architecture Review (2026-11-26)
> - Full source code of `buildstart-calling-agent` (`c3f1506`), `whatsmeow` SDK, and `buildstart-wacaller` (`a18511e`)

---

## 1. Executive Summary & Core Design Principle

The Global Platform (`calling-agent-global`) needs the shared Go server (`buildstart-calling-agent`) to act as a **unified multi-tenant WhatsApp Voice + Text Messaging engine** isolated by platform API keys, while `buildstart-wacaller` (**Local CRM**) is already running in production on this exact Go server and database.

### Non-Negotiable Golden Rule
**Zero regression to the Local CRM Voice Agent or Shared Supabase (~470 tenants).**
- Local CRM's voice quality, 1.2s auto-answer, pre-warmed greeting, 300ms barge-in, missed-call fallback SMS, 4s auto-callback, MinIO call recording playback (`Calls.tsx`), and `post-call-wacaller` Sinhala Lead Dossier webhook **must continue working identically**.
- Every new capability added for Global (`platform='global'`) either defaults to today's behavior for `platform='local'` or executes on an explicitly isolated `global` code path.

---

## 2. Core Engineering Corrections & Evaluation of the 15 Follow-Up Items

### 2.1 Original 7 Architectural Corrections (Confirmed & Agreed by Both Sides)
1. **RLS instead of removing `whatsapp_infra` from `PGRST_DB_SCHEMAS`**: Removing `whatsapp_infra` from `PGRST_DB_SCHEMAS` would break Local CRM's `agent-sync-wacaller`, `get-calls-wacaller`, and `schedule-call-wacaller` Edge Functions. Enabling RLS on all `whatsapp_infra` tables blocks `anon`/`authenticated` PostgREST access while allowing `service_role` (`BYPASSRLS`) and Go's DB role.
2. **30-Minute In-Memory `inboundMediaCache` (`whatsmeow` media constraint)**: `whatsmeow` does not persist received message protobufs/media keys in Postgres. Caching the ~250-byte encrypted `DownloadableMessage` descriptor per session (`30m` TTL, max `500` items) enables `GET /api/v1/sessions/{sid}/messages/{id}/media` on demand. *(Global's webhook handler will also download media promptly upon receipt.)*
3. **Bypassing `CallManager.HandleCallOffer` when `voice_enabled == false`**: Prevents `<preaccept>` relay allocation and prevents `OnEnded` from firing unwanted missed-call auto-callbacks when an agent is paused or out of credits.
4. **Self-Sent Message ID Tracking (`apiSentMsgIDs`)**: Distinguishes human replies typed on the owner's physical phone (`message.owner_sent`) from messages sent via Go's own API/fallback paths.
5. **Dual-Mode Webhook Dispatch**: Keeps `POST_CALL_WEBHOOK_URL` (`post-call-wacaller` flat JSON) 100% untouched for `platform='local'`, while using a dedicated HMAC-signed dispatcher for `platform='global'`.
6. **SSRF Protection on `media_url` + Bounded Webhook Workers**: Blocks loopback/private/link-local IPs (`127.0.0.0/8`, RFC1918, `169.254.0.0/16`, IPv6 local) and caps media downloads at `20 MB`.
7. **Keep Voice Previews Out of Go**: Voice previews run in Global's Deno Edge Function so non-call TTS synthesis never contends with real-time 20ms VoIP RTP loops.

---

### 2.2 Engineering Evaluation of the Global Developer's 15 Review Points

Every one of the 15 follow-up items was checked against `whatsmeow` internals, Go runtime behavior, Local CRM's `Calls.tsx` / Edge Functions, and the shared Hetzner Supabase stack:

| # | Global Developer Suggestion | Verdict & Engineering Decision (Local CRM Safe) | Phase |
| :---: | :--- | :--- | :---: |
| **1a** | **Dedicated least-privilege DB role for Go** (instead of `postgres` superuser) | **ADOPT.** Create dedicated Postgres role `wacalls_engine` granted access **only** to `whatsapp_infra` tables/sequences + an explicit RLS policy `TO wacalls_engine` (no `SUPERUSER`, no `BYPASSRLS`, zero access to `wacaller_customization` or the other ~470 tenant schemas). Update Go `.env` `DATABASE_URL` to use `wacalls_engine` on `127.0.0.1:5434`. | Phase 1 |
| **1b** | **Rotate leaked `postgres` superuser password** | **SEPARATE MAINTENANCE RUNBOOK (§9).** Note `supabase-db` is already bound only to `127.0.0.1:5434` (`0.0.0.0:5432` is Supavisor). Rotating `POSTGRES_PASSWORD` requires updating all shared Supabase containers across ~470 client apps — **never bundle that into a Go deploy**. We firewall external `5432`/`6543`/`9000` immediately in Phase 1 and provide a dedicated zero-surprise rotation runbook in §9. | Infra (§9) |
| **2** | **Persistent Postgres `webhook_outbox`** so restarts never drop call billing or incoming messages | **ADOPT.** Add `whatsapp_infra.webhook_outbox`. Global events insert synchronously (`<1ms` on local loopback) before returning, and workers deliver from the table with retries (`0s, 2s, 6s, 18s, 45s, 120s` $\rightarrow$ `dead`), surviving any Go restart. Never drops events under load. | Phase 2 |
| **3** | **Per-chat ordered delivery** so fast consecutive messages from one customer never arrive reversed | **ADOPT.** Each `webhook_outbox` row stores `partition_key = session_id + ":" + chat_jid`. `16` sharded workers (`fnv32a(partition_key) % 16`) claim and deliver events strictly in `id ASC` (`BIGSERIAL`) order per `partition_key`. | Phase 2 |
| **4** | **Enforce 1 session per `(platform, business_id)`** on `POST /api/sessions` | **ADOPT.** Make `SessionManager.Create` idempotent under mutex: if a session already exists for `business_id` on the caller's `platform`, return `200 {"id": existingID, "reused": true}` (`404` if owned by the other platform). Add `UNIQUE (business_id)` index on `whatsapp_infra.whatsapp_sessions`. | Phase 1 |
| **5** | **Inject current local date/time (`timezone` + `_ "time/tzdata"`) & caller phone** into prompt at call start (`global` only) | **ADOPT (`global` only).** Add nullable `timezone TEXT` to `agent_configs`. Refactor `onIncomingOffer` to resolve `callerPN`/`peerNum` *before* `createCall` so Global's `GeminiLiveAgent` receives a `[LIVE CALL CONTEXT]` block with local weekday/date/time + caller E.164 phone. Local CRM prompts remain 100% untouched. | Phase 2 |
| **6** | **Emit `call.missed` (`reason: "agent_disabled"`)** when `voice_enabled == false` | **ADOPT.** When `voice_enabled == false` on a `global` session, Go still skips `CallManager` / `<preaccept>` completely so the phone rings normally, and writes one deduplicated `call.missed` event (`reason: "agent_disabled"`, `auto_callback_scheduled: false`) to `webhook_outbox`. | Phase 2 |
| **7** | **Add `is_offline` flag on `message.received`** after server downtime/reconnect | **ADOPT (adapted to `whatsmeow` API).** Verified in `whatsmeow`: `events.Message` has no `IsOffline` field; instead `whatsmeow` brackets reconnect replay between `*events.OfflineSyncPreview` and `*events.OfflineSyncCompleted` (plus `evt.SourceWebMsg != nil` for history sync). Go will skip `SourceWebMsg != nil` history dumps, track `sess.inOfflineSync` (`OfflineSyncPreview` $\rightarrow$ `OfflineSyncCompleted` with 60s timeout), and set `is_offline = sess.inOfflineSync.Load() \|\| age > 120s` (also suppressing keyword auto-callbacks when `is_offline == true`). | Phase 3 |
| **8** | **Track self-sent message IDs across EVERY send path** (fallback SMS, media send, API text send) | **ADOPT.** Funnel `sendTextSync`, `sendWhatsAppMessage` (missed-call fallback), and `sendMediaSync` through a single `sendProtoMessage()` method on `Session` that records `resp.ID` in `sess.apiSentMsgIDs` (`10m` TTL) atomically around `client.SendMessage`. | Phase 3 |
| **9** | **Private call recordings** (don't leave Global recordings in a public MinIO bucket) | **ADOPT VIA DUAL-BUCKET ISOLATION (Critical Local CRM Protection!).** Making the existing `call-recordings` bucket private would immediately break `CallAudioPlayer.tsx` in Local CRM (`Calls.tsx` streams `call_logs.recording_url` directly in `<audio src=...>`). Instead: keep `MINIO_BUCKET=call-recordings` untouched for `platform='local'`, store `platform='global'` recordings in a **private** bucket (`MINIO_GLOBAL_BUCKET=call-recordings-private`), and expose `GET /api/v1/sessions/{sid}/calls/{call_id}/recording` (`?mode=url\|stream`, returning a 1-hour presigned URL or streaming WAV bytes). | Phase 2 |
| **10** | **Enable RLS on all 21 `whatsapp_infra` tables** via schema loop (not just the 5 app tables) | **ADOPT.** Use a PL/pgSQL `DO $$` loop over `pg_tables WHERE schemaname = 'whatsapp_infra'` to enable RLS and create the `wacalls_engine` policy across all 21+ tables (including `whatsmeow_*` Signal key tables). | Phase 1 |
| **11** | **`DELETE /api/v1/agent-configs/{business_id}`** for clean agent deletion | **ADOPT.** Verifies `platform` ownership (`404` on mismatch) and deletes the `agent_configs` row (`204 No Content`). | Phase 2 |
| **12** | **Emit `call.ended` for unanswered outbound / auto-callback calls** | **ADOPT.** In `cm.OnEnded`, when `!isTrulyAnswered && c.Direction != Incoming` on a `global` session, enqueue `call.ended` (`outcome: "unanswered"`, `duration_seconds: 0`, `end_reason`). | Phase 2 |
| **13** | **Separate `GEMINI_API_KEY_GLOBAL`** so Global traffic cannot exhaust Local CRM's Gemini quota | **PROMOTE TO MUST-HAVE (Phase 2).** 4-line check in `createCall`: use `GEMINI_API_KEY_GLOBAL` when `sess.platform == "global"` (fallback to `GEMINI_API_KEY` if unset). Protects Local CRM from noisy-neighbor rate limits and separates Google AI billing! | Phase 2 |
| **14** | **Automated Go tests** for isolation, HMAC, keywords, SSRF, and monotonic DB writes | **ADOPT.** Dedicated table-driven Go test suite run in CI on both `development` and `main`. | Phases 1–3 |
| **15** | **Phase 4 spike on `gemini-3.1-flash-live-preview` tools + 10-min `goAway` reconnect safety** | **ADOPT.** Verify live `functionDeclarations` support first, and ensure `sendToolResponse()` waits on `g.ready` and acquires `g.wsMu` on the current `g.ws` so an in-flight tool call across a 10-minute `goAway` session resumption never writes to a closed WebSocket. | Phase 4 |

---

## 3. Coordinated Changes Required in Local CRM (`buildstart-wacaller`)

Before enabling mandatory `X-Api-Key` authentication on all Go server endpoints (**Phase 1**), we make **three targeted updates** in `buildstart-wacaller`:

### 3.1 Upgrade `supabase/functions/wa-pair-wacaller/index.ts`
Load `GO_API_KEY` (via `_secrets.ts` + `Deno.env`, matching `post-call-wacaller`) and support action routing (`POST` body `{ action?: "status" | "pair" | "logout" | "delete" }`, defaulting to `"status"` when body is empty):
- **`status` (default)**: Calls `GET ${goServerUrl}/api/sessions?business_id=${businessId}` with `X-Api-Key: ${goApiKey}`; if none exists, calls `POST ${goServerUrl}/api/sessions` with `{ business_id: businessId }`. Returns `{ success: true, session_id, state, paired, qr, jid }`.
- **`pair`**: Verifies session ownership, calls `POST ${goServerUrl}/api/sessions/${sid}/pair` with `X-Api-Key`.
- **`delete`**: Verifies session ownership, calls `DELETE ${goServerUrl}/api/sessions/${sid}` with `X-Api-Key`.

### 3.2 Update `frontend/src/components/settings/VoiceAgentConnection.tsx`
Replace all direct browser calls to `GO_SERVER_URL` (`EventSource`, `/pair`, `/api/sessions`, `DELETE`, and the broken `supabase.from("whatsapp_sessions")` fallback) with authenticated `fetch` calls to `wa-pair-wacaller` (`status` polled every `2s` while QR modal/card is open, `pair`, `delete`).

### 3.3 Harden `supabase/functions/post-call-wacaller/index.ts`
- Constant-time byte comparison on `X-Webhook-Secret`.
- Verify `businessId` exists in `wacaller_customization.profiles` (`eq("user_id", businessId)`) rather than shared `auth.users` alone.

---

## 4. Additive PostgreSQL Migration & Least-Privilege Role (`whatsapp_infra`)

All schema changes are **additive-only** (`IF NOT EXISTS` + defaults preserving current Local CRM rows), create the durable `webhook_outbox`, create the restricted `wacalls_engine` role, and enable RLS across **all** tables in `whatsapp_infra`:

```sql
BEGIN;

-- 1. Least-privilege DB role for the Go server (no access to other ~470 tenant schemas!)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wacalls_engine') THEN
    CREATE ROLE wacalls_engine WITH LOGIN PASSWORD '<GENERATED_64_CHAR_SECRET>' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END $$;

REVOKE ALL ON SCHEMA public FROM wacalls_engine;
GRANT USAGE, CREATE ON SCHEMA whatsapp_infra TO wacalls_engine;
ALTER ROLE wacalls_engine SET search_path TO whatsapp_infra;

-- 2. Platform ownership & 1-session-per-business uniqueness on whatsapp_sessions
ALTER TABLE whatsapp_infra.whatsapp_sessions
  ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'local';

CREATE UNIQUE INDEX IF NOT EXISTS idx_whatsapp_sessions_unique_business
  ON whatsapp_infra.whatsapp_sessions (business_id);

CREATE INDEX IF NOT EXISTS idx_whatsapp_sessions_platform
  ON whatsapp_infra.whatsapp_sessions (platform);

-- 3. Per-agent behavior, timezone, and tool columns on agent_configs
ALTER TABLE whatsapp_infra.agent_configs
  ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'local',
  ADD COLUMN IF NOT EXISTS voice_enabled BOOLEAN NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS auto_answer BOOLEAN NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS auto_callback BOOLEAN NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS max_call_seconds INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS timezone TEXT,
  ADD COLUMN IF NOT EXISTS callback_keywords JSONB,
  ADD COLUMN IF NOT EXISTS tools_json JSONB;

-- 4. Scheduled calls outcome linkage & error tracking
ALTER TABLE whatsapp_infra.scheduled_calls
  ADD COLUMN IF NOT EXISTS call_id TEXT,
  ADD COLUMN IF NOT EXISTS error_message TEXT;

-- 5. Durable Webhook Outbox (survives Go restarts; ordered per partition_key)
CREATE TABLE IF NOT EXISTS whatsapp_infra.webhook_outbox (
  seq              BIGSERIAL UNIQUE NOT NULL,
  event_id         TEXT PRIMARY KEY,
  platform         TEXT NOT NULL,
  session_id       TEXT NOT NULL,
  business_id      UUID NOT NULL,
  partition_key    TEXT NOT NULL,
  event_type       TEXT NOT NULL,
  payload          JSONB NOT NULL,
  status           TEXT NOT NULL DEFAULT 'pending', -- pending | delivering | delivered | dead
  attempts         INTEGER NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error       TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivered_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_pending
  ON whatsapp_infra.webhook_outbox (status, next_attempt_at, seq)
  WHERE status IN ('pending', 'delivering');

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_cleanup
  ON whatsapp_infra.webhook_outbox (delivered_at)
  WHERE status = 'delivered';

-- 6. Grant wacalls_engine privileges ONLY inside whatsapp_infra + default privileges for future tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA whatsapp_infra TO wacalls_engine;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA whatsapp_infra TO wacalls_engine;
ALTER DEFAULT PRIVILEGES IN SCHEMA whatsapp_infra GRANT ALL PRIVILEGES ON TABLES TO wacalls_engine;
ALTER DEFAULT PRIVILEGES IN SCHEMA whatsapp_infra GRANT ALL PRIVILEGES ON SEQUENCES TO wacalls_engine;

-- 7. Enable RLS across ALL tables in whatsapp_infra (all 21 existing + webhook_outbox)
--    Blocks PostgREST `anon` and `authenticated` roles completely while allowing:
--    (a) `service_role` (has BYPASSRLS, so Local CRM *-wacaller Edge Functions keep working)
--    (b) `wacalls_engine` (via explicit policy below)
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'whatsapp_infra' LOOP
    EXECUTE format('ALTER TABLE whatsapp_infra.%I ENABLE ROW LEVEL SECURITY', r.tablename);
    EXECUTE format('DROP POLICY IF EXISTS wacalls_engine_all ON whatsapp_infra.%I', r.tablename);
    EXECUTE format(
      'CREATE POLICY wacalls_engine_all ON whatsapp_infra.%I FOR ALL TO wacalls_engine USING (true) WITH CHECK (true)',
      r.tablename
    );
  END LOOP;
END $$;

COMMIT;
```

---

## 5. Detailed Go Server Upgrade Specification (4 Phases)

### Phase 1: Security Hardening, Platform Isolation, Idempotent Sessions & Core Bug Fixes
**Unblocks**: `E0`, `E1`, `G5`, `G7`, Review Items `1a`, `4`, `10`.

1. **Multi-Platform API Key Auth (`cmd/server/auth.go`)**:
   - `GO_API_KEY` $\rightarrow$ `"local"`; `GO_API_KEY_GLOBAL` $\rightarrow$ `"global"` (constant-time comparison).
   - Required on **every** route except `GET /health`, `/api/health`, `/api/v1/health` (public health returns only `{ "status": "online", "timestamp": ... }` with zero JIDs/session metadata).
   - Static console `/` returns `404` unless `SERVE_STATIC_CONSOLE=true`.
   - Bind Go to `-addr 127.0.0.1:8080` in `buildstart-calling-agent.service` and block external non-local traffic to host ports `5432`, `6543`, and `9000` via `DOCKER-USER` iptables rules.
2. **Strict Platform Ownership & Idempotent Session Creation (`sessionmanager.go`, `httpapi.go`)**:
   - `SessionManager.Create(platform, businessID)` holds `m.mu.Lock()`:
     - Checks if any session already exists for `businessID`:
       - Same `platform` $\rightarrow$ returns existing `session.id` (`200 {"id": existing.id, "reused": true}`), preventing duplicate sessions on retries/double-clicks (**Item 4**).
       - Different `platform` $\rightarrow$ returns `404 {"error": "not found"}`.
     - Checks `agent_configs` platform if pre-registered (`404` on mismatch).
   - Every `{sid}` and `{call_id}` endpoint checks `sess.platform == callerPlatform(r)` (`404` on mismatch).
   - `GET /api/sessions`, `/api/lines`, `/api/calls`, `/api/history`, `/api/events` filter strictly by `callerPlatform(r)` and optional `?business_id=<uuid>`.
   - `POST /api/calls/dial` requires explicit `session_id` (`defaultSession()` removed).
3. **Engine Bug Fixes (`sessionstore.go`, `scheduler.go`)**:
   - **`C3`**: Fix `listCallHistory("", limit)` SQL placeholder numbering (`LIMIT $1` vs `LIMIT $2`).
   - **`C4`**: Monotonic `ON CONFLICT (call_id) DO UPDATE` in `saveCallRecord` (never overwrite `'ended'` back to `'ringing'`/`'connected'`; keep `GREATEST` duration and longer `transcript_json` array) + synchronous final save before webhook dispatch.
   - **`G5`**: Real SQL transaction around `FOR UPDATE SKIP LOCKED` in `scheduler.go`, removal of cross-tenant session fallback, and persistence of `call_id` / `error_message`.

---

### Phase 2: Agent Config API, Runtime Context, Durable Ordered Outbox & Private Global Recordings
**Unblocks**: `E2`, `E3` (call/session events), `E6`, Review Items `2`, `3`, `5`, `6`, `9`, `11`, `12`, `13`.

1. **Agent Config CRUD API (`cmd/server/httpapi_agent_config.go`)**:
   - **`PUT /api/v1/agent-configs/{business_id}`**: Upserts `business_prompt`, `greeting_message`, `fallback_message`, `voice_model`, `voice_enabled` (default `true`), `auto_answer` (default `true`), `auto_callback` (default `true`), `callback_keywords` (`nil` = default list, `[]` = disabled), `max_call_seconds` (default `0`), `timezone` (optional IANA string validated via `time.LoadLocation`), `tools` (`tools_json`), stamped with `platform = callerPlatform(r)` (`404` if owned by another platform).
   - **`GET /api/v1/agent-configs/{business_id}`**: Returns config (`404` if missing or wrong platform).
   - **`DELETE /api/v1/agent-configs/{business_id}`** (**Item 11**): Verifies platform ownership and deletes config (`204 No Content`).
2. **Gemini Key Isolation & Live Call Context Injection (`cmd/server/session.go`, `main.go`)**:
   - Import `_ "time/tzdata"` in `main.go` so all IANA timezones resolve reliably.
   - **Gemini Key Isolation (Item 13)**: In `createCall`, if `sess.platform == "global"` and `os.Getenv("GEMINI_API_KEY_GLOBAL") != ""`, use `GEMINI_API_KEY_GLOBAL`; otherwise use `GEMINI_API_KEY`.
   - **Call-Start Context Injection (Item 5 — `global` only)**:
     - Reorder `onIncomingOffer` so `callerPN` / `callbackJID` / `peerNum` are resolved *before* `createCall(callID, peerNum, "inbound")`.
     - For `sess.platform == "global"`, append to the prompt passed into `NewGeminiLiveAgent`:
       ```text

       [LIVE CALL CONTEXT]
       Current local date and time: Thursday, 2026-11-26 14:30 (America/New_York, EST)
       Caller WhatsApp number: +14155550123
       Call direction: inbound
       ```
       (`platform == "local"` prompts are passed untouched.)
3. **Per-Agent Call Behavior & `voice_enabled == false` Missed Event (`cmd/server/session.go`)**:
   - **`voice_enabled == false` (Correction 3 + Item 6)**:
     - Save `caller_pn` $\rightarrow$ `putLIDMapping` if present.
     - For `sess.platform == "global"`, enqueue one deduplicated `call.missed` event (`{ call_id, caller_number, peer_jid, started_at, reason: "agent_disabled", auto_callback_scheduled: false }`).
     - Return immediately before `createCall` and `cm.HandleCallOffer` (owner's phone rings normally; zero `<preaccept>`, zero auto-callback).
   - **`auto_answer`, `auto_callback`, `max_call_seconds`, and fixed `callback_keywords` (`C5` — word boundaries, group/broadcast skip, negation filter)**.
   - **Unanswered Outbound / Auto-Callback Events (Item 12)**: When `!isTrulyAnswered && c.Direction != Incoming` on a `global` session, enqueue `call.ended` (`outcome: "unanswered"`, `duration_seconds: 0`, `end_reason`).
4. **Durable, Per-Chat Ordered Webhook Outbox (`cmd/server/outbox.go` — Items 2 & 3)**:
   - **Local CRM (`platform == "local"`)**: Unchanged direct `POST_CALL_WEBHOOK_URL` (`post-call-wacaller`).
   - **Global (`platform == "global"`)**:
     - `EnqueueGlobalEvent(ctx, sessionID, businessID, partitionKey, eventType, data)` synchronously inserts into `whatsapp_infra.webhook_outbox` (`event_id = "evt_" + hex16`, `status = 'pending'`) and signals the dispatcher.
     - **Partitioning**:
       - Chat/call events use `partition_key = sessionID + ":" + canonicalPeerJID`.
       - Session status events use `partition_key = sessionID`.
     - **Sharded Sequential Workers**:
       - `16` persistent goroutines (`shard 0..15`), plus a startup recovery sweep resetting stale `delivering` rows (`> 2m`) back to `pending`.
       - Worker `k` queries due `pending` rows where `hashtext(partition_key) & 15 = k` **excluding any `partition_key` that has an older undelivered `seq`**, ordered by `seq ASC`. This guarantees two rapid messages from the same customer are **never delivered out of order**, even across retries!
     - **Delivery & HMAC**:
       - Computes fresh `t=<unix>` on each attempt and signs `X-Engine-Signature: t=<ts>,v1=<HMAC-SHA256(PLATFORM_GLOBAL_WEBHOOK_SECRET, ts + "." + rawBody)>` (`event_id` stays constant for Global idempotency).
       - Backoff on timeout (`15s`) or `408`/`429`/`5xx`: `2s`, `6s`, `18s`, `45s`, `120s` (`max 6` attempts $\rightarrow$ `status = 'dead'`).
       - Hourly background cleanup deletes `status = 'delivered'` rows older than `7 days`.
5. **Dual-Bucket Recording Privacy (`cmd/server/recorder.go` — Item 9)**:
   - `platform == "local"`: Uploads to existing `MINIO_BUCKET` (`call-recordings`) and writes public `recording_url` to `call_logs` — **Local CRM `CallAudioPlayer.tsx` works with zero changes!**
   - `platform == "global"`:
     - Uploads `<business_id>/<call_id>.wav` to **private** bucket `MINIO_GLOBAL_BUCKET` (default `call-recordings-private`, auto-created with private policy on startup if missing).
     - Stores object key `global-private:<business_id>/<call_id>.wav` in `call_logs.recording_url` and emits `recording.ready` (`{ call_id, recording_key: "<business_id>/<call_id>.wav" }`).
     - Adds **`GET /api/v1/sessions/{sid}/calls/{call_id}/recording`**: verifies `global` platform + session ownership and streams the WAV bytes from MinIO with HTTP `Range` support (`Accept-Ranges: bytes`, `audio/wav`) or returns `{ "url": "<presigned-url>", "expires_in": 3600 }` when `?presign=true` (using `MINIO_PUBLIC_S3_ENDPOINT` if configured, or direct streaming through Global's Edge Function / signed token).

---

### Phase 3: Full WhatsApp Messaging Agent Support (`E4` & `E7`, Items `7` & `8`)
**Unblocks**: Global Phase 5 (Messaging Agent, Voice Notes, Catalog Images, Inbox & Human Takeover).

1. **Unified Outbound Send Tracker (`Item 8`)**:
   - All outbound WhatsApp sends in Go — `sendTextSync` (`/messages/send`), `sendWhatsAppMessage` (missed-call `fallback_message`), and `sendMediaSync` (images/docs/audio/video) — delegate to a single `sess.sendProtoMessage(ctx, targetJID, waMsg)` method.
   - `sendProtoMessage` generates a message ID via `sess.client.GenerateMessageID()`, pre-inserts that ID into `sess.apiSentMsgIDs` (`10m` TTL) **before** `SendMessage(ctx, target, msg, whatsmeow.SendRequestExtra{ID: msgID})` returns! Pre-registering before network I/O eliminates any race condition where WhatsApp echoes a `DeviceSentMessage` stanza before `SendMessage` unblocks!
2. **Offline Sync Tracking & `is_offline` Flag (`Item 7`)**:
   - Handle `*events.OfflineSyncPreview`: set `sess.inOfflineSync.Store(true)` and arm a 60s safety reset timer.
   - Handle `*events.OfflineSyncCompleted`: set `sess.inOfflineSync.Store(false)`.
   - Ignore `evt.SourceWebMsg != nil` (initial companion history sync dumps).
   - Compute `isOffline := sess.inOfflineSync.Load() || time.Since(evt.Info.Timestamp) > 2*time.Minute`.
   - Suppress callback keyword triggers when `isOffline == true`.
3. **Inbound `message.received` & `message.owner_sent` (`cmd/server/messaging.go`)**:
   - Active only for `sess.platform == "global"`.
   - Skips `@g.us`, `status@broadcast`, `@newsletter`, protocol/key-distribution messages.
   - Saves `SenderAlt`/`RecipientAlt` `@lid` $\leftrightarrow$ PN mappings into `whatsmeow_lid_map`.
   - **`!evt.Info.IsFromMe`** $\rightarrow$ caches downloadable media descriptor in `sess.mediaCache` (`30m` TTL, max `500` entries) and enqueues `message.received` (including `"is_offline": isOffline`, `type`, `text`, `caption`, `quoted_message_id`, `media`) into `webhook_outbox` with `partition_key = sess.id + ":" + chatJID`.
   - **`evt.Info.IsFromMe && !sess.wasSentByAPI(evt.Info.ID)`** $\rightarrow$ enqueues `message.owner_sent` (`{ message_id, to_jid, to_pn, timestamp, type, text, is_offline }`) with `partition_key = sess.id + ":" + chatJID`.
4. **Media Download, SSRF-Safe Media Send, Presence & Read Endpoints**:
   - `GET /api/v1/sessions/{sid}/messages/{id}/media` $\rightarrow$ decrypts via `sess.client.Download(ctx, cachedMsg)` and streams bytes.
   - `POST /api/v1/sessions/{sid}/messages/send` $\rightarrow$ supports `{ to, text }` and `{ to, media_url, media_type, caption, filename }` with custom `DialContext` SSRF IP blocker + `20 MB` cap + `15s` timeout.
   - `POST /api/v1/sessions/{sid}/presence` (`composing` / `paused`) & `POST /api/v1/sessions/{sid}/messages/{id}/read`.

---

### Phase 4: Gemini Live In-Call Function Calling (`E5` / `G6` & Item `15`)
**Unblocks**: Global Phase 6 (Real-time in-call slot checks, bookings, order quotes, and `end_call`).

1. **Pre-Implementation Verification Spike (`Item 15`)**:
   - Write a standalone verification test against `models/gemini-3.1-flash-live-preview` (`v1beta.GenerativeService.BidiGenerateContent`) confirming `setup.tools[].functionDeclarations`, `toolCall`, `toolCallCancellation`, and `toolResponse` behavior alongside `responseModalities: ["AUDIO"]` and `thinkingBudget: 0`.
2. **Zero-Impact Gating for Local CRM**:
   - `setupObj["tools"]` is omitted completely when `len(g.tools) == 0` (all Local CRM calls).
3. **Reconnect-Safe Tool Execution (`Item 15`)**:
   - Execute `tool.invoke` (`8s` HMAC POST to `PLATFORM_GLOBAL_TOOLS_URL`) asynchronously off `readLoop()` with per-call cancellation on `toolCallCancellation`.
   - When sending `toolResponse`, wait up to `3s` if `g.isReconnecting.Load()` is true (10-minute `goAway` session resumption), then write under `g.wsMu` to the newly resumed `g.ws` so in-flight tool responses survive mid-call WebSocket rotation cleanly.
   - Built-in `end_call` waits for current model speech buffer to drain (up to `3s`), then terminates the call cleanly.

---

## 6. Automated Go Test Suite Specification (`Item 14`)

Add deterministic, fast (`<2s`, no external network/Postgres required) unit tests run in `.github/workflows/ci.yml` on both `development` and `main`:
1. `cmd/server/auth_test.go`:
   - Missing/invalid key $\rightarrow$ `401`; `local` key accessing `global` session/call/config $\rightarrow$ `404`; `global` key accessing `local` session $\rightarrow$ `404`; `GET /api/sessions` returns only caller's platform; `POST /api/sessions` idempotently returns existing session for same `(platform, business_id)` and `404` for cross-platform `business_id`; public `/health` leaks zero phone JIDs.
2. `cmd/server/outbox_test.go`:
   - `X-Engine-Signature` HMAC-SHA256 matches `t=<ts>,v1=<hex>` test vector; two rapid events on the same `partition_key` deliver strictly in sequence (`seq 1` before `seq 2`) even when `seq 1` retries once on HTTP `502`.
3. `cmd/server/keywords_test.go`:
   - Word-boundary matching (`"call me"` matches `"please call me"`, rejects `"recall"`, rejects `"don't call me"` / `"do not call me"` / `"කතා කරන්න එපා"`, rejects group chats and `is_offline == true`, honors `callback_keywords: []` to disable).
4. `cmd/server/ssrf_test.go`:
   - Custom `DialContext` rejects `127.0.0.1`, `::1`, `10.0.0.1`, `172.18.0.5` (Docker bridge), `192.168.1.1`, `169.254.169.254` (cloud metadata), and `http://` URLs while permitting public HTTPS IPs.
5. `cmd/server/sessionstore_sql_test.go`:
   - Verifies `listCallHistory("", 50)` vs `listCallHistory("sid", 50)` query/arg arity and monotonic conflict behavior.

---

## 7. Updated Environment Variables (`/opt/buildstart-calling-agent/.env`)

```dotenv
# Least-privilege DB connection (replaces superuser postgres DSN)
DATABASE_URL=postgres://wacalls_engine:<64-char-secret>@127.0.0.1:5434/postgres?search_path=whatsapp_infra&sslmode=disable

# Local CRM settings (unchanged behavior)
GEMINI_API_KEY=<local-crm-gemini-key>
PUBLIC_HOST=buildstart-calling-agent.buildstart.io
GO_API_KEY=<existing-local-crm-key>
POST_CALL_WEBHOOK_URL=http://127.0.0.1:8000/functions/v1/post-call-wacaller
POST_CALL_WEBHOOK_SECRET=<existing-local-webhook-secret>
AUTO_CALLBACK_ENABLED=true
MINIO_ENDPOINT=127.0.0.1:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=...
MINIO_BUCKET=call-recordings
MINIO_PUBLIC_URL=https://storage.buildstart.io/call-recordings

# Global Platform settings (isolated keys, webhooks, Gemini quota & private recordings)
GO_API_KEY_GLOBAL=<strong-64-hex-global-api-key>
GEMINI_API_KEY_GLOBAL=<dedicated-global-gemini-key>
PLATFORM_GLOBAL_WEBHOOK_URL=https://<global-supabase-domain>/functions/v1/engine-webhook
PLATFORM_GLOBAL_WEBHOOK_SECRET=<strong-64-hex-hmac-secret>
PLATFORM_GLOBAL_TOOLS_URL=https://<global-supabase-domain>/functions/v1/engine-tools
MINIO_GLOBAL_BUCKET=call-recordings-private
SERVE_STATIC_CONSOLE=false
```

---

## 8. Safe Execution Order & Rollback

1. **Step 1 — Local CRM Prep (`buildstart-wacaller`)**: Deploy `wa-pair-wacaller` (with `GO_API_KEY` in `_secrets.ts`), `post-call-wacaller` (`profiles` guard + constant-time secret check), and `VoiceAgentConnection.tsx` (Vercel). Confirm Local CRM Voice Agent settings & QR pairing work via `wa-pair-wacaller`.
2. **Step 2 — Database Migration & Firewall**: Back up `whatsapp_infra`, run the additive SQL script (§4: `wacalls_engine` role, additive columns, `UNIQUE (business_id)`, `webhook_outbox`, and 22-table RLS loop), verify Local CRM Edge Functions still read/write `whatsapp_infra`, and block external non-local access to ports `5432`/`6543`/`9000`.
3. **Step 3 — Deploy Go Phases 1 + 2 + 3**: Switch `.env` `DATABASE_URL` to `wacalls_engine`, add `GO_API_KEY_GLOBAL` / `GEMINI_API_KEY_GLOBAL` / `MINIO_GLOBAL_BUCKET`, bind `-addr 127.0.0.1:8080`, deploy binary, restart `buildstart-calling-agent.service`, and run full Local CRM + Global API smoke checks.
4. **Step 4 — Deploy Go Phase 4 (`E5` Live Tools)**: Run the `gemini-3.1-flash-live-preview` tool-calling spike, then ship `tools` support when Global reaches Phase 6.
5. **5-Second Rollback**: Because all DB changes are additive with Local-compatible defaults, swapping back `buildstart-calling-server.bak` (and `.env.bak` if needed) restores previous state in `<5 seconds` with zero schema rollback.

---

## 9. Standalone Runbook: Rotating the Shared `postgres` Superuser Password (`Item 1b`)

> **Why separate**: Hetzner's `supabase-db` hosts ~470 client schemas/functions. Rotating `POSTGRES_PASSWORD` requires restarting the shared Supabase containers, so it **must** run in a dedicated 2-minute low-traffic maintenance window — never mixed into a Go engine deploy.

1. **Audit Consumers First (Read-Only)**:
   - Check established TCP connections to `:5432`, `:5434`, and `:6543` (`ss -tnp | grep -E ":(5432|5434|6543)"`) to verify no external VPS/apps connect directly using `postgres`.
   - Grep `/opt/` and `/root/supabase/supabase/docker/` for `POSTGRES_PASSWORD` references (Go will already be migrated onto `wacalls_engine` in Phase 1, so Go won't even use `postgres` anymore!).
2. **Rotate Cleanly in Maintenance Window**:
   - Generate new strong password `NEW_PG_PASS`.
   - Inside `supabase-db` (`psql -U postgres`): update `ALTER USER postgres WITH PASSWORD '<NEW_PG_PASS>';` (and check Supabase internal roles `authenticator`, `supabase_admin`, `supabase_auth_admin`, `supabase_storage_admin`, `supabase_functions_admin` if they share `POSTGRES_PASSWORD` in `/root/supabase/supabase/docker/.env`).
   - Update `POSTGRES_PASSWORD` in `/root/supabase/supabase/docker/.env` (and Supavisor `pooler.exs` / `_supabase` tenant record if applicable).
   - Recreate dependent Supabase containers (`docker compose up -d`) and verify PostgREST, GoTrue Auth, Storage, and Edge Functions health immediately.
