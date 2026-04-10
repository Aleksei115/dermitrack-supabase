-- =============================================================================
-- Pre-PROD Audit Cleanup — 2026-04-09
-- =============================================================================
-- Fixes issues found by scripts/audit-prod-database.sh before merging
-- migration-ai-first branch (dkijnakwldhqljmaxyli) → PROD.
--
-- Issues addressed:
--   #01 — 6 orphan inventory_movements for MEXBR172 (visit_id IS NULL,
--          task_id 097c6548 deleted, net-zero COLLECTION+PLACEMENT, no lots)
--   #10a — 1 chatbot.conversation pointing to deleted auth.users UUID
--          (c03b0c69), with 10 messages — test data
--
--   #14 — 1 ENTREGADA collection (9ecb1d5a) without items — backfill from movements
--
-- Issues accepted as legacy data (excluded from audit queries):
--   #12 — 1 legacy COMPLETED visit (3c5c0567) with 0 tasks, pre-dates task system (pre-2026)
--   #13 — 1 legacy COMPLETED visit (1e4d7f88) with no movements, migrated_from_legacy
-- =============================================================================

DO $$
DECLARE
  v_deleted_movement_ids bigint[];
  v_deleted_movement_count int;
  v_deleted_conversation_id uuid;
  v_deleted_message_count int;
BEGIN
  -- =========================================================================
  -- FIX #01 — Delete 6 orphan inventory_movements for MEXBR172
  -- =========================================================================
  -- These movements have:
  --   - visit_id IS NULL
  --   - task_id = 097c6548-7015-47b2-844b-cb96666187a3 (no longer in visit_tasks)
  --   - 3 COLLECTION + 3 PLACEMENT for same SKUs/quantities (net-zero effect)
  --   - No associated cabinet_inventory_lots
  --   - movement_date = 2026-04-01
  --   - cabinet_inventory for affected SKUs already at 0
  -- =========================================================================

  -- Guard: only proceed if exactly 6 orphan movements match
  SELECT array_agg(id ORDER BY id)
  INTO v_deleted_movement_ids
  FROM inventory_movements
  WHERE visit_id IS NULL
    AND task_id = '097c6548-7015-47b2-844b-cb96666187a3'
    AND client_id = 'MEXBR172'
    AND movement_date::date = '2026-04-01';

  IF COALESCE(array_length(v_deleted_movement_ids, 1), 0) = 0 THEN
    -- Records don't exist (already cleaned or different environment)
    RAISE NOTICE 'FIX #01: No orphan movements found for MEXBR172 — skipping';
  ELSIF array_length(v_deleted_movement_ids, 1) <> 6 THEN
    RAISE EXCEPTION 'Expected 0 or 6 orphan movements for MEXBR172, found %',
      array_length(v_deleted_movement_ids, 1);
  ELSE
    -- Verify no lots depend on these movements
    IF EXISTS (
      SELECT 1 FROM cabinet_inventory_lots
      WHERE movement_id = ANY(v_deleted_movement_ids)
         OR consumed_by_movement_id = ANY(v_deleted_movement_ids)
    ) THEN
      RAISE EXCEPTION 'Lots reference these movements — aborting';
    END IF;

    DELETE FROM inventory_movements
    WHERE id = ANY(v_deleted_movement_ids);

    GET DIAGNOSTICS v_deleted_movement_count = ROW_COUNT;

    -- Audit log
    INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_after)
    VALUES (
      'inventory_movements',
      'MEXBR172-2026-04-01-orphans-audit',
      'DELETE',
      'admin_aleksei',
      jsonb_build_object(
        'reason', 'pre-PROD audit cleanup #01: orphan movements with deleted task',
        'deleted_ids', to_jsonb(v_deleted_movement_ids),
        'deleted_count', v_deleted_movement_count,
        'client_id', 'MEXBR172',
        'task_id', '097c6548-7015-47b2-844b-cb96666187a3'
      )
    );

    RAISE NOTICE 'FIX #01: Deleted % orphan movements for MEXBR172 (ids: %)',
      v_deleted_movement_count, v_deleted_movement_ids;
  END IF;

  -- =========================================================================
  -- FIX #10a — Delete orphan chatbot conversation + messages
  -- =========================================================================
  -- Conversation 6d193264 has user_id = c03b0c69-c1eb-4876-ab74-c496eb3998fc
  -- which is an auth.users UUID that no longer maps to any public.users record.
  -- 10 test messages. Messages cascade on conversation delete.
  -- =========================================================================

  v_deleted_conversation_id := '6d193264-9ffc-4d5c-9f18-e4b5504ce66e';

  -- Guard: verify conversation exists and has the expected user_id
  IF NOT EXISTS (
    SELECT 1 FROM chatbot.conversations
    WHERE id = v_deleted_conversation_id
      AND user_id = 'c03b0c69-c1eb-4876-ab74-c496eb3998fc'
  ) THEN
    RAISE NOTICE 'FIX #10a: Conversation already cleaned or user_id changed — skipping';
  ELSE
    -- Count and delete messages first (no CASCADE on this FK)
    SELECT count(*)::int INTO v_deleted_message_count
    FROM chatbot.messages
    WHERE conversation_id = v_deleted_conversation_id;

    DELETE FROM chatbot.messages
    WHERE conversation_id = v_deleted_conversation_id;

    DELETE FROM chatbot.conversations
    WHERE id = v_deleted_conversation_id;

    -- Audit log
    INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_after)
    VALUES (
      'chatbot.conversations',
      v_deleted_conversation_id::text,
      'DELETE',
      'admin_aleksei',
      jsonb_build_object(
        'reason', 'pre-PROD audit cleanup #10a: conversation with deleted auth user',
        'orphan_user_id', 'c03b0c69-c1eb-4876-ab74-c496eb3998fc',
        'deleted_messages', v_deleted_message_count
      )
    );

    RAISE NOTICE 'FIX #10a: Deleted conversation % with % messages',
      v_deleted_conversation_id, v_deleted_message_count;
  END IF;

  -- =========================================================================
  -- FIX #14 — Backfill collection_items for ENTREGADA collection 9ecb1d5a
  -- =========================================================================
  -- Collection for MEXEG032 (visit db6f84be) was delivered with COLLECTION
  -- movements recorded, but collection_items were never populated.
  -- Backfill from inventory_movements WHERE type = 'COLLECTION' for that visit.
  -- =========================================================================

  IF NOT EXISTS (
    SELECT 1 FROM collection_items
    WHERE collection_id = '9ecb1d5a-41aa-4530-b006-b5cedac58f34'
  ) THEN
    INSERT INTO collection_items (collection_id, sku, quantity)
    SELECT
      '9ecb1d5a-41aa-4530-b006-b5cedac58f34',
      im.sku,
      im.quantity
    FROM inventory_movements im
    WHERE im.visit_id = 'db6f84be-828f-4a02-9b19-2ded75b2f4e1'
      AND im.type = 'COLLECTION';

    INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_after)
    VALUES (
      'collection_items',
      '9ecb1d5a-MEXEG032-backfill',
      'INSERT',
      'admin_aleksei',
      jsonb_build_object(
        'reason', 'pre-PROD audit cleanup #14: backfill collection_items from movements',
        'collection_id', '9ecb1d5a-41aa-4530-b006-b5cedac58f34',
        'client_id', 'MEXEG032',
        'visit_id', 'db6f84be-828f-4a02-9b19-2ded75b2f4e1'
      )
    );

    RAISE NOTICE 'FIX #14: Backfilled collection_items for 9ecb1d5a from COLLECTION movements';
  ELSE
    RAISE NOTICE 'FIX #14: collection_items already exist — skipping';
  END IF;

END $$;
