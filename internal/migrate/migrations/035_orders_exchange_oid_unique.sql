-- migrate:ddl
-- 035_orders_exchange_oid_unique
--
-- PR21 correction (blocker 4): one real exchange_order_id must never be attached to two internal
-- orders on the same exchange. The pre-existing `idx_orders_exchange_oid (exchange_id,
-- exchange_order_id)` was NON-unique, so two concurrent operator attaches could both pass an
-- application-level "id unused" check and bind the same venue id to different orders. This replaces
-- it with a UNIQUE index, giving a hard DB-level guarantee (in addition to the per-exchange FOR
-- UPDATE serialization the attach path now performs).
--
-- exchange_order_id is NULLable and is NULL for an unplaced order; MariaDB unique indexes permit
-- multiple NULLs, so unplaced orders never conflict — only real venue ids are constrained. If
-- historical rows already hold a DUPLICATE (exchange_id, exchange_order_id) non-null pair, this
-- ADD UNIQUE fails loudly (the migration stops so an operator can investigate) — it never deletes
-- or rewrites existing data.

-- Add the UNIQUE index FIRST (it also leads with exchange_id, so it can carry the exchange_id
-- foreign key), THEN drop the old non-unique index. Doing it in this order avoids MariaDB error
-- 1553 (cannot drop an index still needed by a foreign key).
ALTER TABLE orders
  ADD UNIQUE INDEX IF NOT EXISTS uq_orders_exchange_oid (exchange_id, exchange_order_id);

ALTER TABLE orders DROP INDEX IF EXISTS idx_orders_exchange_oid;
