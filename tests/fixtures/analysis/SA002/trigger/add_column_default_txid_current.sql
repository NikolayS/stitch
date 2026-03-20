ALTER TABLE audit_log ADD COLUMN txid bigint DEFAULT txid_current();
