---
name: schema-validate
description: Validate all playbook setup definitions against the schema. USE WHEN modifying the playbook data model, after importing setups, or before a release.
---

# /schema-validate

Validate all playbook definitions and database schema integrity.

## Steps

1. Run schema validation:
   ```bash
   cargo test --lib db::schema_validation -- --nocapture
   ```

2. Check all stored setups:
   - Every setup has required fields (name, at least one condition, stop logic, at least one target)
   - All condition references point to valid pipeline fields
   - Backtest results have valid numeric ranges (win rate 0-1, profit factor ≥ 0, etc.)
   - No orphaned records (trades referencing deleted setups, etc.)

3. Check backward compatibility:
   - If the schema has been modified, verify all existing data still loads correctly
   - Run migration tests if schema version changed

4. Report:
   - Number of setups validated
   - Number of conditions validated
   - Any validation errors with specific field/value details
   - Schema version and migration status
