ALTER CATALOG app VERSION '1.1.0' AS (
  UPDATE SCHEMA docs_schema TO LATEST ON docs_group
) BY $admin;

UPDATE CATALOG app TO '1.1.0' ON app_db BY $admin;
