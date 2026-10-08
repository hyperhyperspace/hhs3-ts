CREATE CATALOG app CREATORS ($admin) VERSION '1.0.0'
  PARAMS (:manager identity)
AS (
  TABLEGROUP users USING SCHEMA users_schema
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :manager, publicKey = publicKey(:manager), name = 'Admin'),
      caps (label = 'manager', grantee = :manager)
    ),
  TABLEGROUP docs_group USING SCHEMA docs_schema
    BIND users => users
    USING IDENTITIES users.identities
);

CREATE DATABASE app_db USING CATALOG app AT '1.0.0'
  CREATORS ($admin)
  WITH PARAMS (:manager = $admin)
  BY $admin;
