CREATE SCHEMA users_schema CREATORS ($admin) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER ALLOW insert IF true,
  TABLE caps (
    label string PUB READONLY,
    grantee string PUB READONLY
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
    ALLOW delete IF grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);

CREATE TABLEGROUP users
  USING SCHEMA users_schema
  USING IDENTITIES identities
  WITH ROWS (
    identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
    caps (label = 'manager', grantee = $admin)
  );
