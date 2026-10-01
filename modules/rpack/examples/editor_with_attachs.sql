-- The editor app: two schemas, a catalog that releases them as table groups,
-- and a database deployed from the catalog. $admin plays both roles here: the
-- developer who signs the schemas and the catalog, and the admin who deploys
-- the database.

CREATE SCHEMA hhs:user CREATORS ($admin) VERSION '1.0.0' AS (
  
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER,
  
  TABLE caps (
    label string PUB READONLY,
    grantee identity PUB READONLY
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
    ALLOW delete IF caps.grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author,
  
  TABLE profiles (
    ownerId string PUB READONLY REFERENCES identities,
    keyId identity PUB READONLY,
    displayName string NULL PUB,
    bio string NULL PUB,
    avatarUrl string NULL PUB,
    email string NULL
  )
    ALLOW insert IF EXISTS identities WHERE identities.keyId = profiles.keyId
    ALLOW update IF profiles.keyId = $author
    ALLOW delete IF profiles.keyId = $author
);

CREATE SCHEMA hhs:doc CREATORS ($admin) VERSION '1.0.0' AS (
  
  TABLE pages (
    title string,
    deleted boolean,
  ) ALLOW insert IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
    ALLOW update IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
    ALLOW delete IF false,

  TABLE blocks (
    pageId string READONLY REFERENCES pages,
    content string
  ) ALLOW all IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author,
);

-- The first release. :admin is supplied by whoever deploys the catalog, and
-- becomes the first manager.
CREATE CATALOG editor CREATORS ($admin) VERSION '1.0.0'
  PARAMS (:admin identity)
AS (
  TABLEGROUP user USING SCHEMA hhs:user AT LATEST
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  TABLEGROUP doc USING SCHEMA hhs:doc AT LATEST
    BIND user => user
    USING IDENTITIES user.identities
    ALLOW UPDATE REF user IF EXISTS caps WHERE caps.grantee = $author,
  FILES attachs
    USING IDENTITIES user.identities
    ALLOW WRITE IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author


) NOTE 'initial release' BY $admin;

