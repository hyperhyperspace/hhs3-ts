CREATE DATABASE app CREATORS ($admin);

CREATE SCHEMA hhs:user CREATORS ($admin) AS (
  
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER,
  
  TABLE caps (
    label string PUB READONLY,
    grantee string PUB READONLY
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
    ALLOW delete IF caps.grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author,
  
  TABLE profiles (
    ownerId string PUB READONLY REFERENCES identities,
    keyId string PUB READONLY,
    displayName string NULL PUB,
    bio string NULL PUB,
    avatarUrl string NULL PUB,
    email string NULL
  )
    ALLOW insert IF EXISTS identities WHERE identities.keyId = profiles.keyId
    ALLOW update IF profiles.keyId = $author
    ALLOW delete IF profiles.keyId = $author
);

CREATE SCHEMA hhs:doc CREATORS ($admin) AS (
  
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

ADD SCHEMA hhs:user TO app BY $admin;

ADD SCHEMA hhs:doc TO app BY $admin;

-- hhs:user
CREATE TABLEGROUP user USING SCHEMA hhs:user AT LATEST
  USING IDENTITIES identities
  WITH ROWS (
  identities (keyId=$admin, publicKey=publicKey($admin), name='Admin'),
  caps (label='manager', grantee=$admin)
);

-- hhs:doc
CREATE TABLEGROUP doc USING SCHEMA hhs:doc AT LATEST
  BIND user => user
  USING IDENTITIES user.identities
  ALLOW UPDATE REF user IF EXISTS caps WHERE caps.grantee = $author;

ADD TABLEGROUP user TO app BY $admin;

ADD TABLEGROUP doc TO app BY $admin;
