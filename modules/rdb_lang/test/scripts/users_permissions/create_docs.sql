CREATE SCHEMA docs_schema CREATORS ($admin) AS (
  TABLE docs (
    body string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);

CREATE TABLEGROUP docs_group
  USING SCHEMA docs_schema
  BIND users => users
  USING IDENTITIES users.identities;
