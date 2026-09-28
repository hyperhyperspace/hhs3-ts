CREATE SCHEMA docs_schema CREATORS ($admin) AS (
  TABLE docs (
    body string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);
