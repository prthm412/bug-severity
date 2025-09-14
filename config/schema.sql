PRAGMA journal_mode = WAL;

-- Issues
CREATE TABLE IF NOT EXISTS issues (
  id INTEGER PRIMARY KEY,
  repo_full_name TEXT,
  number INTEGER,
  title TEXT,
  body TEXT,
  labels_json TEXT,
  mapped_severity TEXT,
  state TEXT,
  created_at TEXT,
  closed_at TEXT
);

-- Commits
CREATE TABLE IF NOT EXISTS commits (
  sha TEXT PRIMARY KEY,
  repo_full_name TEXT,
  author TEXT,
  timestamp TEXT,
  message TEXT,
  files_changed INTEGER,
  insertions INTEGER,
  deletions INTEGER,
  pr_number INTEGER
);

-- Links commit<->issue (e.g., fixes #123)
CREATE TABLE IF NOT EXISTS commit_issue_link (
  commit_sha TEXT,
  issue_id INTEGER,
  link_type TEXT,
  PRIMARY KEY (commit_sha, issue_id)
);

-- Raw webhook events
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT,
  payload TEXT,
  received_at TEXT DEFAULT (datetime('now'))
);

-- Materialized features (optional for now)
CREATE TABLE IF NOT EXISTS features (
  commit_sha TEXT PRIMARY KEY,
  feature_vector_json TEXT,
  generated_at TEXT DEFAULT (datetime('now'))
);

-- Predictions
CREATE TABLE IF NOT EXISTS predictions (
  commit_sha TEXT PRIMARY KEY,
  predicted_class TEXT,
  severity_score REAL,
  proba_json TEXT,
  model_version TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_issues_repo_time ON issues(repo_full_name, created_at);
CREATE INDEX IF NOT EXISTS idx_commits_repo_time ON commits(repo_full_name, timestamp);
