# etl/historic_backfill.py

import os, json, re, time, sqlite3, yaml
from typing import Dict, List, Optional, Tuple
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "data/dev.sqlite3"
SETTINGS = "config/settings.yaml"
SCHEMA = "config/schema.sql"

GITHUB_API = "https://api.github.com"
SESSION = requests.Session()
TOKEN = os.getenv("GITHUB_TOKEN")

if TOKEN:
    SESSION.headers.update({"Authorization": f"token {TOKEN}"})
SESSION.headers.update({"Accept": "application/vnd.github+json", "User-Agent": "bug-sev-diss"})

def load_settings() -> dict:
    with open(SETTINGS, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.close()

def insert_issue(conn, repo: str, issue: dict, mapped_sev: Optional[str]):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO issues
        (id, repo_full_name, number, title, body, labels_json, mapped_severity, state, created_at, closed_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
    """, (
        issue["id"], repo, issue["number"], issue.get("title"),
        issue.get("body"), json.dumps(issue.get("labels", [])),
        mapped_sev, issue.get("state"),
        issue.get("created_at"), issue.get("closed_at")
    ))
    conn.commit()

def insert_commit(conn, repo: str, commit: dict):
    cur = conn.cursor()
    stats = commit.get("stats") or {}
    files = commit.get("files") or []
    cur.execute("""
        INSERT OR REPLACE INTO commits
        (sha, repo_full_name, author, timestamp, message, files_changed, insertions, deletions, pr_number)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        commit["sha"],                               # sha
        repo,                                       # repo_full_name
        ((commit.get("author") or {}).get("login")  # author login if available
         or (commit.get("commit", {}).get("author", {}) or {}).get("name")),
        (commit.get("commit", {}).get("author", {}) or {}).get("date"),  # timestamp
        (commit.get("commit", {}) or {}).get("message"),                 # message
        len(files) if isinstance(files, list) else None,                 # files_changed
        stats.get("additions"),                                          # insertions
        stats.get("deletions"),                                          # deletions
        None                                                             # pr_number (fill later if needed)
    ))
    conn.commit()


def map_severity_from_labels(labels: List[dict], mapping: Dict[str, str]) -> Optional[str]:
    # Normalize label names and map; return L/M/H/C names
    names = [str(lb.get("name", "")).strip().lower() for lb in labels]
    for name in names:
        if name in mapping:
            return mapping[name]
    return None

def rate_limit_sleep(resp: requests.Response):
    if resp.status_code == 403 and 'rate limit' in resp.text.lower():
        reset = resp.headers.get("X-RateLimit-Reset")
        wait = max(5, int(reset) - int(time.time())) if reset else 60
        print(f"Rate limited. Sleeping {wait}s...")
        time.sleep(wait)

def paged(url: str, params: dict, limit: int):
    """Generic pagination helper (simple)."""
    pulled = 0
    while url and pulled < limit:
        r = SESSION.get(url, params=params)
        if r.status_code == 403:
            rate_limit_sleep(r); continue
        r.raise_for_status()
        items = r.json()
        if isinstance(items, dict):
            yield items
            return
        for it in items:
            yield it
            pulled += 1
            if pulled >= limit:
                return
        
        # parse next link
        nxt = None
        if 'link' in r.headers:
            for part in r.headers['link'].split(','):
                if 'rel="next"' in part:
                    nxt = part[part.find('<')+1:part.find('>')]
                    break
        url = nxt
        params = {}     # only for first page

def fetch_issues(repo: str, limit: int) -> List[dict]:
    url = f"{GITHUB_API}/repos/{repo}/issues"
    # include both open and closed; issues + PRs will appear-filter PRs out later
    params = {"state": "all", "per_page": 100}
    return list(paged(url, params, limit))

def fetch_commits(repo: str, limit: int) -> List[dict]:
    url = f"{GITHUB_API}/repos/{repo}/commits"
    params = {"per_page": 100}
    commits = []
    for item in paged(url, params, limit):
        # enrich each commmit with stats/files (one extra call)
        sha = item.get("sha")
        if not sha:
            continue
        detail_url = f"{GITHUB_API}/repos/{repo}/commits/{sha}"
        r = SESSION.get(detail_url)
        if r.status_code == 403:
            rate_limit_sleep(r); r = SESSION.get(detail_url)
        if r.ok:
            commits.append(r.json())
        time.sleep(0.1)     # to not overload the api
    return commits

def main():
    init_db()
    cfg = load_settings()
    label_map = {k.lower(): v for k, v in (cfg.get("label_mapping") or {}).items()}
    limits = cfg.get("limits", {})
    issue_limit = int(limits.get("issues", 1000))
    commit_limit = int(limits.get("commits", 1000))

    conn = sqlite3.connect(DB_PATH)
    for repo in cfg.get("repos", []):
        print(f"== Repo: {repo} ==")

        # 1) Issues (with labels) -> map severity
        raw_issues = fetch_issues(repo, issue_limit)
        for iss in raw_issues:
            if "pull_request" in iss:
                continue        # skipping PRs here; only taking issues
            sev = map_severity_from_labels(iss.get("labels", []), label_map)
            insert_issue(conn, repo, iss, sev)
        
        # 2) Commits (basic)
        raw_commits = fetch_commits(repo, commit_limit)
        for cm in raw_commits:
            insert_commit(conn, repo, cm)
    
    conn.close()
    print("Done. Data stored in", DB_PATH)

if __name__ == "__main__":
    main()