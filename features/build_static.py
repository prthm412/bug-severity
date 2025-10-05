# features/build_static.py

import os, json, sqlite3, re
from datetime import datetime
import pandas as pd
from pathlib import Path

DB_PATH = "data/dev.sqlite3"
OUT_TABLE = "features"

def _entropy(txt: str) -> float:
    if not txt: return 0.0
    import math
    freqs = {}
    for ch in txt.lower():
        freqs[ch] = freqs.get(ch, 0) + 1
    n = len(txt)
    return -sum((c/n) * math.log2(c/n) for c in freqs.values())

def load_commits(conn):
    q = """
    SELECT sha, repo_full_name, author, timestamp, message,
        COALESCE(files_changed,0)   AS files_changed,
        COALESCE(insertions,0)      AS insertions,
        COALESCE(deletions,0)       AS deletions
    FROM commits
    ORDER BY timestamp DESC
    """
    return pd.read_sql_query(q, conn)

def build_static_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["net_churn"] = df["insertions"] - df["deletions"]
    df["abs_churn"] = (df["insertions"].abs() + df["deletions"].abs())
    df["msg_len"] = df["message"].fillna("").str.len()
    df["msg_entropy"] = df["message"].fillna("").apply(_entropy)
    # simple keyword flags (baseline signal)
    kws = {
        "fix": r"\bfix(e[ds])?\b",
        "bug": r"\bbug(s)?\b",
        "refactor": r"\brefactor(ing|ed)?\b",
        "test": r"\btest(s|ing)?\b",
        "revert": r"\brevert(ed)?\b",
    }
    for name, pat in kws.items():
        df[f"kw_{name}"] = df["message"].fillna("").str.contains(pat, flags=re.I, regex=True).astype(int)
    
    feats = df[[
        "sha", "repo_full_name", "author", "ts", "files_changed",
        "insertions", "deletions", "net_churn", "abs_churn",
        "msg_len", "msg_entropy", "kw_fix", "kw_bug", "kw_refactor", "kw_test", "kw_revert"
    ]].rename(columns={"ts":"timestamp"})
    return feats

def write_features(conn, feats: pd.DataFrame):
    # creates table if not exists
    conn.execute("""
    CREATE TABLE IF NOT EXISTS features (
        commit_sha TEXT PRIMARY KEY,
        feature_vector_json TEXT,
        generated_at TEXT DEFAULT (datetime('now'))
    )""")
    # upsert rows
    cur = conn.cursor()
    for _, r in feats.iterrows():
        payload = {
            "files_changed": int(r["files_changed"]),
            "insertions": int(r["insertions"]),
            "deletions": int(r["deletions"]),
            "net_churn": int(r["net_churn"]),
            "abs_churn": int(r["abs_churn"]),
            "msg_len": int(r["msg_len"]),
            "msg_entropy": float(r["msg_entropy"]),
            "kw_fix": int(r["kw_fix"]),
            "kw_bug": int(r["kw_bug"]),
            "kw_refactor": int(r["kw_refactor"]),
            "kw_test": int(r["kw_test"]),
            "kw_revert": int(r["kw_revert"]),
        }
        cur.execute(
            "INSERT OR REPLACE INTO features (commit_sha, feature_vector_json) VALUES (?, ?)",
            (r["sha"], json.dumps(payload))
        )
    conn.commit()

def main():
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        commits = load_commits(conn)
        if commits.empty:
            print("No commits found. Run historic_backfill first.")
            return
        feats = build_static_features(commits)
        write_features(conn, feats)
        print(f"Wrote static features for {len(feats)} commits → table 'features'.")

if __name__ == "__main__":
    main()