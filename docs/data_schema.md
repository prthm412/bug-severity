# BugSage+ Data Schema

## CSV Columns (train/val/test)

| Column Name | Type | Description |
|------------|------|-------------|
| `project` | str | Repository name |
| `commit_sha` | str | Git commit hash |
| `parent_sha` | str | Parent commit hash |
| `commit_time` | datetime | When commit was made |
| `author` | str | Commit author |
| `author_email` | str | Author email |
| `message` | str | Commit message |
| `file_path` | str | Changed file path |
| `files_changed` | int | Number of files in commit |
| `insertions` | int | Lines added |
| `deletions` | int | Lines deleted |
| `hunks_count` | int | Number of hunks |
| `issue_id` | str | Linked issue ID |
| `is_bugfix` | bool | Is this a bug fix? |
| `patch_text` | str | **Git diff content** |
| `severity_label` | str | **low/medium/high** |
| `severity_confidence` | float | Label confidence |
| `severity_reasons` | str | Why this severity |
| `file_age_days` | float | **Temporal: File age** |
| `churn_60d` | float | **Temporal: 60-day churn** |
| `recent_severe_30d` | int | **Temporal: Recent severe bugs** |

## Model Input Features

**Text:** `patch_text` (tokenized to 512 tokens)  
**Temporal:** `[churn_60d, file_age_days, recent_severe_30d]`  
**Target:** `severity_label` → encoded as 0 (low), 1 (medium), 2 (high)