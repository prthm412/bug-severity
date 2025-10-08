"""
Quick script to check actual date range in Salt repo
"""
from git import Repo
from datetime import datetime

repo_path = "data/raw/salt"
repo = Repo(repo_path)

print("Checking Salt repository dates...\n")

# Get oldest commit
print("Finding oldest commit...")
oldest_commits = list(repo.iter_commits(max_count=1, reverse=True))
if oldest_commits:
    oldest = oldest_commits[0]
    oldest_date = datetime.fromtimestamp(oldest.committed_date)
    print(f"✅ Oldest commit: {oldest_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   SHA: {oldest.hexsha[:8]}")
    print(f"   Message: {oldest.message.split(chr(10))[0][:60]}...")

# Get newest commit
print("\nFinding newest commit...")
newest = list(repo.iter_commits(max_count=1))[0]
newest_date = datetime.fromtimestamp(newest.committed_date)
print(f"✅ Newest commit: {newest_date.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   SHA: {newest.hexsha[:8]}")
print(f"   Message: {newest.message.split(chr(10))[0][:60]}...")

# Count total commits
print("\nCounting all commits (this may take a minute)...")
total = sum(1 for _ in repo.iter_commits())
print(f"✅ Total commits: {total:,}")

# Check commits since 2020
print("\nCounting commits since 2020-01-01...")
since_2020 = sum(1 for _ in repo.iter_commits(since=datetime(2020, 1, 1)))
print(f"✅ Commits since 2020: {since_2020:,}")

print("\n" + "="*60)
print("Repository date range looks correct!" if oldest_date.year < 2020 else "⚠️ Something might be wrong")
print("="*60)