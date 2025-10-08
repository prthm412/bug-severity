"""
Script 2: Extract commits from repository
Mines commits with diffs, metadata, and statistics
"""
import os
import sys
from pathlib import Path
import yaml
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.utils.logging import setup_logging
from bugsage.utils.git_ops import (
    list_commits, get_commit_info, get_diff, Diff, CommitInfo
)

logger = setup_logging("extract_commits")


def load_repo_config(config_path):
    """Load repository configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def parse_date(date_str):
    """Parse date string to datetime"""
    if date_str is None:
        return None
    
    if isinstance(date_str, str):
        return datetime.fromisoformat(date_str)
    
    return date_str


def extract_issue_id(commit_message):
    """
    Extract issue ID from commit message
    
    Looks for patterns like:
    - #12345
    - GH-12345
    - fixes #12345
    - closes #12345
    """
    import re
    
    patterns = [
        r'#(\d+)',
        r'GH-(\d+)',
        r'(?:fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved)\s+#(\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, commit_message, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None


def is_bugfix_commit(commit_message):
    """Determine if commit is a bug fix based on message"""
    message_lower = commit_message.lower()
    
    bugfix_keywords = [
        'fix', 'bug', 'issue', 'problem', 'error',
        'crash', 'failure', 'incorrect', 'wrong'
    ]
    
    return any(keyword in message_lower for keyword in bugfix_keywords)


def should_include_commit(commit_info, diff, config):
    """
    Determine if commit should be included based on filters
    
    Args:
        commit_info: CommitInfo object
        diff: Diff object
        config: Repository configuration
    
    Returns:
        tuple: (should_include, reason)
    """
    filters = config['extraction']
    
    # Check if merge commit
    if filters.get('exclude_merges', True):
        if 'merge' in commit_info.message.lower():
            return False, "merge_commit"
    
    # Check number of files changed
    min_files = filters.get('min_files_changed', 1)
    max_files = filters.get('max_files_changed', 50)
    
    if len(commit_info.files_changed) < min_files:
        return False, "too_few_files"
    
    if len(commit_info.files_changed) > max_files:
        return False, "too_many_files"
    
    # Check lines changed
    total_lines = commit_info.insertions + commit_info.deletions
    min_lines = filters.get('min_lines_changed', 1)
    max_lines = filters.get('max_lines_changed', 1000)
    
    if total_lines < min_lines:
        return False, "too_few_lines"
    
    if total_lines > max_lines:
        return False, "too_many_lines"
    
    # Check file extensions
    include_exts = filters.get('include_extensions', [])
    if include_exts:
        has_valid_ext = any(
            any(f.endswith(ext) for ext in include_exts)
            for f in commit_info.files_changed
        )
        if not has_valid_ext:
            return False, "no_valid_extensions"
    
    # Check excluded paths
    exclude_paths = filters.get('exclude_paths', [])
    if exclude_paths:
        has_excluded = any(
            any(ex in f for ex in exclude_paths)
            for f in commit_info.files_changed
        )
        if has_excluded:
            return False, "excluded_path"
    
    return True, "included"


def extract_commits(repo_path, config):
    """
    Extract commits from repository
    
    Args:
        repo_path: Path to git repository
        config: Repository configuration
    
    Returns:
        pandas.DataFrame with extracted commits
    """
    repo_name = config['repository']['name']
    logger.info(f"Extracting commits from {repo_name}...")
    
    # Parse date range
    start_date = parse_date(config['extraction'].get('start_date'))
    end_date = parse_date(config['extraction'].get('end_date'))
    
    if start_date:
        logger.info(f"Start date: {start_date.strftime('%Y-%m-%d')}")
    if end_date:
        logger.info(f"End date: {end_date.strftime('%Y-%m-%d')}")
    
    # Get branch
    branches = config['extraction'].get('branches', ['master'])
    branch = branches[0]
    
    logger.info(f"Branch: {branch}")
    
    # List all commits
    logger.info("Listing commits...")
    commit_shas = list_commits(
        repo_path,
        since=start_date,
        until=end_date,
        branch=branch
    )
    
    total_commits = len(commit_shas)
    logger.info(f"Found {total_commits:,} commits in date range")
    
    if total_commits == 0:
        logger.warning("No commits found! Check date range and branch.")
        return pd.DataFrame()
    
    # Extract commit data
    records = []
    filter_stats = {}
    
    logger.info("Extracting commit details...")
    
    for sha in tqdm(commit_shas, desc="Processing commits"):
        try:
            # Get commit info
            commit_info = get_commit_info(repo_path, sha)
            
            # Get diff
            diff = get_diff(repo_path, sha)
            
            # Check filters
            include, reason = should_include_commit(commit_info, diff, config)
            
            # Track filter statistics
            filter_stats[reason] = filter_stats.get(reason, 0) + 1
            
            if not include:
                continue
            
            # Extract issue ID
            issue_id = extract_issue_id(commit_info.message)
            
            # Determine if bugfix
            is_bugfix = is_bugfix_commit(commit_info.message)
            
            # Create record for each file changed
            for file_path in commit_info.files_changed:
                record = {
                    'project': repo_name,
                    'repo': repo_name,
                    'commit_sha': commit_info.sha,
                    'parent_sha': commit_info.parent_sha,
                    'commit_time': commit_info.commit_time,
                    'author': commit_info.author,
                    'author_email': commit_info.author_email,
                    'message': commit_info.message,
                    'file_path': file_path,
                    'files_changed': len(commit_info.files_changed),
                    'insertions': commit_info.insertions,
                    'deletions': commit_info.deletions,
                    'hunks_count': len(diff.hunks),
                    'issue_id': issue_id,
                    'is_bugfix': is_bugfix,
                    'patch_text': diff.raw_diff,
                }
                
                records.append(record)
        
        except Exception as e:
            logger.warning(f"Error processing commit {sha[:8]}: {e}")
            continue
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Log statistics
    logger.info(f"\n📊 Extraction Statistics:")
    logger.info(f"  Total commits scanned: {total_commits:,}")
    logger.info(f"  Commits included: {filter_stats.get('included', 0):,}")
    logger.info(f"  Total records (commit-file pairs): {len(df):,}")
    logger.info(f"\n  Filter breakdown:")
    
    for reason, count in sorted(filter_stats.items()):
        if reason != 'included':
            logger.info(f"    - {reason}: {count:,}")
    
    if len(df) > 0:
        logger.info(f"\n  Bugfix commits: {df['is_bugfix'].sum():,} ({df['is_bugfix'].mean()*100:.1f}%)")
        logger.info(f"  Commits with issue IDs: {df['issue_id'].notna().sum():,} ({df['issue_id'].notna().mean()*100:.1f}%)")
        logger.info(f"  Date range: {df['commit_time'].min()} to {df['commit_time'].max()}")
    
    return df


def main():
    """Main function"""
    print("=" * 70)
    print("⛏️  BugSage+ Commit Extraction")
    print("=" * 70)
    print()
    
    # Load config
    config_path = Path("config/datasets/salt.yaml")
    
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        return
    
    logger.info(f"Loading config: {config_path}")
    config = load_repo_config(config_path)
    
    # Check if repo exists
    repo_name = config['repository']['name']
    repo_path = Path("data/raw") / repo_name
    
    if not repo_path.exists():
        logger.error(f"Repository not found: {repo_path}")
        logger.error("Run: python scripts/01_setup_repos.py first")
        return
    
    logger.info(f"Repository: {repo_path}")
    print()
    
    # Extract commits
    df = extract_commits(str(repo_path), config)
    
    if len(df) == 0:
        logger.error("No commits extracted!")
        return
    
    # Save raw commits
    output_path = Path(config['output']['raw_commits'])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"\n💾 Saving to: {output_path}")
    df.to_csv(output_path, index=False)
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"✅ Saved {len(df):,} records ({file_size_mb:.2f} MB)")
    
    print()
    print("=" * 70)
    print("🎉 Extraction Complete!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Explore data: notebooks/01_data_exploration.ipynb")
    print("2. Label commits: python scripts/03_label_commits.py")


if __name__ == "__main__":
    main()