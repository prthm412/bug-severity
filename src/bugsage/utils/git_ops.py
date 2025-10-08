"""
Git operations for mining commits and extracting diffs
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
import git
from unidiff import PatchSet


@dataclass
class Hunk:
    """Represents a contiguous block of changes in a diff"""
    hunk_id: int
    file_path: str
    old_start: int
    old_lines: int
    new_start: int
    new_lines: int
    content: str
    added_lines: List[tuple]  # (line_num, content)
    removed_lines: List[tuple]  # (line_num, content)


@dataclass
class Diff:
    """Represents the complete diff for a commit"""
    commit_sha: str
    parent_sha: str
    hunks: List[Hunk]
    files_changed: List[str]
    insertions: int
    deletions: int
    raw_diff: str


@dataclass
class CommitInfo:
    """Metadata about a commit"""
    sha: str
    parent_sha: Optional[str]
    message: str
    author: str
    author_email: str
    commit_time: datetime
    files_changed: List[str]
    insertions: int
    deletions: int


def list_commits(
    repo_path: str,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    branch: str = "master"
) -> List[str]:
    """
    List commit SHAs in a repository within a time range.
    
    Args:
        repo_path: Path to git repository
        since: Start date (inclusive)
        until: End date (inclusive)
        branch: Branch name to traverse
    
    Returns:
        List of commit SHAs (most recent first)
    """
    repo = git.Repo(repo_path)
    
    # Check if branch exists, fallback to main if not
    try:
        repo.commit(branch)
    except:
        if branch == "master":
            try:
                repo.commit("main")
                branch = "main"
            except:
                # Get default branch
                branch = repo.active_branch.name
    
    # Build kwargs for iter_commits
    kwargs = {"rev": branch}
    if since:
        kwargs["since"] = since
    if until:
        kwargs["until"] = until
    
    commits = []
    for commit in repo.iter_commits(**kwargs):
        commits.append(commit.hexsha)
    
    return commits


def get_commit_info(repo_path: str, commit_sha: str) -> CommitInfo:
    """
    Extract metadata for a specific commit.
    
    Args:
        repo_path: Path to git repository
        commit_sha: Commit SHA to examine
    
    Returns:
        CommitInfo dataclass with metadata
    """
    repo = git.Repo(repo_path)
    commit = repo.commit(commit_sha)
    
    # Get parent (handle initial commit case)
    parent_sha = commit.parents[0].hexsha if commit.parents else None
    
    # Get stats
    stats = commit.stats.total
    files_changed = list(commit.stats.files.keys())
    
    return CommitInfo(
        sha=commit.hexsha,
        parent_sha=parent_sha,
        message=commit.message.strip(),
        author=commit.author.name,
        author_email=commit.author.email,
        commit_time=datetime.fromtimestamp(commit.committed_date),
        files_changed=files_changed,
        insertions=stats.get("insertions", 0),
        deletions=stats.get("deletions", 0)
    )


def get_diff(repo_path: str, commit_sha: str) -> Diff:
    """
    Extract the full diff for a commit.
    
    Args:
        repo_path: Path to git repository
        commit_sha: Commit SHA to diff
    
    Returns:
        Diff dataclass containing all changes
    """
    repo = git.Repo(repo_path)
    commit = repo.commit(commit_sha)
    
    # Handle initial commit
    if not commit.parents:
        return Diff(
            commit_sha=commit_sha,
            parent_sha=None,
            hunks=[],
            files_changed=[],
            insertions=0,
            deletions=0,
            raw_diff=""
        )
    
    parent = commit.parents[0]
    
    # Get unified diff
    raw_diff = repo.git.diff(parent.hexsha, commit_sha, unified=3)
    
    # Parse diff
    hunks = split_hunks(raw_diff)
    
    stats = commit.stats.total
    return Diff(
        commit_sha=commit_sha,
        parent_sha=parent.hexsha,
        hunks=hunks,
        files_changed=list(commit.stats.files.keys()),
        insertions=stats.get("insertions", 0),
        deletions=stats.get("deletions", 0),
        raw_diff=raw_diff
    )


def split_hunks(diff_text: str) -> List[Hunk]:
    """
    Split a unified diff into individual hunks.
    
    Args:
        diff_text: Raw unified diff string
    
    Returns:
        List of Hunk objects
    """
    if not diff_text.strip():
        return []
    
    try:
        patch_set = PatchSet(diff_text)
    except Exception as e:
        print(f"Warning: Failed to parse diff: {e}")
        return []
    
    all_hunks = []
    hunk_counter = 0
    
    for patched_file in patch_set:
        file_path = patched_file.path
        
        for hunk in patched_file:
            added_lines = []
            removed_lines = []
            hunk_content = []
            
            for line in hunk:
                hunk_content.append(str(line))
                
                if line.is_added:
                    added_lines.append((line.target_line_no, line.value))
                elif line.is_removed:
                    removed_lines.append((line.source_line_no, line.value))
            
            all_hunks.append(Hunk(
                hunk_id=hunk_counter,
                file_path=file_path,
                old_start=hunk.source_start,
                old_lines=hunk.source_length,
                new_start=hunk.target_start,
                new_lines=hunk.target_length,
                content="".join(hunk_content),
                added_lines=added_lines,
                removed_lines=removed_lines
            ))
            hunk_counter += 1
    
    return all_hunks


def get_file_content_at_commit(
    repo_path: str, 
    commit_sha: str, 
    file_path: str
) -> Optional[str]:
    """
    Get the content of a file at a specific commit.
    
    Args:
        repo_path: Path to git repository
        commit_sha: Commit SHA
        file_path: Path to file within repo
    
    Returns:
        File content as string, or None if file doesn't exist
    """
    repo = git.Repo(repo_path)
    
    try:
        commit = repo.commit(commit_sha)
        return commit.tree[file_path].data_stream.read().decode('utf-8')
    except (KeyError, AttributeError):
        return None


def get_file_history(
    repo_path: str,
    file_path: str,
    max_count: Optional[int] = None
) -> List[CommitInfo]:
    """
    Get commit history for a specific file.
    
    Args:
        repo_path: Path to git repository
        file_path: Path to file within repo
        max_count: Maximum number of commits to retrieve
    
    Returns:
        List of CommitInfo objects affecting this file
    """
    repo = git.Repo(repo_path)
    
    commits = []
    for commit in repo.iter_commits(paths=file_path, max_count=max_count):
        commits.append(get_commit_info(repo_path, commit.hexsha))
    
    return commits