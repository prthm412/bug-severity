"""
Script 1: Clone and set up repositories for mining
"""
import os
import sys
from pathlib import Path
import yaml
from git import Repo
from tqdm import tqdm
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.utils.logging import setup_logging

logger = setup_logging("setup_repos")


def load_repo_config(config_path):
    """Load repository configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def clone_or_update_repo(repo_config, base_path="data/raw"):
    """
    Clone repository or update if it already exists
    
    Args:
        repo_config: Repository configuration dict
        base_path: Base directory for repos
    
    Returns:
        Path to cloned repository
    """
    repo_name = repo_config['repository']['name']
    repo_url = repo_config['repository']['url']
    repo_path = Path(base_path) / repo_name
    
    logger.info(f"Setting up repository: {repo_name}")
    
    # Create base directory
    Path(base_path).mkdir(parents=True, exist_ok=True)
    
    if repo_path.exists():
        logger.info(f"Repository already exists at {repo_path}")
        logger.info("Updating repository...")
        
        try:
            repo = Repo(repo_path)
            origin = repo.remotes.origin
            
            # Fetch updates
            logger.info("Fetching latest changes...")
            origin.fetch()
            
            # Get current branch
            current_branch = repo.active_branch.name
            logger.info(f"Current branch: {current_branch}")
            
            # Pull latest changes
            logger.info("Pulling latest changes...")
            origin.pull(current_branch)
            
            logger.info("✅ Repository updated successfully!")
            
        except Exception as e:
            logger.error(f"Error updating repository: {e}")
            logger.info("Continuing with existing repository...")
    
    else:
        logger.info(f"Cloning repository from {repo_url}...")
        logger.info(f"Target location: {repo_path}")
        logger.info("⏳ This may take 5-30 minutes depending on repository size...")
        
        try:
            # Clone with progress
            repo = Repo.clone_from(
                repo_url,
                repo_path,
                progress=CloneProgress()
            )
            
            logger.info("✅ Repository cloned successfully!")
            
        except Exception as e:
            logger.error(f"❌ Error cloning repository: {e}")
            return None
    
    # Get repository statistics
    try:
        repo = Repo(repo_path)
        
        # Count commits
        commit_count = sum(1 for _ in repo.iter_commits())
        
        # Get branches
        branches = [b.name for b in repo.branches]
        
        # Get date range
        commits_list = list(repo.iter_commits(max_count=1))
        if commits_list:
            latest_commit = commits_list[0]
            first_commits = list(repo.iter_commits(max_count=1, reverse=True))
            if first_commits:
                first_commit = first_commits[0]
                
                logger.info(f"\nRepository Statistics:")
                logger.info(f"  Total commits: {commit_count:,}")
                logger.info(f"  Branches: {', '.join(branches[:5])}")
                logger.info(f"  First commit: {first_commit.committed_datetime.strftime('%Y-%m-%d')}")
                logger.info(f"  Latest commit: {latest_commit.committed_datetime.strftime('%Y-%m-%d')}")
        
    except Exception as e:
        logger.warning(f"Could not get repository statistics: {e}")
    
    return repo_path


class CloneProgress:
    """Progress callback for git clone"""
    def __init__(self):
        self.pbar = None
    
    def __call__(self, op_code, cur_count, max_count=None, message=''):
        if self.pbar is None and max_count:
            self.pbar = tqdm(total=max_count, desc="Cloning")
        
        if self.pbar:
            self.pbar.update(cur_count - self.pbar.n)
            
            if cur_count >= max_count:
                self.pbar.close()


def main():
    """Main function"""
    print("=" * 70)
    print("🔧 BugSage+ Repository Setup")
    print("=" * 70)
    print()
    
    # Check which repos to set up
    config_dir = Path("config/datasets")
    
    if not config_dir.exists():
        logger.error(f"Config directory not found: {config_dir}")
        return
    
    # Find all repo configs
    repo_configs = list(config_dir.glob("*.yaml"))
    
    if not repo_configs:
        logger.error(f"No repository configs found in {config_dir}")
        return
    
    logger.info(f"Found {len(repo_configs)} repository config(s)")
    print()
    
    # Setup each repository
    for config_path in repo_configs:
        logger.info(f"Processing: {config_path.name}")
        print()
        
        try:
            config = load_repo_config(config_path)
            repo_path = clone_or_update_repo(config)
            
            if repo_path:
                logger.info(f"✅ {config['repository']['name']} ready at: {repo_path}")
            else:
                logger.error(f"❌ Failed to setup {config['repository']['name']}")
        
        except Exception as e:
            logger.error(f"❌ Error processing {config_path.name}: {e}")
        
        print("\n" + "-" * 70 + "\n")
    
    print()
    print("=" * 70)
    print("🎉 Repository Setup Complete!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Run: python scripts/02_extract_commits.py")
    print("2. Check: data/raw/ for cloned repositories")


if __name__ == "__main__":
    main()