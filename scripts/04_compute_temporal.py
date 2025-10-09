"""
Script 4: Compute temporal features for commits
Calculates churn, file age, and recent severe bug counts
"""
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import logging

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.utils.logging import setup_logging

logger = setup_logging("compute_temporal")


def compute_file_age(df, file_path, commit_time):
    """
    Compute how old a file is at commit time
    
    Args:
        df: Full dataframe
        file_path: Path to file
        commit_time: Current commit timestamp
    
    Returns:
        Age in days, or None if first commit
    """
    file_commits = df[df['file_path'] == file_path]
    file_commits = file_commits[file_commits['commit_time'] < commit_time]
    
    if len(file_commits) == 0:
        return 0  # New file
    
    first_commit = file_commits['commit_time'].min()
    age_days = (commit_time - first_commit).days
    
    return age_days


def compute_churn(df, file_path, commit_time, window_days=60):
    """
    Compute file churn (number of changes in time window)
    
    Args:
        df: Full dataframe
        file_path: Path to file
        commit_time: Current commit timestamp
        window_days: Look-back window
    
    Returns:
        Number of commits to this file in window
    """
    window_start = commit_time - timedelta(days=window_days)
    
    file_commits = df[df['file_path'] == file_path]
    recent_commits = file_commits[
        (file_commits['commit_time'] >= window_start) &
        (file_commits['commit_time'] < commit_time)
    ]
    
    return len(recent_commits)


def compute_recent_severe(df, file_path, commit_time, window_days=30):
    """
    Count recent high-severity bugs in this file
    
    Args:
        df: Full dataframe
        file_path: Path to file
        commit_time: Current commit timestamp
        window_days: Look-back window
    
    Returns:
        Number of high-severity commits in window
    """
    window_start = commit_time - timedelta(days=window_days)
    
    file_commits = df[df['file_path'] == file_path]
    recent_high = file_commits[
        (file_commits['commit_time'] >= window_start) &
        (file_commits['commit_time'] < commit_time) &
        (file_commits['severity_label'] == 'high')
    ]
    
    return len(recent_high)


def compute_temporal_features(input_path, output_path):
    """
    Compute all temporal features
    
    Args:
        input_path: Path to labeled commits CSV
        output_path: Path to save with temporal features
    """
    logger.info(f"Loading data from {input_path}...")
    df = pd.read_csv(input_path)
    df['commit_time'] = pd.to_datetime(df['commit_time'])
    
    logger.info(f"Computing temporal features for {len(df):,} records...")
    
    # Sort by time (important for temporal calculations)
    df = df.sort_values('commit_time').reset_index(drop=True)
    
    # Initialize feature columns
    df['file_age_days'] = 0
    df['churn_60d'] = 0
    df['recent_severe_30d'] = 0
    
    # Compute features for each record
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Computing features"):
        # File age
        df.at[idx, 'file_age_days'] = compute_file_age(
            df.iloc[:idx+1],  # Only use data up to this point
            row['file_path'],
            row['commit_time']
        )
        
        # Churn (60-day window)
        df.at[idx, 'churn_60d'] = compute_churn(
            df.iloc[:idx+1],
            row['file_path'],
            row['commit_time'],
            window_days=60
        )
        
        # Recent severe bugs (30-day window)
        df.at[idx, 'recent_severe_30d'] = compute_recent_severe(
            df.iloc[:idx+1],
            row['file_path'],
            row['commit_time'],
            window_days=30
        )
    
    # Statistics
    logger.info(f"\nTemporal Feature Statistics:")
    logger.info(f"  File age (days):")
    logger.info(f"    Mean: {df['file_age_days'].mean():.1f}")
    logger.info(f"    Median: {df['file_age_days'].median():.1f}")
    logger.info(f"    Max: {df['file_age_days'].max():.0f}")
    
    logger.info(f"  Churn (60d):")
    logger.info(f"    Mean: {df['churn_60d'].mean():.1f}")
    logger.info(f"    Median: {df['churn_60d'].median():.1f}")
    logger.info(f"    Max: {df['churn_60d'].max():.0f}")
    
    logger.info(f"  Recent severe (30d):")
    logger.info(f"    Mean: {df['recent_severe_30d'].mean():.2f}")
    logger.info(f"    Median: {df['recent_severe_30d'].median():.1f}")
    logger.info(f"    Max: {df['recent_severe_30d'].max():.0f}")
    
    # Files with high churn
    high_churn = df[df['churn_60d'] > 10].copy()
    if len(high_churn) > 0:
        logger.info(f"\n  High churn files (>10 changes in 60d): {len(high_churn):,}")
        top_churn = high_churn.groupby('file_path')['churn_60d'].max().sort_values(ascending=False).head(5)
        for file_path, churn in top_churn.items():
            logger.info(f"    {file_path}: {churn:.0f} changes")
    
    # Save
    logger.info(f"\nSaving to: {output_path}")
    df.to_csv(output_path, index=False)
    
    file_size_mb = Path(output_path).stat().st_size / (1024 * 1024)
    logger.info(f"Saved {len(df):,} records ({file_size_mb:.2f} MB)")
    
    return df


def main():
    """Main function"""
    print("=" * 70)
    print("BugSage+ Temporal Feature Computation")
    print("=" * 70)
    print()
    
    # Paths
    input_path = Path("data/interim/salt_commits_labeled.csv")
    output_path = Path("data/interim/salt_commits_temporal.csv")
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        logger.error("Run: python scripts/03_label_commits.py first")
        return
    
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    print()
    
    # Compute features
    df = compute_temporal_features(input_path, output_path)
    
    print()
    print("=" * 70)
    print("Temporal Feature Computation Complete")
    print("=" * 70)


if __name__ == "__main__":
    main()