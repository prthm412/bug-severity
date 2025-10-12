"""
Script 5: Create train/validation/test splits
Uses temporal splitting to simulate real-world deployment
"""
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.utils.logging import setup_logging

logger = setup_logging("create_splits")


def temporal_split(df, val_ratio=0.15, test_ratio=0.15):
    """
    Split data temporally (chronologically)
    
    Training data is oldest, test data is newest.
    This simulates real deployment: train on past, predict future.
    
    Args:
        df: DataFrame with commit_time column
        val_ratio: Validation set ratio
        test_ratio: Test set ratio
    
    Returns:
        train_df, val_df, test_df
    """
    # Sort by time
    df = df.sort_values('commit_time').reset_index(drop=True)
    
    n = len(df)
    
    # Calculate split indices
    train_end = int(n * (1 - val_ratio - test_ratio))
    val_end = int(n * (1 - test_ratio))
    
    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()
    
    return train_df, val_df, test_df


def analyze_split(train_df, val_df, test_df, split_name="temporal"):
    """
    Analyze and report split statistics
    
    Args:
        train_df: Training set
        val_df: Validation set
        test_df: Test set
        split_name: Name of split strategy
    """
    logger.info(f"\nSplit Strategy: {split_name}")
    logger.info(f"  Total records: {len(train_df) + len(val_df) + len(test_df):,}")
    
    logger.info(f"\n  Train set: {len(train_df):,} ({len(train_df)/(len(train_df)+len(val_df)+len(test_df))*100:.1f}%)")
    logger.info(f"    Date range: {train_df['commit_time'].min().date()} to {train_df['commit_time'].max().date()}")
    
    logger.info(f"\n  Validation set: {len(val_df):,} ({len(val_df)/(len(train_df)+len(val_df)+len(test_df))*100:.1f}%)")
    logger.info(f"    Date range: {val_df['commit_time'].min().date()} to {val_df['commit_time'].max().date()}")
    
    logger.info(f"\n  Test set: {len(test_df):,} ({len(test_df)/(len(train_df)+len(val_df)+len(test_df))*100:.1f}%)")
    logger.info(f"    Date range: {test_df['commit_time'].min().date()} to {test_df['commit_time'].max().date()}")
    
    # Label distribution in each split
    logger.info(f"\n  Label Distribution:")
    
    for split_name, split_df in [("Train", train_df), ("Val", val_df), ("Test", test_df)]:
        logger.info(f"    {split_name}:")
        for severity in ['high', 'medium', 'low']:
            count = (split_df['severity_label'] == severity).sum()
            pct = count / len(split_df) * 100
            logger.info(f"      {severity:8s}: {count:4d} ({pct:5.1f}%)")


def create_splits(input_path, output_dir):
    """
    Create and save train/val/test splits
    
    Args:
        input_path: Path to temporal features CSV
        output_dir: Directory to save splits
    """
    logger.info(f"Loading data from {input_path}...")
    df = pd.read_csv(input_path)
    df['commit_time'] = pd.to_datetime(df['commit_time'])
    
    logger.info(f"Creating temporal splits for {len(df):,} records...")
    
    # Create splits
    train_df, val_df, test_df = temporal_split(df, val_ratio=0.15, test_ratio=0.15)
    
    # Analyze
    analyze_split(train_df, val_df, test_df)
    
    # Save splits
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    train_path = output_dir / "train.csv"
    val_path = output_dir / "val.csv"
    test_path = output_dir / "test.csv"
    
    logger.info(f"\nSaving splits:")
    
    logger.info(f"  Train: {train_path}")
    train_df.to_csv(train_path, index=False)
    
    logger.info(f"  Val: {val_path}")
    val_df.to_csv(val_path, index=False)
    
    logger.info(f"  Test: {test_path}")
    test_df.to_csv(test_path, index=False)
    
    # Calculate total size
    total_size = sum([
        train_path.stat().st_size,
        val_path.stat().st_size,
        test_path.stat().st_size
    ]) / (1024 * 1024)
    
    logger.info(f"\nTotal size: {total_size:.2f} MB")
    
    return train_df, val_df, test_df


def main():
    """Main function"""
    print("=" * 70)
    print("BugSage+ Dataset Splitting")
    print("=" * 70)
    print()
    
    # Paths
    input_path = Path("data/interim/salt_commits_temporal.csv")
    output_dir = Path("data/processed")
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        logger.error("Run: python scripts/04_compute_temporal.py first")
        return
    
    logger.info(f"Input: {input_path}")
    logger.info(f"Output directory: {output_dir}")
    
    # Create splits
    train_df, val_df, test_df = create_splits(input_path, output_dir)
    
    print()
    print("=" * 70)
    print("Dataset Splitting Complete")
    print("=" * 70)
    print()
    print("Files created:")
    print(f"  - data/processed/train.csv ({len(train_df):,} records)")
    print(f"  - data/processed/val.csv ({len(val_df):,} records)")
    print(f"  - data/processed/test.csv ({len(test_df):,} records)")


if __name__ == "__main__":
    main()