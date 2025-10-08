"""
Script 3: Label commits with severity (low, medium, high)
Uses hybrid approach: keywords + file criticality + issue tracking
"""
import os
import sys
from pathlib import Path
import yaml
import pandas as pd
import re
from tqdm import tqdm
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.utils.logging import setup_logging

logger = setup_logging("label_commits")


def load_repo_config(config_path):
    """Load repository configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def extract_keywords(text, keyword_dict):
    """
    Check if text contains keywords from dictionary
    
    Args:
        text: Text to search (lowercase)
        keyword_dict: Dict of {severity: [keywords]}
    
    Returns:
        Highest severity found, or None
    """
    text_lower = text.lower()
    
    # Check in priority order: high -> medium -> low
    for severity in ['high', 'medium', 'low']:
        if severity in keyword_dict:
            for keyword in keyword_dict[severity]:
                if keyword in text_lower:
                    return severity
    
    return None


def classify_by_keywords(message, config):
    """
    Classify severity based on commit message keywords
    
    Args:
        message: Commit message
        config: Repository configuration
    
    Returns:
        tuple: (severity, confidence, matched_keyword)
    """
    keyword_patterns = config['labels'].get('keyword_patterns', {})
    
    severity = extract_keywords(message, keyword_patterns)
    
    if severity:
        # High confidence if explicit severity keywords
        if severity == 'high':
            confidence = 0.85
        elif severity == 'medium':
            confidence = 0.70
        else:
            confidence = 0.60
        
        return severity, confidence, 'keyword'
    
    return None, 0.0, None


def classify_by_file_path(file_path):
    """
    Classify severity based on file criticality
    
    Critical files (high severity):
    - Authentication, security, crypto
    - Core transport/networking
    - Database/data handling
    
    Args:
        file_path: Path to file
    
    Returns:
        tuple: (severity, confidence, reason)
    """
    path_lower = file_path.lower()
    
    # High severity paths
    high_severity_patterns = [
        'auth', 'security', 'crypt', 'password',
        'token', 'session', 'permission',
        'transport/tcp', 'transport/zeromq',
        'database', 'sql', 'query'
    ]
    
    for pattern in high_severity_patterns:
        if pattern in path_lower:
            return 'high', 0.60, f'critical_file:{pattern}'
    
    # Medium severity paths
    medium_severity_patterns = [
        'modules/', 'states/', 'grains/',
        'utils/', 'transport/', 'client/'
    ]
    
    for pattern in medium_severity_patterns:
        if pattern in path_lower:
            return 'medium', 0.50, f'core_file:{pattern}'
    
    # Low severity paths
    low_severity_patterns = [
        'test', 'doc', 'example', 'template',
        'readme', 'changelog', 'license'
    ]
    
    for pattern in low_severity_patterns:
        if pattern in path_lower:
            return 'low', 0.70, f'non_critical:{pattern}'
    
    # Default: medium (unknown file type)
    return 'medium', 0.30, 'unknown_file'


def classify_by_change_size(insertions, deletions):
    """
    Classify severity based on change size
    
    Larger changes = potentially higher impact
    
    Args:
        insertions: Lines added
        deletions: Lines deleted
    
    Returns:
        tuple: (severity_boost, confidence, reason)
    """
    total_lines = insertions + deletions
    
    if total_lines > 500:
        return 'high', 0.40, 'very_large_change'
    elif total_lines > 200:
        return 'medium', 0.35, 'large_change'
    elif total_lines < 5:
        return 'low', 0.50, 'tiny_change'
    
    return None, 0.0, None


def assign_severity(row, config):
    """
    Assign severity to a commit using hybrid approach
    
    Priority:
    1. Keywords in commit message (highest confidence)
    2. File path criticality
    3. Change size
    
    Args:
        row: DataFrame row with commit data
        config: Repository configuration
    
    Returns:
        dict: {severity, confidence, reasons}
    """
    signals = []
    
    # Signal 1: Commit message keywords
    keyword_sev, keyword_conf, keyword_reason = classify_by_keywords(
        row['message'], config
    )
    if keyword_sev:
        signals.append({
            'severity': keyword_sev,
            'confidence': keyword_conf,
            'source': 'message_keyword',
            'reason': keyword_reason
        })
    
    # Signal 2: File path
    file_sev, file_conf, file_reason = classify_by_file_path(row['file_path'])
    signals.append({
        'severity': file_sev,
        'confidence': file_conf,
        'source': 'file_path',
        'reason': file_reason
    })
    
    # Signal 3: Change size
    size_sev, size_conf, size_reason = classify_by_change_size(
        row['insertions'], row['deletions']
    )
    if size_sev:
        signals.append({
            'severity': size_sev,
            'confidence': size_conf,
            'source': 'change_size',
            'reason': size_reason
        })
    
    # Combine signals
    # Weight by confidence, prioritize higher severity
    severity_scores = {'high': 0, 'medium': 0, 'low': 0}
    
    for signal in signals:
        sev = signal['severity']
        conf = signal['confidence']
        severity_scores[sev] += conf
    
    # Get highest score
    final_severity = max(severity_scores, key=severity_scores.get)
    final_confidence = severity_scores[final_severity] / len(signals)
    
    # Compile reasons
    reasons = [s['reason'] for s in signals if s['reason']]
    
    return {
        'severity_label': final_severity,
        'severity_confidence': final_confidence,
        'severity_reasons': '; '.join(reasons)
    }


def label_commits(input_path, config):
    """
    Label all commits with severity
    
    Args:
        input_path: Path to raw commits CSV
        config: Repository configuration
    
    Returns:
        pandas.DataFrame with severity labels
    """
    logger.info(f"Loading commits from {input_path}...")
    df = pd.read_csv(input_path)
    
    logger.info(f"Labeling {len(df):,} records...")
    
    # Apply labeling
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Labeling commits"):
        result = assign_severity(row, config)
        results.append(result)
    
    # Add results to dataframe
    results_df = pd.DataFrame(results)
    df = pd.concat([df, results_df], axis=1)
    
    # Statistics
    logger.info(f"\nLabeling Statistics:")
    
    severity_counts = df['severity_label'].value_counts()
    for severity, count in severity_counts.items():
        pct = count / len(df) * 100
        logger.info(f"  {severity.upper():8s}: {count:5d} ({pct:5.2f}%)")
    
    # Average confidence
    avg_confidence = df['severity_confidence'].mean()
    logger.info(f"\n  Average confidence: {avg_confidence:.3f}")
    
    # High confidence labels
    high_conf = (df['severity_confidence'] > 0.7).sum()
    high_conf_pct = high_conf / len(df) * 100
    logger.info(f"  High confidence (>0.7): {high_conf:,} ({high_conf_pct:.1f}%)")
    
    return df


def main():
    """Main function"""
    print("=" * 70)
    print("  BugSage+ Severity Labeling")
    print("=" * 70)
    print()
    
    # Load config
    config_path = Path("config/datasets/salt.yaml")
    
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        return
    
    logger.info(f"Loading config: {config_path}")
    config = load_repo_config(config_path)
    
    # Input/output paths
    input_path = Path(config['output']['raw_commits'])
    output_path = Path(config['output']['labeled_commits'])
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        logger.error("Run: python scripts/02_extract_commits.py first")
        return
    
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    print()
    
    # Label commits
    df = label_commits(input_path, config)
    
    # Save labeled data
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"\nSaving to: {output_path}")
    df.to_csv(output_path, index=False)
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"Saved {len(df):,} labeled records ({file_size_mb:.2f} MB)")
    
    print()
    print("=" * 70)
    print("Labeling Complete!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Explore labels: notebooks/02_label_analysis.ipynb")
    print("2. Build features: python scripts/04_build_features.py")


if __name__ == "__main__":
    main()