"""
Script 8: Explain individual commit predictions
Demonstrates the explainability features of BugSage+
"""
import sys
from pathlib import Path
import torch
import pandas as pd
import argparse
import logging

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.data.dataset import CommitDataset
from bugsage.models.bugsage import BugSageBaseline
from bugsage.explain.attention import AttentionExtractor, AttentionAnalyzer
from bugsage.explain.attribution import GradientAttributor
from bugsage.explain.visualizer import ExplanationVisualizer
from bugsage.utils.logging import setup_logging

logger = setup_logging("explain_commit")


def load_model(checkpoint_path: str, device: str):
    """Load trained model from checkpoint"""
    model = BugSageBaseline(
        encoder_name="microsoft/codebert-base",
        temporal_dim=3,
        hidden_dims=[256, 128],
        num_classes=3,
        dropout=0.3
    )
    
    checkpoint = torch.load(checkpoint_path, map_location=device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model = model.to(device)
    model.eval()
    
    logger.info(f"Loaded model from epoch {checkpoint['epoch']}")
    logger.info(f"Validation F1: {checkpoint['val_f1']:.4f}")
    
    return model


def explain_commit_by_index(
    index: int,
    dataset_path: str,
    model,
    device: str,
    method: str = 'attention'
):
    """
    Explain a specific commit from the dataset
    
    Args:
        index: Index in the dataset
        dataset_path: Path to CSV file
        model: Trained model
        device: Device to use
        method: Explanation method ('attention' or 'gradients')
    """
    # Load dataset
    df = pd.read_csv(dataset_path)
    
    if index >= len(df):
        logger.error(f"Index {index} out of range (max: {len(df)-1})")
        return
    
    # Get commit
    row = df.iloc[index]
    commit_sha = row['commit_sha']
    diff_text = row['patch_text']
    true_severity = row['severity_label']
    temporal_features = torch.tensor([
        row['churn_60d'],
        row['file_age_days'],
        row['recent_severe_30d']
    ], dtype=torch.float32)
    
    logger.info(f"\nAnalyzing commit: {commit_sha}")
    logger.info(f"True severity: {true_severity}")
    logger.info(f"Temporal features: churn={row['churn_60d']:.2f}, age={row['file_age_days']:.1f}, recent_severe={row['recent_severe_30d']}")
    print("\n" + "="*70)
    print(f"Commit: {commit_sha}")
    print(f"True Severity: {true_severity.upper()}")
    print("="*70)
    
    # Initialize visualizer
    visualizer = ExplanationVisualizer()
    
    if method == 'attention':
        # Attention-based explanation
        logger.info("Computing attention-based explanations...")
        
        extractor = AttentionExtractor(model)
        analyzer = AttentionAnalyzer(extractor)
        
        result = analyzer.analyze_commit(diff_text, temporal_features, device)
        
        # Print summary
        summary = visualizer.create_summary_report(
            result['prediction'],
            result['top_tokens'],
            method='attention'
        )
        print(summary)
        
        # Show importance bars
        bars = visualizer.create_importance_bars(
            result['tokens'],
            result['token_scores'],
            top_k=15
        )
        print(bars)
        
        # Show token heatmap
        print(f"\n{'='*70}")
        print("Token Importance Heatmap (colored by importance)")
        print(f"{'='*70}")
        heatmap = visualizer.create_heatmap_text(
            result['tokens'],
            result['token_scores'],
            width=70
        )
        print(heatmap)
        
        # Save HTML report
        html_path = f"artifacts/explanations/commit_{index}_attention.html"
        visualizer.save_html_report(
            result['prediction'],
            result['tokens'],
            result['token_scores'],
            diff_text,
            html_path
        )
        logger.info(f"\nHTML report saved to: {html_path}")
        
    elif method == 'gradients':
        # Gradient-based explanation
        logger.info("Computing gradient-based explanations...")
        
        attributor = GradientAttributor(model)
        
        result = attributor.explain_prediction(
            diff_text,
            temporal_features,
            device,
            method='integrated_gradients'
        )
        
        # Print summary
        summary = visualizer.create_summary_report(
            result['prediction'],
            result['top_attributions'],
            method='integrated_gradients'
        )
        print(summary)
        
        # Show importance bars
        bars = visualizer.create_importance_bars(
            result['tokens'],
            result['attributions'],
            top_k=15
        )
        print(bars)
        
        # Show token heatmap
        print(f"\n{'='*70}")
        print("Token Attribution Heatmap (colored by gradient importance)")
        print(f"{'='*70}")
        heatmap = visualizer.create_heatmap_text(
            result['tokens'],
            result['attributions'],
            width=70
        )
        print(heatmap)
        
        # Save HTML report
        html_path = f"artifacts/explanations/commit_{index}_gradients.html"
        visualizer.save_html_report(
            result['prediction'],
            result['tokens'],
            result['attributions'],
            diff_text,
            html_path
        )
        logger.info(f"\nHTML report saved to: {html_path}")
    
    else:
        raise ValueError(f"Unknown method: {method}")
    
    print("\n" + "="*70)
    print("Explanation Complete!")
    print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Explain commit severity predictions")
    parser.add_argument(
        '--index',
        type=int,
        default=0,
        help='Index of commit in test set to explain'
    )
    parser.add_argument(
        '--method',
        type=str,
        default='attention',
        choices=['attention', 'gradients'],
        help='Explanation method to use'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        default='data/processed/test.csv',
        help='Path to dataset CSV'
    )
    parser.add_argument(
        '--checkpoint',
        type=str,
        default='artifacts/models/baseline_best.pt',
        help='Path to model checkpoint'
    )
    
    args = parser.parse_args()
    
    # Setup
    DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logger.info(f"Device: {DEVICE}")
    
    # Check files exist
    if not Path(args.checkpoint).exists():
        logger.error(f"Checkpoint not found: {args.checkpoint}")
        logger.error("Train the model first: python scripts/06_train_baseline.py")
        return
    
    if not Path(args.dataset).exists():
        logger.error(f"Dataset not found: {args.dataset}")
        return
    
    # Load model
    logger.info("Loading model...")
    model = load_model(args.checkpoint, DEVICE)
    
    # Explain commit
    explain_commit_by_index(
        args.index,
        args.dataset,
        model,
        DEVICE,
        args.method
    )


if __name__ == "__main__":
    main()