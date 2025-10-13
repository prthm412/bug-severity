"""
Script 7: Evaluate baseline model on test set
"""
import sys
from pathlib import Path
import torch
from torch.utils.data import DataLoader
import numpy as np
from sklearn.metrics import (
    accuracy_score, f1_score, classification_report,
    confusion_matrix
)
import pandas as pd
import logging

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.data.dataset import CommitDataset
from bugsage.models.bugsage import BugSageBaseline
from bugsage.models.loss import FocalLoss
from bugsage.utils.logging import setup_logging

logger = setup_logging("evaluate_baseline")


def evaluate_model(model, dataloader, device):
    """Evaluate model and return detailed metrics"""
    model.eval()
    
    all_preds = []
    all_labels = []
    all_probs = []
    
    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            temporal_features = batch['temporal_features'].to(device)
            labels = batch['labels'].to(device)
            
            logits = model(input_ids, attention_mask, temporal_features)
            probs = torch.softmax(logits, dim=1)
            preds = torch.argmax(logits, dim=1)
            
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())
            all_probs.extend(probs.cpu().numpy())
    
    return np.array(all_preds), np.array(all_labels), np.array(all_probs)


def main():
    print("=" * 70)
    print("BugSage Baseline Model Evaluation")
    print("=" * 70)
    print()
    
    DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    logger.info(f"Device: {DEVICE}")
    
    # Load test dataset
    logger.info("Loading test dataset...")
    test_dataset = CommitDataset("data/processed/test.csv")
    
    test_loader = DataLoader(
        test_dataset,
        batch_size=8,
        shuffle=False,
        num_workers=0
    )
    
    # Load model
    logger.info("Loading model...")
    model = BugSageBaseline(
        encoder_name="microsoft/codebert-base",
        temporal_dim=3,
        hidden_dims=[256, 128],
        num_classes=3,
        dropout=0.3
    )
    
    # Load checkpoint
    checkpoint_path = Path("artifacts/models/baseline_best.pt")
    
    if not checkpoint_path.exists():
        logger.error(f"Checkpoint not found: {checkpoint_path}")
        logger.error("Run training first: python scripts/06_train_baseline.py")
        return
    
    checkpoint = torch.load(checkpoint_path, map_location=DEVICE)
    model.load_state_dict(checkpoint['model_state_dict'])
    model = model.to(DEVICE)
    
    logger.info(f"Loaded checkpoint from epoch {checkpoint['epoch']}")
    logger.info(f"Validation F1: {checkpoint['val_f1']:.4f}")
    
    # Evaluate
    logger.info("\nEvaluating on test set...")
    preds, labels, probs = evaluate_model(model, test_loader, DEVICE)
    
    # Calculate metrics
    accuracy = accuracy_score(labels, preds)
    macro_f1 = f1_score(labels, preds, average='macro')
    weighted_f1 = f1_score(labels, preds, average='weighted')
    
    print()
    print("=" * 70)
    print("Test Set Results")
    print("=" * 70)
    print(f"\nAccuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"Macro F1: {macro_f1:.4f}")
    print(f"Weighted F1: {weighted_f1:.4f}")
    
    # Classification report
    label_names = ['low', 'medium', 'high']
    print("\nPer-Class Metrics:")
    print(classification_report(labels, preds, target_names=label_names, digits=4))
    
    # Confusion matrix
    cm = confusion_matrix(labels, preds)
    print("\nConfusion Matrix:")
    print("                Predicted")
    print("              Low  Medium  High")
    print(f"Actual Low    {cm[0,0]:4d}   {cm[0,1]:4d}   {cm[0,2]:4d}")
    print(f"       Medium {cm[1,0]:4d}   {cm[1,1]:4d}   {cm[1,2]:4d}")
    print(f"       High   {cm[2,0]:4d}   {cm[2,1]:4d}   {cm[2,2]:4d}")
    
    # Save results
    results = {
        'accuracy': accuracy,
        'macro_f1': macro_f1,
        'weighted_f1': weighted_f1,
        'per_class_metrics': classification_report(labels, preds, target_names=label_names, output_dict=True)
    }
    
    import json
    results_path = Path("artifacts/results/baseline_test_results.json")
    results_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"\nResults saved to: {results_path}")
    
    print()
    print("=" * 70)


if __name__ == "__main__":
    main()