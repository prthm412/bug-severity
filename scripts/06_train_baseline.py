"""
Script 6: Train baseline BugSage model
"""
import os
import sys
from pathlib import Path
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torch.optim import AdamW
from transformers import get_linear_schedule_with_warmup
from tqdm import tqdm
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, classification_report
import logging
import json

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bugsage.data.dataset import CommitDataset
from bugsage.models.bugsage import BugSageBaseline
from bugsage.models.loss import FocalLoss
from bugsage.utils.logging import setup_logging

logger = setup_logging("train_baseline")


def evaluate(model, dataloader, criterion, device):
    """Evaluate model on dataloader"""
    model.eval()
    
    total_loss = 0
    all_preds = []
    all_labels = []
    
    with torch.no_grad():
        for batch in tqdm(dataloader, desc="Evaluating"):
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            temporal_features = batch['temporal_features'].to(device)
            labels = batch['labels'].to(device)
            
            logits = model(input_ids, attention_mask, temporal_features)
            loss = criterion(logits, labels)
            
            total_loss += loss.item()
            
            preds = torch.argmax(logits, dim=1)
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())
    
    avg_loss = total_loss / len(dataloader)
    accuracy = accuracy_score(all_labels, all_preds)
    macro_f1 = f1_score(all_labels, all_preds, average='macro')
    
    return {
        'loss': avg_loss,
        'accuracy': accuracy,
        'macro_f1': macro_f1,
        'predictions': all_preds,
        'labels': all_labels
    }


def train_epoch(model, dataloader, criterion, optimizer, scheduler, device, epoch):
    """Train for one epoch"""
    model.train()
    
    total_loss = 0
    all_preds = []
    all_labels = []
    
    progress_bar = tqdm(dataloader, desc=f"Epoch {epoch}")
    
    for batch_idx, batch in enumerate(progress_bar):
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        temporal_features = batch['temporal_features'].to(device)
        labels = batch['labels'].to(device)
        
        optimizer.zero_grad()
        
        logits = model(input_ids, attention_mask, temporal_features)
        loss = criterion(logits, labels)
        
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        scheduler.step()
        
        total_loss += loss.item()
        
        preds = torch.argmax(logits, dim=1)
        all_preds.extend(preds.cpu().numpy())
        all_labels.extend(labels.cpu().numpy())
        
        # Update progress bar
        if batch_idx % 10 == 0:
            progress_bar.set_postfix({'loss': f'{loss.item():.4f}'})
    
    avg_loss = total_loss / len(dataloader)
    accuracy = accuracy_score(all_labels, all_preds)
    macro_f1 = f1_score(all_labels, all_preds, average='macro')
    
    return {
        'loss': avg_loss,
        'accuracy': accuracy,
        'macro_f1': macro_f1
    }


def main():
    print("=" * 70)
    print("BugSage Baseline Model Training")
    print("=" * 70)
    print()
    
    # Configuration
    BATCH_SIZE = 8
    EPOCHS = 5
    LEARNING_RATE = 2e-5
    DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    logger.info(f"Device: {DEVICE}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Epochs: {EPOCHS}")
    logger.info(f"Learning rate: {LEARNING_RATE}")
    
    # Load datasets
    logger.info("\nLoading datasets...")
    train_dataset = CommitDataset("data/processed/train.csv")
    val_dataset = CommitDataset("data/processed/val.csv")
    
    # Create dataloaders
    train_loader = DataLoader(
        train_dataset,
        batch_size=BATCH_SIZE,
        shuffle=True,
        num_workers=0
    )
    
    val_loader = DataLoader(
        val_dataset,
        batch_size=BATCH_SIZE,
        shuffle=False,
        num_workers=0
    )
    
    # Get class weights
    class_weights = train_dataset.get_label_weights().to(DEVICE)
    logger.info(f"\nClass weights: {class_weights}")
    
    # Create model
    logger.info("\nInitializing model...")
    model = BugSageBaseline(
        encoder_name="microsoft/codebert-base",
        temporal_dim=3,
        hidden_dims=[256, 128],
        num_classes=3,
        dropout=0.3
    )
    model = model.to(DEVICE)
    
    # Count parameters
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info(f"Total parameters: {total_params:,}")
    logger.info(f"Trainable parameters: {trainable_params:,}")
    
    # Loss and optimizer
    criterion = FocalLoss(alpha=class_weights, gamma=2.0)
    optimizer = AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=0.01)
    
    # Learning rate scheduler
    total_steps = len(train_loader) * EPOCHS
    warmup_steps = int(0.1 * total_steps)
    scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=warmup_steps,
        num_training_steps=total_steps
    )
    
    logger.info(f"Total training steps: {total_steps}")
    logger.info(f"Warmup steps: {warmup_steps}")
    
    # Training loop
    best_val_f1 = 0
    history = []
    
    logger.info("\nStarting training...")
    print()
    
    for epoch in range(1, EPOCHS + 1):
        print(f"\nEpoch {epoch}/{EPOCHS}")
        print("-" * 70)
        
        # Train
        train_metrics = train_epoch(
            model, train_loader, criterion, optimizer, scheduler, DEVICE, epoch
        )
        
        logger.info(f"Train - Loss: {train_metrics['loss']:.4f}, "
                   f"Acc: {train_metrics['accuracy']:.4f}, "
                   f"F1: {train_metrics['macro_f1']:.4f}")
        
        # Validate
        val_metrics = evaluate(model, val_loader, criterion, DEVICE)
        
        logger.info(f"Val   - Loss: {val_metrics['loss']:.4f}, "
                   f"Acc: {val_metrics['accuracy']:.4f}, "
                   f"F1: {val_metrics['macro_f1']:.4f}")
        
        # Save history
        history.append({
            'epoch': epoch,
            'train_loss': train_metrics['loss'],
            'train_acc': train_metrics['accuracy'],
            'train_f1': train_metrics['macro_f1'],
            'val_loss': val_metrics['loss'],
            'val_acc': val_metrics['accuracy'],
            'val_f1': val_metrics['macro_f1']
        })
        
        # Save best model
        if val_metrics['macro_f1'] > best_val_f1:
            best_val_f1 = val_metrics['macro_f1']
            
            checkpoint_path = Path("artifacts/models/baseline_best.pt")
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            
            torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'val_f1': best_val_f1,
                'val_metrics': val_metrics
            }, checkpoint_path)
            
            logger.info(f"Saved best model (F1: {best_val_f1:.4f})")
    
    print()
    print("=" * 70)
    print("Training Complete")
    print("=" * 70)
    print(f"\nBest validation F1: {best_val_f1:.4f}")
    
    # Save history
    history_df = pd.DataFrame(history)
    history_df.to_csv("artifacts/models/training_history.csv", index=False)
    logger.info("Training history saved")


if __name__ == "__main__":
    main()