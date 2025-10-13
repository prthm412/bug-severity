"""
Loss functions for BugSage
"""
import torch
import torch.nn as nn
import torch.nn.functional as F


class FocalLoss(nn.Module):
    """
    Focal Loss for handling class imbalance
    
    Focuses training on hard examples
    Reference: https://arxiv.org/abs/1708.02002
    """
    
    def __init__(self, alpha=None, gamma=2.0, reduction='mean'):
        """
        Args:
            alpha: Class weights [num_classes]
            gamma: Focusing parameter (higher = more focus on hard examples)
            reduction: 'mean' or 'sum'
        """
        super().__init__()
        self.alpha = alpha
        self.gamma = gamma
        self.reduction = reduction
    
    def forward(self, logits, targets):
        """
        Args:
            logits: [batch_size, num_classes]
            targets: [batch_size]
        
        Returns:
            loss: scalar
        """
        # Get probabilities
        probs = F.softmax(logits, dim=1)
        
        # Get probability of correct class
        pt = probs.gather(1, targets.unsqueeze(1)).squeeze(1)
        
        # Compute focal term: (1 - pt)^gamma
        focal_weight = (1 - pt) ** self.gamma
        
        # Compute cross-entropy
        ce_loss = F.cross_entropy(logits, targets, reduction='none')
        
        # Apply focal weight
        focal_loss = focal_weight * ce_loss
        
        # Apply class weights if provided
        if self.alpha is not None:
            alpha_t = self.alpha[targets]
            focal_loss = alpha_t * focal_loss
        
        # Reduction
        if self.reduction == 'mean':
            return focal_loss.mean()
        elif self.reduction == 'sum':
            return focal_loss.sum()
        else:
            return focal_loss