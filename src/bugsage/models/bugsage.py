"""
BugSage baseline model architecture
"""
import torch
import torch.nn as nn
from transformers import AutoModel


class BugSageBaseline(nn.Module):
    """
    Baseline BugSage model
    
    Architecture:
    - CodeBERT encoder for diff text
    - MLP for temporal features
    - Fusion layer
    - Classification head
    """
    
    def __init__(
        self,
        encoder_name="microsoft/codebert-base",
        temporal_dim=3,
        hidden_dims=[256, 128],
        num_classes=3,
        dropout=0.3
    ):
        super().__init__()
        
        # CodeBERT encoder
        self.encoder = AutoModel.from_pretrained(encoder_name)
        encoder_dim = self.encoder.config.hidden_size  # 768 for CodeBERT
        
        # Freeze encoder layers (optional - for faster training)
        # Uncomment to freeze:
        # for param in self.encoder.parameters():
        #     param.requires_grad = False
        
        # Temporal feature MLP
        self.temporal_mlp = nn.Sequential(
            nn.Linear(temporal_dim, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 64),
            nn.ReLU()
        )
        
        # Fusion layer (combine encoder + temporal)
        fusion_input_dim = encoder_dim + 64
        
        # Build classifier
        layers = []
        prev_dim = fusion_input_dim
        
        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.ReLU(),
                nn.Dropout(dropout)
            ])
            prev_dim = hidden_dim
        
        # Output layer
        layers.append(nn.Linear(prev_dim, num_classes))
        
        self.classifier = nn.Sequential(*layers)
    
    def forward(self, input_ids, attention_mask, temporal_features):
        """
        Forward pass
        
        Args:
            input_ids: [batch_size, seq_len]
            attention_mask: [batch_size, seq_len]
            temporal_features: [batch_size, 3]
        
        Returns:
            logits: [batch_size, num_classes]
        """
        # Encode diff text
        encoder_output = self.encoder(
            input_ids=input_ids,
            attention_mask=attention_mask
        )
        
        # Use [CLS] token representation
        code_embedding = encoder_output.last_hidden_state[:, 0, :]  # [batch_size, 768]
        
        # Process temporal features
        temporal_embedding = self.temporal_mlp(temporal_features)  # [batch_size, 64]
        
        # Concatenate
        combined = torch.cat([code_embedding, temporal_embedding], dim=1)  # [batch_size, 832]
        
        # Classify
        logits = self.classifier(combined)  # [batch_size, num_classes]
        
        return logits