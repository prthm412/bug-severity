"""
Attention-based explainability for BugSage
Extracts and analyzes attention weights from CodeBERT
"""
import torch
import torch.nn.functional as F
import numpy as np
from typing import Dict, List, Tuple, Optional
from transformers import RobertaTokenizer


class AttentionExtractor:
    """Extract and process attention weights from the model"""
    
    def __init__(self, model, tokenizer_name: str = "microsoft/codebert-base"):
        """
        Args:
            model: Trained BugSageBaseline model
            tokenizer_name: Name of the tokenizer to use
        """
        self.model = model
        self.tokenizer = RobertaTokenizer.from_pretrained(tokenizer_name)
        self.model.eval()
    
    def extract_attention(
        self, 
        input_ids: torch.Tensor, 
        attention_mask: torch.Tensor
    ) -> Dict[str, torch.Tensor]:
        """
        Extract attention weights from all layers
        
        Args:
            input_ids: Token IDs [batch_size, seq_len]
            attention_mask: Attention mask [batch_size, seq_len]
            
        Returns:
            Dictionary containing attention weights from all layers
        """
        with torch.no_grad():
            # Get encoder outputs with attention weights
            outputs = self.model.encoder(
                input_ids=input_ids,
                attention_mask=attention_mask,
                output_attentions=True
            )
            
            # outputs.attentions is a tuple of (num_layers,)
            # Each element is [batch_size, num_heads, seq_len, seq_len]
            attentions = outputs.attentions
            
            return {
                'attentions': attentions,
                'last_hidden_state': outputs.last_hidden_state
            }
    
    def aggregate_attention(
        self, 
        attentions: Tuple[torch.Tensor], 
        method: str = 'mean'
    ) -> torch.Tensor:
        """
        Aggregate attention across layers and heads
        
        Args:
            attentions: Tuple of attention tensors from each layer
            method: Aggregation method ('mean', 'max', 'last')
            
        Returns:
            Aggregated attention weights [batch_size, seq_len, seq_len]
        """
        # Stack all layers: [num_layers, batch_size, num_heads, seq_len, seq_len]
        stacked = torch.stack(attentions)
        
        if method == 'mean':
            # Average across layers and heads
            aggregated = stacked.mean(dim=0).mean(dim=1)
        elif method == 'max':
            # Max across layers and heads
            aggregated = stacked.max(dim=0)[0].max(dim=1)[0]
        elif method == 'last':
            # Use only last layer, average heads
            aggregated = stacked[-1].mean(dim=1)
        else:
            raise ValueError(f"Unknown aggregation method: {method}")
        
        return aggregated
    
    def get_token_importance(
        self, 
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        method: str = 'mean'
    ) -> Tuple[List[str], np.ndarray]:
        """
        Get importance scores for each token
        
        Args:
            input_ids: Token IDs [1, seq_len]
            attention_mask: Attention mask [1, seq_len]
            method: Aggregation method
            
        Returns:
            tokens: List of tokens
            scores: Importance score for each token
        """
        # Extract attention
        attention_data = self.extract_attention(input_ids, attention_mask)
        attentions = attention_data['attentions']
        
        # Aggregate attention: [1, seq_len, seq_len]
        agg_attention = self.aggregate_attention(attentions, method)
        
        # Get attention TO each token (sum across source positions)
        # This tells us how much each token is "attended to"
        token_scores = agg_attention[0].sum(dim=0).cpu().numpy()
        
        # Normalize scores
        token_scores = token_scores / token_scores.sum()
        
        # Decode tokens
        tokens = self.tokenizer.convert_ids_to_tokens(input_ids[0].cpu().numpy())
        
        # Filter out padding tokens
        valid_mask = attention_mask[0].cpu().numpy().astype(bool)
        tokens = [t for t, m in zip(tokens, valid_mask) if m]
        token_scores = token_scores[valid_mask]
        
        return tokens, token_scores
    
    def get_cls_attention(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor
    ) -> Tuple[List[str], np.ndarray]:
        """
        Get attention from [CLS] token to all other tokens
        This shows what the classifier focuses on
        
        Args:
            input_ids: Token IDs [1, seq_len]
            attention_mask: Attention mask [1, seq_len]
            
        Returns:
            tokens: List of tokens
            scores: Attention from CLS to each token
        """
        # Extract attention
        attention_data = self.extract_attention(input_ids, attention_mask)
        attentions = attention_data['attentions']
        
        # Use last layer, average across heads
        last_layer_attention = attentions[-1].mean(dim=1)  # [1, seq_len, seq_len]
        
        # Get attention FROM [CLS] (position 0) TO all tokens
        cls_attention = last_layer_attention[0, 0, :].cpu().numpy()
        
        # Decode tokens
        tokens = self.tokenizer.convert_ids_to_tokens(input_ids[0].cpu().numpy())
        
        # Filter padding
        valid_mask = attention_mask[0].cpu().numpy().astype(bool)
        tokens = [t for t, m in zip(tokens, valid_mask) if m]
        cls_attention = cls_attention[valid_mask]
        
        # Normalize
        cls_attention = cls_attention / cls_attention.sum()
        
        return tokens, cls_attention
    
    def visualize_attention(
        self,
        tokens: List[str],
        scores: np.ndarray,
        top_k: int = 20
    ) -> str:
        """
        Create a text visualization of token importance
        
        Args:
            tokens: List of tokens
            scores: Importance scores
            top_k: Number of top tokens to show
            
        Returns:
            Formatted string visualization
        """
        # Get top-k tokens
        top_indices = np.argsort(scores)[-top_k:][::-1]
        
        lines = ["Top Important Tokens:", "=" * 50]
        
        for i, idx in enumerate(top_indices, 1):
            token = tokens[idx]
            score = scores[idx]
            bar_length = int(score * 100)
            bar = "█" * bar_length
            lines.append(f"{i:2d}. {token:20s} {score:.4f} {bar}")
        
        return "\n".join(lines)


class AttentionAnalyzer:
    """Higher-level attention analysis"""
    
    def __init__(self, extractor: AttentionExtractor):
        self.extractor = extractor
    
    def analyze_commit(
        self,
        diff_text: str,
        temporal_features: torch.Tensor,
        device: str = 'cpu'
    ) -> Dict:
        """
        Analyze a commit and return attention-based explanations
        
        Args:
            diff_text: The git diff text
            temporal_features: Temporal feature vector [3]
            device: Device to run on
            
        Returns:
            Dictionary with analysis results
        """
        # Tokenize
        encoding = self.extractor.tokenizer(
            diff_text,
            max_length=512,
            padding='max_length',
            truncation=True,
            return_tensors='pt'
        )
        
        input_ids = encoding['input_ids'].to(device)
        attention_mask = encoding['attention_mask'].to(device)
        temporal_features = temporal_features.unsqueeze(0).to(device)
        
        # Get prediction
        with torch.no_grad():
            logits = self.extractor.model(input_ids, attention_mask, temporal_features)
            probs = F.softmax(logits, dim=1)
            pred_class = torch.argmax(logits, dim=1).item()
            pred_prob = probs[0, pred_class].item()
        
        # Get token importance
        tokens, token_scores = self.extractor.get_token_importance(
            input_ids, attention_mask
        )
        
        # Get CLS attention
        _, cls_attention = self.extractor.get_cls_attention(
            input_ids, attention_mask
        )
        
        # Identify changed lines (tokens with +/-)
        changed_token_indices = []
        for i, token in enumerate(tokens):
            if token in ['+', '-', 'Ġ+', 'Ġ-'] or token.startswith(('Ġ+', 'Ġ-')):
                changed_token_indices.append(i)
        
        # Get scores for changed lines
        changed_line_scores = [token_scores[i] for i in changed_token_indices]
        avg_changed_score = np.mean(changed_line_scores) if changed_line_scores else 0
        
        severity_names = ['low', 'medium', 'high']
        
        return {
            'prediction': {
                'class': pred_class,
                'severity': severity_names[pred_class],
                'confidence': pred_prob,
                'probabilities': {
                    'low': probs[0, 0].item(),
                    'medium': probs[0, 1].item(),
                    'high': probs[0, 2].item()
                }
            },
            'tokens': tokens,
            'token_scores': token_scores,
            'cls_attention': cls_attention,
            'changed_lines': {
                'num_changed_tokens': len(changed_token_indices),
                'avg_attention': avg_changed_score,
                'indices': changed_token_indices
            },
            'top_tokens': self._get_top_tokens(tokens, token_scores, k=10)
        }
    
    def _get_top_tokens(
        self, 
        tokens: List[str], 
        scores: np.ndarray, 
        k: int = 10
    ) -> List[Dict]:
        """Get top-k important tokens"""
        top_indices = np.argsort(scores)[-k:][::-1]
        return [
            {
                'token': tokens[i],
                'score': float(scores[i]),
                'rank': rank + 1
            }
            for rank, i in enumerate(top_indices)
        ]