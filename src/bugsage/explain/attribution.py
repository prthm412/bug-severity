"""
Gradient-based attribution methods for BugSage
Uses integrated gradients and saliency maps
"""
import torch
import torch.nn.functional as F
import numpy as np
from typing import Dict, List, Tuple, Optional
from transformers import RobertaTokenizer


class GradientAttributor:
    """Gradient-based attribution for model predictions"""
    
    def __init__(self, model, tokenizer_name: str = "microsoft/codebert-base"):
        """
        Args:
            model: Trained BugSageBaseline model
            tokenizer_name: Name of the tokenizer
        """
        self.model = model
        self.tokenizer = RobertaTokenizer.from_pretrained(tokenizer_name)
        self.model.eval()
    
    def compute_saliency(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        temporal_features: torch.Tensor,
        target_class: Optional[int] = None
    ) -> Tuple[List[str], np.ndarray]:
        """
        Compute saliency map (gradient of output w.r.t. input embeddings)
        
        Args:
            input_ids: Token IDs [1, seq_len]
            attention_mask: Attention mask [1, seq_len]
            temporal_features: Temporal features [1, 3]
            target_class: Target class to explain (None = predicted class)
            
        Returns:
            tokens: List of tokens
            saliency: Saliency score for each token
        """
        # Enable gradients
        self.model.zero_grad()
        
        # Get embeddings with gradient tracking
        embeddings = self.model.encoder.embeddings.word_embeddings(input_ids)
        embeddings.requires_grad_(True)
        
        # Forward pass with custom embeddings
        encoder_outputs = self.model.encoder(
            inputs_embeds=embeddings,
            attention_mask=attention_mask,
            return_dict=True
        )
        
        pooled = encoder_outputs.last_hidden_state[:, 0, :]  # [CLS] token
        combined = torch.cat([pooled, temporal_features], dim=1)
        logits = self.model.classifier(combined)
        
        # Get target class
        if target_class is None:
            target_class = torch.argmax(logits, dim=1).item()
        
        # Compute gradient
        target_logit = logits[0, target_class]
        target_logit.backward()
        
        # Get gradient magnitude
        saliency = embeddings.grad.abs().sum(dim=2).squeeze(0).cpu().numpy()
        
        # Normalize
        saliency = saliency / (saliency.max() + 1e-10)
        
        # Decode tokens
        tokens = self.tokenizer.convert_ids_to_tokens(input_ids[0].cpu().numpy())
        
        # Filter padding
        valid_mask = attention_mask[0].cpu().numpy().astype(bool)
        tokens = [t for t, m in zip(tokens, valid_mask) if m]
        saliency = saliency[valid_mask]
        
        return tokens, saliency
    
    def compute_integrated_gradients(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        temporal_features: torch.Tensor,
        target_class: Optional[int] = None,
        steps: int = 50
    ) -> Tuple[List[str], np.ndarray]:
        """
        Compute integrated gradients for more accurate attribution
        
        Args:
            input_ids: Token IDs [1, seq_len]
            attention_mask: Attention mask [1, seq_len]
            temporal_features: Temporal features [1, 3]
            target_class: Target class to explain
            steps: Number of interpolation steps
            
        Returns:
            tokens: List of tokens
            attributions: Attribution score for each token
        """
        # Get original embeddings
        with torch.no_grad():
            original_embeds = self.model.encoder.embeddings.word_embeddings(input_ids)
        
        # Baseline is zero embeddings
        baseline_embeds = torch.zeros_like(original_embeds)
        
        # Get target class
        if target_class is None:
            with torch.no_grad():
                logits = self.model(input_ids, attention_mask, temporal_features)
                target_class = torch.argmax(logits, dim=1).item()
        
        # Accumulate gradients
        accumulated_grads = torch.zeros_like(original_embeds)
        
        for step in range(steps):
            # Interpolate between baseline and original
            alpha = (step + 1) / steps
            interpolated_embeds = baseline_embeds + alpha * (original_embeds - baseline_embeds)
            interpolated_embeds.requires_grad_(True)
            
            # Forward pass
            self.model.zero_grad()
            encoder_outputs = self.model.encoder(
                inputs_embeds=interpolated_embeds,
                attention_mask=attention_mask,
                return_dict=True
            )
            
            pooled = encoder_outputs.last_hidden_state[:, 0, :]
            combined = torch.cat([pooled, temporal_features], dim=1)
            logits = self.model.classifier(combined)
            
            # Backward
            target_logit = logits[0, target_class]
            target_logit.backward()
            
            # Accumulate gradients
            accumulated_grads += interpolated_embeds.grad
        
        # Average gradients
        avg_grads = accumulated_grads / steps
        
        # Multiply by difference from baseline
        integrated_grads = (original_embeds - baseline_embeds) * avg_grads
        
        # Sum across embedding dimension
        attributions = integrated_grads.abs().sum(dim=2).squeeze(0).cpu().numpy()
        
        # Normalize
        attributions = attributions / (attributions.max() + 1e-10)
        
        # Decode tokens
        tokens = self.tokenizer.convert_ids_to_tokens(input_ids[0].cpu().numpy())
        
        # Filter padding
        valid_mask = attention_mask[0].cpu().numpy().astype(bool)
        tokens = [t for t, m in zip(tokens, valid_mask) if m]
        attributions = attributions[valid_mask]
        
        return tokens, attributions
    
    def explain_prediction(
        self,
        diff_text: str,
        temporal_features: torch.Tensor,
        device: str = 'cpu',
        method: str = 'integrated_gradients'
    ) -> Dict:
        """
        Full explanation of a prediction
        
        Args:
            diff_text: Git diff text
            temporal_features: Temporal features [3]
            device: Device to use
            method: 'saliency' or 'integrated_gradients'
            
        Returns:
            Explanation dictionary
        """
        # Tokenize
        encoding = self.tokenizer(
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
            logits = self.model(input_ids, attention_mask, temporal_features)
            probs = F.softmax(logits, dim=1)
            pred_class = torch.argmax(logits, dim=1).item()
        
        # Compute attributions
        if method == 'saliency':
            tokens, scores = self.compute_saliency(
                input_ids, attention_mask, temporal_features, pred_class
            )
        elif method == 'integrated_gradients':
            tokens, scores = self.compute_integrated_gradients(
                input_ids, attention_mask, temporal_features, pred_class
            )
        else:
            raise ValueError(f"Unknown method: {method}")
        
        severity_names = ['low', 'medium', 'high']
        
        return {
            'prediction': {
                'class': pred_class,
                'severity': severity_names[pred_class],
                'confidence': probs[0, pred_class].item(),
                'probabilities': {
                    'low': probs[0, 0].item(),
                    'medium': probs[0, 1].item(),
                    'high': probs[0, 2].item()
                }
            },
            'tokens': tokens,
            'attributions': scores,
            'method': method,
            'top_attributions': self._get_top_tokens(tokens, scores, k=15)
        }
    
    def _get_top_tokens(
        self,
        tokens: List[str],
        scores: np.ndarray,
        k: int = 10
    ) -> List[Dict]:
        """Get top-k tokens by attribution score"""
        top_indices = np.argsort(scores)[-k:][::-1]
        return [
            {
                'token': tokens[i],
                'score': float(scores[i]),
                'rank': rank + 1
            }
            for rank, i in enumerate(top_indices)
        ]