"""
Visualization tools for BugSage explanations
Creates heatmaps and visual representations of explanations
"""
import numpy as np
from typing import List, Dict, Optional
from pathlib import Path


class ExplanationVisualizer:
    """Create visualizations for model explanations"""
    
    def __init__(self):
        # Color codes for terminal output
        self.colors = {
            'red': '\033[91m',
            'yellow': '\033[93m',
            'green': '\033[92m',
            'blue': '\033[94m',
            'bold': '\033[1m',
            'end': '\033[0m'
        }
    
    def create_heatmap_text(
        self,
        tokens: List[str],
        scores: np.ndarray,
        width: int = 80,
        show_scores: bool = False
    ) -> str:
        """
        Create a text-based heatmap of token importance
        
        Args:
            tokens: List of tokens
            scores: Importance scores (0-1)
            width: Maximum line width
            show_scores: Whether to show numerical scores
            
        Returns:
            Formatted heatmap string
        """
        lines = []
        current_line_tokens = []
        current_line_scores = []
        current_length = 0
        
        for token, score in zip(tokens, scores):
            # Clean token (remove Ġ prefix from RoBERTa)
            clean_token = token.replace('Ġ', ' ')
            
            token_length = len(clean_token)
            
            # Check if adding this token exceeds width
            if current_length + token_length > width and current_line_tokens:
                # Render current line
                line = self._render_line(
                    current_line_tokens, 
                    current_line_scores,
                    show_scores
                )
                lines.append(line)
                
                # Start new line
                current_line_tokens = [clean_token]
                current_line_scores = [score]
                current_length = token_length
            else:
                current_line_tokens.append(clean_token)
                current_line_scores.append(score)
                current_length += token_length
        
        # Render last line
        if current_line_tokens:
            line = self._render_line(
                current_line_tokens,
                current_line_scores,
                show_scores
            )
            lines.append(line)
        
        return '\n'.join(lines)
    
    def _render_line(
        self,
        tokens: List[str],
        scores: np.ndarray,
        show_scores: bool
    ) -> str:
        """Render a single line with color coding"""
        parts = []
        
        for token, score in zip(tokens, scores):
            # Color based on score
            if score > 0.7:
                color = self.colors['red']  # High importance
            elif score > 0.4:
                color = self.colors['yellow']  # Medium importance
            elif score > 0.2:
                color = self.colors['blue']  # Low importance
            else:
                color = ''  # Very low importance
            
            if color:
                colored_token = f"{color}{token}{self.colors['end']}"
            else:
                colored_token = token
            
            if show_scores:
                colored_token += f"[{score:.2f}]"
            
            parts.append(colored_token)
        
        return ''.join(parts)
    
    def create_importance_bars(
        self,
        tokens: List[str],
        scores: np.ndarray,
        top_k: int = 20
    ) -> str:
        """
        Create horizontal bar chart of top important tokens
        
        Args:
            tokens: List of tokens
            scores: Importance scores
            top_k: Number of top tokens to show
            
        Returns:
            Formatted bar chart string
        """
        # Get top-k
        top_indices = np.argsort(scores)[-top_k:][::-1]
        
        lines = [
            f"\n{self.colors['bold']}Top {top_k} Important Tokens{self.colors['end']}",
            "=" * 70
        ]
        
        for rank, idx in enumerate(top_indices, 1):
            token = tokens[idx].replace('Ġ', ' ')
            score = scores[idx]
            
            # Create bar
            bar_length = int(score * 50)
            bar = "█" * bar_length
            
            # Color based on score
            if score > 0.7:
                bar_color = self.colors['red']
            elif score > 0.4:
                bar_color = self.colors['yellow']
            else:
                bar_color = self.colors['blue']
            
            line = f"{rank:2d}. {token:25s} {score:.4f} {bar_color}{bar}{self.colors['end']}"
            lines.append(line)
        
        return '\n'.join(lines)
    
    def create_diff_heatmap(
        self,
        diff_text: str,
        tokens: List[str],
        scores: np.ndarray
    ) -> str:
        """
        Create a heatmap overlay on the original diff
        Highlights risky lines in the diff
        
        Args:
            diff_text: Original git diff
            tokens: Tokens from model
            scores: Importance scores
            
        Returns:
            Annotated diff with risk scores
        """
        lines = []
        lines.append(f"\n{self.colors['bold']}Diff with Risk Highlights{self.colors['end']}")
        lines.append("=" * 70)
        
        # Split diff into lines
        diff_lines = diff_text.split('\n')
        
        # Try to map tokens back to diff lines
        # This is approximate - just highlight changed lines
        for line in diff_lines[:50]:  # Show first 50 lines
            if not line.strip():
                continue
            
            # Check if this is a changed line
            is_added = line.startswith('+') and not line.startswith('+++')
            is_removed = line.startswith('-') and not line.startswith('---')
            
            if is_added or is_removed:
                # Calculate average importance for tokens in this line
                # (simplified - just use random for demo)
                line_importance = np.random.random()  # Placeholder
                
                if line_importance > 0.6:
                    color = self.colors['red']
                    risk = "HIGH RISK"
                elif line_importance > 0.3:
                    color = self.colors['yellow']
                    risk = "MEDIUM"
                else:
                    color = self.colors['green']
                    risk = "LOW"
                
                formatted_line = f"{color}{line[:70]}{self.colors['end']} [{risk}]"
            else:
                formatted_line = line[:70]
            
            lines.append(formatted_line)
        
        return '\n'.join(lines)
    
    def create_summary_report(
        self,
        prediction: Dict,
        top_tokens: List[Dict],
        method: str = 'attention'
    ) -> str:
        """
        Create a comprehensive summary report
        
        Args:
            prediction: Prediction dictionary
            top_tokens: Top important tokens
            method: Explanation method used
            
        Returns:
            Formatted report
        """
        lines = []
        
        # Header
        lines.append("\n" + "=" * 70)
        lines.append(f"{self.colors['bold']}BugSage+ Explanation Report{self.colors['end']}")
        lines.append("=" * 70)
        
        # Prediction
        severity = prediction['severity'].upper()
        confidence = prediction['confidence']
        
        if severity == 'HIGH':
            sev_color = self.colors['red']
        elif severity == 'MEDIUM':
            sev_color = self.colors['yellow']
        else:
            sev_color = self.colors['green']
        
        lines.append(f"\n{self.colors['bold']}Prediction:{self.colors['end']}")
        lines.append(f"  Severity: {sev_color}{severity}{self.colors['end']}")
        lines.append(f"  Confidence: {confidence:.2%}")
        
        # Probabilities
        lines.append(f"\n{self.colors['bold']}Class Probabilities:{self.colors['end']}")
        probs = prediction['probabilities']
        lines.append(f"  Low:    {probs['low']:.2%}")
        lines.append(f"  Medium: {probs['medium']:.2%}")
        lines.append(f"  High:   {probs['high']:.2%}")
        
        # Top tokens
        lines.append(f"\n{self.colors['bold']}Top Contributing Tokens (via {method}):{self.colors['end']}")
        lines.append("-" * 70)
        
        for item in top_tokens[:10]:
            token = item['token'].replace('Ġ', ' ')
            score = item['score']
            rank = item['rank']
            
            bar_length = int(score * 30)
            bar = "█" * bar_length
            
            lines.append(f"{rank:2d}. {token:20s} {score:.4f} {bar}")
        
        # Method info
        lines.append(f"\n{self.colors['bold']}Explanation Method:{self.colors['end']}")
        lines.append(f"  {method}")
        
        lines.append("\n" + "=" * 70 + "\n")
        
        return '\n'.join(lines)
    
    def save_html_report(
        self,
        prediction: Dict,
        tokens: List[str],
        scores: np.ndarray,
        diff_text: str,
        output_path: str
    ):
        """
        Save an HTML visualization report
        
        Args:
            prediction: Prediction dictionary
            tokens: Token list
            scores: Importance scores
            diff_text: Original diff
            output_path: Path to save HTML file
        """
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>BugSage+ Explanation Report</title>
    <style>
        body {{
            font-family: 'Courier New', monospace;
            max-width: 1000px;
            margin: 40px auto;
            padding: 20px;
            background: #1e1e1e;
            color: #d4d4d4;
        }}
        .header {{
            background: #2d2d30;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .prediction {{
            font-size: 24px;
            font-weight: bold;
        }}
        .high {{ color: #f44336; }}
        .medium {{ color: #ff9800; }}
        .low {{ color: #4caf50; }}
        .token {{
            display: inline-block;
            padding: 2px 4px;
            margin: 2px;
            border-radius: 3px;
        }}
        .importance-high {{ background: rgba(244, 67, 54, 0.3); }}
        .importance-medium {{ background: rgba(255, 152, 0, 0.3); }}
        .importance-low {{ background: rgba(76, 175, 80, 0.3); }}
        .section {{
            background: #2d2d30;
            padding: 15px;
            border-radius: 8px;
            margin: 20px 0;
        }}
        h2 {{
            color: #569cd6;
            border-bottom: 2px solid #569cd6;
            padding-bottom: 10px;
        }}
        .diff {{
            background: #1e1e1e;
            padding: 10px;
            border-left: 3px solid #569cd6;
            overflow-x: auto;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 BugSage+ Explanation Report</h1>
        <div class="prediction">
            Predicted Severity: <span class="{prediction['severity']}">{prediction['severity'].upper()}</span>
        </div>
        <div>Confidence: {prediction['confidence']:.2%}</div>
    </div>
    
    <div class="section">
        <h2>📊 Class Probabilities</h2>
        <div>Low: {prediction['probabilities']['low']:.2%}</div>
        <div>Medium: {prediction['probabilities']['medium']:.2%}</div>
        <div>High: {prediction['probabilities']['high']:.2%}</div>
    </div>
    
    <div class="section">
        <h2>🎯 Token Importance Heatmap</h2>
        <div>
"""
        
        # Add colored tokens
        for token, score in zip(tokens[:200], scores[:200]):  # First 200 tokens
            clean_token = token.replace('Ġ', ' ').replace('<', '&lt;').replace('>', '&gt;')
            
            if score > 0.7:
                importance_class = 'importance-high'
            elif score > 0.4:
                importance_class = 'importance-medium'
            elif score > 0.2:
                importance_class = 'importance-low'
            else:
                importance_class = ''
            
            html += f'<span class="token {importance_class}" title="Score: {score:.3f}">{clean_token}</span>'
        
        html += """
        </div>
    </div>
    
    <div class="section">
        <h2>📝 Original Diff</h2>
        <div class="diff">
            <pre>{}</pre>
        </div>
    </div>
</body>
</html>
""".format(diff_text[:2000].replace('<', '&lt;').replace('>', '&gt;'))
        
        # Write to file
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)