# Bug Severity Dissertation

Time-aware, explainable bug severity prediction system for my Master of Technology dissertation.

- **Offline/Training Track**: Collect historic commits/issues → build features → train ML model.
- **Online/Real-Time Track**: Webhook from GitHub → queue → features → model → predictions → dashboard.

## Quick start
```bash
source .venv/Scripts/activate   # on Git Bash
pip install -r requirements.txt