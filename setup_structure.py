"""
Create BugSage+ project structure
"""
import os
from pathlib import Path

def create_structure():
    dirs = [
        "docs",
        "config/datasets",
        "config/experiments",
        "data/raw",
        "data/interim",
        "data/processed",
        "data/external",
        "src/bugsage/utils",
        "src/bugsage/config",
        "src/bugsage/data",
        "src/bugsage/features",
        "src/bugsage/models",
        "src/bugsage/training",
        "src/bugsage/explain",
        "src/bugsage/localization",
        "src/bugsage/evaluation",
        "src/bugsage/ci",
        "scripts/utils",
        "notebooks",
        "tests/unit",
        "tests/integration",
        "tests/fixtures",
        ".github/workflows",
        "docker/requirements",
        "artifacts/logs",
        "artifacts/models",
        "artifacts/results",
        "artifacts/explanations",
        "artifacts/cache",
    ]
    
    print("Creating BugSage+ structure...\n")
    
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        # Create __init__.py for Python packages
        if dir_path.startswith("src/") or dir_path.startswith("tests/"):
            init_file = Path(dir_path) / "__init__.py"
            if not init_file.exists():
                init_file.write_text("# BugSage+ module\n")
        
        print(f"✅ {dir_path}")
    
    print(f"\nCreated {len(dirs)} directories!")

if __name__ == "__main__":
    create_structure()