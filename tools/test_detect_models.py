#!/usr/bin/env python3
"""
Test Suite for detect_models.py
================================

Tests for the model detection static analysis tool.

Run with:
    python -m pytest test_detect_models.py -v
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

# Get the directory containing this test file
TOOLS_DIR = Path(__file__).parent.absolute()


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def detect_models_script():
    """Path to detect_models.py script."""
    return TOOLS_DIR / "detect_models.py"


# =============================================================================
# DETECT_MODELS.PY TESTS
# =============================================================================

class TestDetectModels:
    """Tests for detect_models.py static analysis tool."""

    def test_script_exists(self, detect_models_script):
        """Verify detect_models.py exists and is executable."""
        assert detect_models_script.exists(), f"detect_models.py not found at {detect_models_script}"

    def test_help_message(self, detect_models_script):
        """Verify script shows usage when run without arguments."""
        result = subprocess.run(
            [sys.executable, str(detect_models_script)],
            capture_output=True,
            text=True
        )
        assert result.returncode == 1
        assert "Usage:" in result.stderr

    def test_direct_string_pattern(self, detect_models_script, temp_dir):
        """Test detection of direct string literals."""
        test_file = temp_dir / "test_direct.py"
        test_file.write_text('''
from transformers import AutoModel
model = AutoModel.from_pretrained("bert-base-uncased")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "bert-base-uncased" in output["models"]
        assert len(output["unresolved"]) == 0

    def test_variable_reference_pattern(self, detect_models_script, temp_dir):
        """Test detection of model names stored in variables."""
        test_file = temp_dir / "test_variable.py"
        test_file.write_text('''
from transformers import AutoModel
MODEL_NAME = "gpt2"
model = AutoModel.from_pretrained(MODEL_NAME)
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "gpt2" in output["models"]

    def test_fstring_pattern(self, detect_models_script, temp_dir):
        """Test detection of f-string model names."""
        test_file = temp_dir / "test_fstring.py"
        test_file.write_text('''
from transformers import AutoModel
org = "amazon"
name = "chronos-t5-small"
model = AutoModel.from_pretrained(f"{org}/{name}")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "amazon/chronos-t5-small" in output["models"]

    def test_chronos_pipeline(self, detect_models_script, temp_dir):
        """Test detection of ChronosPipeline.from_pretrained()."""
        test_file = temp_dir / "test_chronos.py"
        test_file.write_text('''
from chronos import ChronosPipeline
pipeline = ChronosPipeline.from_pretrained("amazon/chronos-t5-large")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "amazon/chronos-t5-large" in output["models"]

    def test_sentence_transformer(self, detect_models_script, temp_dir):
        """Test detection of SentenceTransformer() constructor."""
        test_file = temp_dir / "test_sentence.py"
        test_file.write_text('''
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "all-MiniLM-L6-v2" in output["models"]

    def test_pipeline_function(self, detect_models_script, temp_dir):
        """Test detection of pipeline() function calls."""
        test_file = temp_dir / "test_pipeline.py"
        test_file.write_text('''
from transformers import pipeline
# Keyword argument form
pipe1 = pipeline("sentiment-analysis", model="ProsusAI/finbert")
# Positional argument form
pipe2 = pipeline("text-generation", "gpt2")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "ProsusAI/finbert" in output["models"]
        assert "gpt2" in output["models"]

    def test_backtrader_params(self, detect_models_script, temp_dir):
        """Test detection of Backtrader-style params tuple."""
        test_file = temp_dir / "test_backtrader.py"
        test_file.write_text('''
import backtrader as bt
from transformers import AutoModel

class MyStrategy(bt.Strategy):
    params = (
        ('lookback', 20),
        ('model_name', 'distilbert-base-uncased'),
        ('threshold', 0.5),
    )

    def __init__(self):
        self.model = AutoModel.from_pretrained(self.params.model_name)
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "distilbert-base-uncased" in output["models"]

    def test_directory_scanning(self, detect_models_script, temp_dir):
        """Test detection across multiple files in a directory."""
        # Create subdirectory with multiple files
        subdir = temp_dir / "agent"
        subdir.mkdir()

        (subdir / "model_loader.py").write_text('''
from transformers import AutoModel
model = AutoModel.from_pretrained("bert-base-uncased")
''')

        (subdir / "strategy.py").write_text('''
from chronos import ChronosPipeline
pipeline = ChronosPipeline.from_pretrained("amazon/chronos-t5-small")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(subdir)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert "bert-base-uncased" in output["models"]
        assert "amazon/chronos-t5-small" in output["models"]

    def test_unresolvable_env_var(self, detect_models_script, temp_dir):
        """Test that environment variables are flagged as unresolvable."""
        test_file = temp_dir / "test_env.py"
        test_file.write_text('''
import os
from transformers import AutoModel
model = AutoModel.from_pretrained(os.environ.get("MODEL_NAME", "default"))
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        # Should be unresolved because os.environ.get is runtime-dependent
        assert len(output["unresolved"]) >= 1

    def test_empty_file(self, detect_models_script, temp_dir):
        """Test handling of file with no model loading."""
        test_file = temp_dir / "test_empty.py"
        test_file.write_text('''
# No model loading here
x = 1 + 2
print("Hello")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        output = json.loads(result.stdout)
        assert output["models"] == []
        assert output["unresolved"] == []

    def test_json_output_is_valid(self, detect_models_script, temp_dir):
        """Verify detect_models.py always outputs valid JSON."""
        test_file = temp_dir / "test.py"
        test_file.write_text('''
from transformers import AutoModel
model = AutoModel.from_pretrained("test-model")
''')

        result = subprocess.run(
            [sys.executable, str(detect_models_script), str(test_file)],
            capture_output=True,
            text=True
        )

        # Should always output valid JSON
        try:
            output = json.loads(result.stdout)
            assert "models" in output
            assert "unresolved" in output
            assert isinstance(output["models"], list)
            assert isinstance(output["unresolved"], list)
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON output: {e}\nOutput was: {result.stdout}")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v"])
