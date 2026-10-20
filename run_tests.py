"""
run_tests.py — wrapper to run pytest with the local lib directory on PYTHONPATH.
Usage: python run_tests.py
"""
import sys
import os

# Add local lib/ and src/ to path
proj = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(proj, "lib"))
sys.path.insert(0, os.path.join(proj, "src"))

import pytest
sys.exit(pytest.main(["tests/", "-v", "--tb=short"]))
