"""
tests/conftest.py
Ensures src/ and lib/ are on sys.path for all pytest runs.
"""
import sys
import os

proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lib_dir = os.path.join(proj, "lib")
src_dir = os.path.join(proj, "src")

if lib_dir not in sys.path:
    sys.path.insert(0, lib_dir)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
