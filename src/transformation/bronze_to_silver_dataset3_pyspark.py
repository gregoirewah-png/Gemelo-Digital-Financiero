"""Entrada compatible del pipeline Silver para Dataset 3."""
import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.transformation.silver_pipeline import process_silver


def process_financial_profile(run_id):
    return process_silver(3, run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", "--run-id", dest="run_id", required=True)
    process_financial_profile(parser.parse_args().run_id)
