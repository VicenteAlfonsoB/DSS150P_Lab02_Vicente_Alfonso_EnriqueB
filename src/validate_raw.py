"""Starter validation checks for raw outputs."""
from pathlib import Path
import json
ROOT=Path(__file__).resolve().parents[1]

def main():
    # TODO: assert expected raw files exist; API event_ids unique; required metadata fields present;
    # updated_at parseable; watermark equals max updated_at after ingestion.
    pass
if __name__=='__main__': main()
