"""
Compile the static Typst report into a PDF.

The report source and all outputs live on OneDrive:
    {onedrive}/reports/analysis_report.typ  →  reports/analysis_report.pdf

Figures and tables are resolved with relative paths inside the .typ file
(../figures/... and ../tables/...) so no path arguments are needed at
compile time.

Usage:
    uv run python -m analysis.create_analysis_report

Or call from main.py:
    from analysis.create_analysis_report import create_analysis_report
    create_analysis_report()
"""

import os
import subprocess
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories


def create_analysis_report() -> str:
    """
    Compile ``reports/analysis_report.typ`` into a PDF in the same directory.

    Returns
    -------
    str
        Path to the compiled PDF, or the ``.typ`` path if compilation failed.
    """
    dirs     = setup_directories()
    onedrive = dirs["root"]
    typ_path = os.path.join(onedrive, "reports", "analysis_report.typ")
    pdf_path = os.path.join(onedrive, "reports", "analysis_report.pdf")

    print(f"Source: {typ_path}")
    print("Compiling report ...")

    result = subprocess.run(
        ["typst", "compile", "--root", onedrive, typ_path, pdf_path],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"typst compile FAILED:\n{result.stderr}")
        return typ_path

    print(f"Report ready: {pdf_path}")
    return pdf_path


if __name__ == "__main__":
    create_analysis_report()
