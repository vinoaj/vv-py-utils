import os
import platform
from pathlib import Path
from unittest.mock import patch

import pytest

from vvpyutils.docx import batch_convert, convert_docx_to_pdf


@pytest.fixture
def sample_docx() -> Path:
    return Path(__file__).parent / "docx_samples" / "demo.docx"


@pytest.fixture
def ensure_output_dir():
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    return output_dir


@pytest.fixture
def ensure_test_dirs():
    """Create and clean fixed directories for testing"""
    # Set up fixed input and output directories for tests
    base_dir = Path(__file__).parent
    input_dir = base_dir / "docx_samples" / "test_input"
    output_dir = base_dir / "output" / "test_output"

    # Create directories
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Clean only output files, not input files
    for file in output_dir.glob("*.pdf"):
        file.unlink()

    return {"input_dir": input_dir, "output_dir": output_dir}


def is_word_available():
    """Check if Microsoft Word is available on the system"""
    if platform.system() == "Darwin":  # macOS
        word_path = "/Applications/Microsoft Word.app"
        return os.path.exists(word_path)
    elif platform.system() == "Windows":
        # Check common Windows Microsoft Word installation paths
        possible_paths = [
            r"C:\Program Files\Microsoft Office\root\Office16\WINWORD.EXE",
            r"C:\Program Files (x86)\Microsoft Office\root\Office16\WINWORD.EXE",
            r"C:\Program Files\Microsoft Office\Office16\WINWORD.EXE",
            r"C:\Program Files (x86)\Microsoft Office\Office16\WINWORD.EXE",
        ]
        return any(os.path.exists(path) for path in possible_paths)
    # For Linux and other systems, assume Word is not available
    return False


def create_empty_pdf(path):
    """Create an empty PDF file for testing purposes"""
    with open(path, "wb") as f:
        # Simple valid PDF file header
        f.write(
            b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents [] >>\nendobj\nxref\n0 4\n0000000000 65535 f \n0000000018 00000 n \n0000000066 00000 n \n0000000125 00000 n \ntrailer\n<< /Root 1 0 R /Size 4 >>\nstartxref\n214\n%%EOF\n"
        )


@patch("vvpyutils.docx.convert")
def test_convert_docx_to_pdf(mock_convert, sample_docx: Path, ensure_test_dirs) -> None:
    # Mock the convert function to create a simple PDF file
    def side_effect(input_path, output_path):
        create_empty_pdf(output_path)

    mock_convert.side_effect = side_effect

    output_dir = ensure_test_dirs["output_dir"]
    output_pdf = output_dir / "mock_single_output.pdf"

    # Remove the file if it already exists to ensure clean test
    if output_pdf.exists():
        os.unlink(output_pdf)

    try:
        result = convert_docx_to_pdf(sample_docx, output_pdf)

        # Check that our function called docx2pdf.convert with correct arguments
        mock_convert.assert_called_once_with(sample_docx, output_pdf)

        # Check the result is as expected
        assert result.exists()
        assert result.suffix == ".pdf"
        assert result == output_pdf
    finally:
        # Clean up PDF file after test
        if output_pdf.exists():
            output_pdf.unlink()


@patch("vvpyutils.docx.convert")
def test_batch_convert(mock_convert, sample_docx: Path, ensure_test_dirs) -> None:
    # Setup mock
    def side_effect(input_path, output_path):
        create_empty_pdf(output_path)

    mock_convert.side_effect = side_effect

    # Get fixed directories
    input_dir = ensure_test_dirs["input_dir"]
    output_dir = ensure_test_dirs["output_dir"] / "mock_batch"
    output_dir.mkdir(exist_ok=True)

    # Create a specific input folder for mock tests only (for symmetry with real_tests)
    mock_input_dir = input_dir / "mock_tests"
    mock_input_dir.mkdir(exist_ok=True)

    try:
        # Create test DOCX files only if they don't exist
        for i in range(3):
            docx_file = mock_input_dir / f"mock_demo_{i}.docx"
            if not docx_file.exists():
                # Just create empty files for testing
                docx_file.write_text("Mock DOCX content")

        # Call batch_convert directly on the mock_tests directory
        successful, failed = batch_convert(mock_input_dir, output_dir)

        assert len(successful) == 3
        assert len(failed) == 0

        for pdf_file in successful:
            assert pdf_file.exists()
            assert pdf_file.suffix == ".pdf"
    finally:
        # Clean up only the PDF files, keep the DOCX files
        for pdf_file in output_dir.glob("*.pdf"):
            if pdf_file.exists():
                pdf_file.unlink()


# Tests using the actual Word application - only run if Word is installed
word_available = is_word_available()


@pytest.mark.skipif(not word_available, reason="Microsoft Word not available")
def test_real_convert_docx_to_pdf(sample_docx, ensure_test_dirs):
    """Test actual conversion with docx2pdf if Word is available"""
    output_dir = ensure_test_dirs["output_dir"]
    output_pdf = output_dir / "real_single_output.pdf"

    # Remove the file if it already exists for clean test
    if output_pdf.exists():
        os.unlink(output_pdf)

    try:
        result = convert_docx_to_pdf(sample_docx, output_pdf)

        # Check the result
        assert result.exists()
        assert result.suffix == ".pdf"
        assert result == output_pdf
    finally:
        # Clean up PDF file after test
        if output_pdf.exists():
            output_pdf.unlink()


@pytest.mark.skipif(not word_available, reason="Microsoft Word not available")
def test_real_batch_convert(sample_docx, ensure_test_dirs):
    """Test actual batch conversion with docx2pdf if Word is available"""
    # Get fixed directories
    input_dir = ensure_test_dirs["input_dir"]
    output_dir = ensure_test_dirs["output_dir"] / "real_batch"
    output_dir.mkdir(exist_ok=True)

    # Create a specific input folder for real tests only
    real_input_dir = input_dir / "real_tests"
    real_input_dir.mkdir(exist_ok=True)

    try:
        # Copy sample DOCX file to input directory only if they don't exist
        for i in range(2):  # Use just 2 files to keep the test shorter
            docx_file = real_input_dir / f"real_demo_{i}.docx"
            if not docx_file.exists() and sample_docx.exists():
                # Copy the sample docx file
                with open(sample_docx, "rb") as src, open(docx_file, "wb") as dst:
                    dst.write(src.read())

        # Run the actual batch conversion specifically on the real_tests folder
        successful, failed = batch_convert(real_input_dir, output_dir)

        # Verify results
        assert len(failed) == 0, f"Failed conversions: {failed}"
        assert len(successful) == 2

        for pdf_file in successful:
            assert pdf_file.exists()
            assert pdf_file.suffix == ".pdf"
    finally:
        # Clean up only PDF files, keep the DOCX files
        for pdf_file in output_dir.glob("*.pdf"):
            if pdf_file.exists():
                pdf_file.unlink()


if __name__ == "__main__":
    pytest.main([__file__])
