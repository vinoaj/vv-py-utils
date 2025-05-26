# Test Input Directory

This directory is used for testing DOCX to PDF conversion. The directory structure is organized as follows:

- `mock_tests/`: Contains mock DOCX files for testing without Microsoft Word
- `real_tests/`: Contains real DOCX files for testing with Microsoft Word (requires Word installation)

Tests are designed to be resilient - creating files if they don't exist and cleaning up output files after tests run.
Input test files are preserved between test runs while output files are cleaned up.
