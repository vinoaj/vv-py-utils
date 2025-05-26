# Test Output Directory

This directory is used for storing PDF files generated during DOCX to PDF conversion tests. The directory structure is organized as follows:

- Root directory: For single file conversion tests
- `mock_batch/`: Output files generated from mock tests (without Word)
- `real_batch/`: Output files generated from real tests (requiring Word installation)

All output files are automatically cleaned up after tests run, even if tests fail (using try/finally blocks).
