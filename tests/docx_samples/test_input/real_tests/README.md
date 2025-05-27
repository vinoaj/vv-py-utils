# Real Tests Directory

This directory contains real DOCX files used for testing the DOCX to PDF conversion functionality with Microsoft Word.

## Important Notes

- Tests in this directory will be **skipped** if Microsoft Word is not available on the system
- Test files are created here automatically if they don't exist
- Test files are preserved between test runs
- Only the PDF output files are cleaned up after tests

These tests validate the actual conversion process using Microsoft Word and the docx2pdf library.
