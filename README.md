## Overview
This script is designed to process various types of documents (Word, Excel, PDF, PowerPoint) and extract text from them. The extracted text is saved into text files (.txt) in a specified output directory. The script supports parallel processing using multiple worker threads to improve efficiency when dealing with large numbers of files.

## Features
- **Supported File Types**:
  - Word documents (.docx)
  - Excel spreadsheets (.xlsx, .xlsm, .xltx, .xltm)
  - PDF documents (.pdf)
  - PowerPoint presentations (.pptx)
- **Parallel Processing**: Uses multiple threads to process files concurrently.
- **Recursive Processing**: Processes all files in a directory and its subdirectories.
- **Error Handling**: Provides error messages for unsupported file types and processing errors.
- **Timing**: Measures and displays the total processing time.

## Requirements
- Python 3.6+
- Required Libraries:
  - `python-docx` for Word document processing
  - `openpyxl` for Excel document processing
  - `PyPDF2` for PDF document processing
  - `python-pptx` for PowerPoint document processing
  - `concurrent-futures` for parallel processing (included in Python standard library)
- Operating System: Windows, macOS, or Linux

### Installation
To install the required libraries, run the following command:
```bash
pip install python-docx openpyxl PyPDF2 python-pptx
```

## Usage
The script processes all files in a specified input directory and its subdirectories. The extracted text from each document is saved as a text file in the specified output directory.

### Command Line Usage
```bash
python document_processor.py
```

### Parameters
The script can be configured by modifying the following parameters in the `__main__` section:
- `input_directory`: Path to the directory containing the documents to process.
- `output_directory`: Path to the directory where the output text files will be saved.
- `max_workers`: Number of worker threads to use for parallel processing. Set to `None` to use the default number of workers.

### Example Configuration
```python
input_directory = "./documents"  # Input directory containing documents
output_directory = "./output"      # Output directory for text files
max_workers = 4                  # Number of parallel worker threads
```

## Functions
### 1. `process_word_document(filepath, output_dir)`
Extracts text from a Word document (.docx).
- **Parameters**:
  - `filepath`: Path to the Word document.
  - `output_dir`: Directory where the output text file will be saved.
- **Output**: A text file with the same name as the input file, saved in the output directory.

### 2. `process_excel_document(filepath, output_dir)`
Extracts text from an Excel spreadsheet (.xlsx, .xlsm, etc.).
- **Parameters**:
  - `filepath`: Path to the Excel file.
  - `output_dir`: Directory where the output text file will be saved.
- **Output**: A text file with the same name as the input file, saved in the output directory. Tab characters (`\t`) are used as delimiters between cells, and newline characters (`\n`) are used between rows.

### 3. `process_pdf_document(filepath, output_dir)`
Extracts text from a PDF document.
- **Parameters**:
  - `filepath`: Path to the PDF file.
  - `output_dir`: Directory where the output text file will be saved.
- **Output**: A text file with the same name as the input file, saved in the output directory.

### 4. `process_ppt_document(filepath, output_dir)`
Extracts text from a PowerPoint presentation (.pptx).
- **Parameters**:
  - `filepath`: Path to the PowerPoint file.
  - `output_dir`: Directory where the output text file will be saved.
- **Output**: A text file with the same name as the input file, saved in the output directory.

### 5. `process_file(filepath, output_dir)`
Processes a single file based on its extension.
- **Parameters**:
  - `filepath`: Path to the file to process.
  - `output_dir`: Directory where the output text file will be saved.
- **Supported File Types**:
  - .docx
  - .xlsx, .xlsm, .xltx, .xltm
  - .pdf
  - .pptx
- **Output**: A text file with the same name as the input file, saved in the output directory.

### 6. `process_directory(input_dir, output_dir, max_workers=None)`
Processes all files in a directory and its subdirectories in parallel.
- **Parameters**:
  - `input_dir`: Path to the directory containing the documents to process.
  - `output_dir`: Directory where the output text files will be saved.
  - `max_workers`: Number of worker threads to use for parallel processing. If `None`, the default number of workers is used.
- **Output**: Text files for each processed document, saved in the specified output directory.

## Configuration Options
- **Output Format**: The output text files are named using the base name of the input file (e.g., `document.docx` becomes `document.txt`).
- **Error Handling**: Errors during processing are printed to the console, and the script continues processing other files.
- **Parallel Processing**: The number of worker threads can be adjusted using the `max_workers` parameter.

## Troubleshooting
- **Unsupported File Types**: Files with unsupported extensions are skipped, and a message is printed to the console.
- **Permission Errors**: Ensure that the script has read permissions for the input files and write permissions for the output directory.
- **Dependency Issues**: Make sure all required libraries are installed.
- **Large Files**: Processing very large files may require more memory and could affect performance.

## Code Structure
The script is organized into several functions, each handling a specific type of document. The `process_directory` function manages the parallel processing of all files in the input directory and its subdirectories. The `process_file` function determines the appropriate processing function based on the file extension.

### Example Output
```
Processing document.docx
Processing spreadsheet.xlsx
Processing presentation.pptx
Processing document.pdf
Error processing unsupported_file.jpg: Unsupported file type: unsupported_file.jpg
Total processing time: 10.50 seconds
```
