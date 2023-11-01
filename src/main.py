import os
import sys

try:
    cwd = os.getcwd().replace('src', '')
    sys.path.index(cwd)
    os.environ['PYTHONPATH'] = cwd
except ValueError:
    cwd = os.getcwd().replace('src', '')
    sys.path.append(cwd)
    os.environ['PYTHONPATH'] = cwd

import argparse
from database import CONFIG_FILE, FILE_FORMAT, SECTION_FILE_FORMAT
from logger import get_logger
from src import DEBUG_LEVEL, LOGGING, OUTPUT_DIR, LOGS_DIR
from src.report_generator import MigrationSummaryObject
from src.templates.pdf_template import PDFTemplate
from src.templates.html_template import HTMLTemplate
from src.templates.excel_template import ExcelTemplate
from src.utility.utils import CommonUtility

format_mapping = {
    "pdf": PDFTemplate,
    "xlsx": ExcelTemplate,
    "html": HTMLTemplate
}


def set_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-format", dest="file_format",
                        help="xlsx - Create an Excel report\n"
                             "pdf - Create a PDF report\n"
                             "html - Create n HTML report", default='xlsx')

    return parser.parse_args()


def create_directories():
    if not os.path.exists(OUTPUT_DIR):
        logger.info("Output directory doesn't currently exist. Creating  now...")
        os.mkdir(OUTPUT_DIR)
        logger.info("Created")


if __name__ == '__main__':
    logger = get_logger(__name__)
    logger.info("STARTING EXECUTION")
    args = set_arguments()

    file_format = CommonUtility.read_configurations(FILE_FORMAT, CONFIG_FILE, SECTION_FILE_FORMAT).lower()

    create_directories()

    if file_format not in format_mapping.keys():
        print("Select format as one of xlsx, pdf, html")
        print("Exiting..")
        exit()

    migration_summary = MigrationSummaryObject(file_format=file_format)
    migration_summary.generate_report_data()
    report = format_mapping[file_format](migration_summary)

    logger.info(f"\n\nFINISHED EXECUTION: File - {migration_summary.output_file_name}")
