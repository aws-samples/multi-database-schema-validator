import os
# from weasyprint import HTML
from jinja2 import Environment, FileSystemLoader
from src.report_generator import MigrationSummaryObject
from src.templates.base_template import Template


class PDFTemplate(Template):
    path = os.path.join(os.path.dirname(__file__), './static')
    templateLoader = FileSystemLoader(searchpath=path)
    environment = Environment(loader=templateLoader, autoescape=True)
    template = environment.get_template("pdf_template.html")

    def __init__(self, migration_summary_object: MigrationSummaryObject):
        super().__init__()

        self.create_report('',
                           migration_summary_object.validation_data, migration_summary_object.comparison_data,
                           migration_summary_object.output_file_name,
                           migration_summary_object.database_summary,
                           migration_summary_object.missing_schemas)

    def create_report(self, combined_row_count_data, validation_data, comparison_data, file_name, database_summary,
                      missing_schemas):
        # Create a Summary
        content = self.template.render(summary_data=validation_data, comparison_data=comparison_data,
                                       database_summary=database_summary, missing_schemas=missing_schemas)
        # HTML(string=content).write_pdf(file_name)
