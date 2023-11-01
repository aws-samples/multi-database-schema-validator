import re
import xlsxwriter
from database import SECTION_SOURCE, SECTION_TARGET
from src.report_generator import MigrationSummaryObject
from src import *
from src.templates.base_template import Template


class ExcelTemplate(Template):
    def __init__(self, migration_summary_object: MigrationSummaryObject):
        super().__init__()

        self.migration_summary_object = migration_summary_object

        self.objects = ['tables', 'views', 'stored_procedures', 'functions', 'indexes', 'triggers', 'constraints',
                        'sequences']

        self.book = xlsxwriter.Workbook(migration_summary_object.output_file_name)

        # Add summary worksheet
        self.summary_sheet = self.book.add_worksheet("Summary")
        self.datatype_sheet = None

        self.start_row = 2
        self.col_sql = 0
        self.col_pg = 5
        self.row = 2

        # Formats
        self.fmt_bold_14 = self.book.add_format({'bold': True, 'font_size': 14})
        self.fmt_bold_16 = self.book.add_format({'bold': True, 'font_size': 16})
        self.fmt_bold = self.book.add_format({'bold': True})

        self.fmt_red = self.book.add_format({'bg_color': 'red'})
        self.fmt_yellow = self.book.add_format({'bg_color': 'yellow'})
        self.fmt_green = self.book.add_format({'bg_color': '#92D050'})
        self.fmt_purple = self.book.add_format({'bg_color': '#B2A1C7'})
        self.fmt_orange = self.book.add_format({'bg_color': '#FBC003'})
        self.fmt_dark_orange = self.book.add_format({'bg_color': '#F89647'})

        self.datatype_mapping = {'bigint': ['bigint'], 'bytea': ['binary', 'image', 'varbinarymax'], 'boolean': ['bit'],
                                 'character': ['char'], 'date': ['date'],
                                 'timestamp without time zone': ['datetime', 'smalldatetime', 'datetime2'],
                                 'numeric': ['decimal', 'money', 'numeric', 'smallmoney'],
                                 'double precision': ['float'], 'integer': ['int'],
                                 'text': ['ntext', 'nvarcharmax', 'text', 'varcharmax'],
                                 'character varying': ['nvarchar', 'varchar', 'nchar'], 'real': ['real'],
                                 'smallint': ['smallint', 'tinyint'], 'uuid': ['uniqueidentifier'],
                                 'xml': ['xml', 'xmlmax']}

        self.datatype_mapping_sql_to_pg = {'bigint': 'bigint', 'binary': 'bytea', 'bit': 'boolean', 'char': 'character',
                                           'date': 'date', 'datetime': 'timestamp without time zone',
                                           'datetime2': 'timestamp without time zone', 'decimal': 'numeric',
                                           'float': 'double precision', 'image': 'bytea', 'int': 'integer',
                                           'money': 'numeric',
                                           'ntext': 'text', 'numeric': 'numeric', 'nvarchar': 'character varying',
                                           'nvarcharmax': 'text', 'real': 'real',
                                           'smalldatetime': 'timestamp without time zone', 'smallint': 'smallint',
                                           'smallmoney': 'numeric', 'text': 'text', 'tinyint': 'smallint',
                                           'uniqueidentifier': 'uuid', 'varbinarymax': 'bytea',
                                           'varchar': 'character varying',
                                           'varcharmax': 'text', 'xml': 'xml',
                                           "xmlmax": "xml",  # Added on my own
                                           "hierarchyid": None,  # Added on my own
                                           "nchar": "character varying",  # Added on my own
                                           "time": None,  # Added on my own
                                           "geographymax": None
                                           }

        self.color_assignment = {
            COLOR_RED: self.fmt_red,
            COLOR_GREEN: self.fmt_green,
            COLOR_YELLOW: self.fmt_yellow
        }

        self.color_reference = [(self.fmt_green, f"{GREEN}%"), (self.fmt_yellow, f'> {YELLOW_LEFT}%'),
                                (self.fmt_red, f'< {YELLOW_LEFT}%')]

        self.create_report(migration_summary_object.combined_row_count_data, migration_summary_object.validation_data,
                           migration_summary_object.comparison_data, migration_summary_object.output_file_name)

    def create_report(self, combined_row_count_data, validation_data, comparison_data, file_name):
        self.add_table_count_validation_sheet(combined_row_count_data)
        self.add_summary_data(validation_data)

        for schema in comparison_data:
            if comparison_data[schema][DISPLAY_FLAG]:
                self.add_schema_worksheet(schema, comparison_data[schema][ALL_ITEMS])

        self.close()

    def add_summary_data(self, data):
        schema_validation = data[SCHEMA]
        start_col = 2
        row = 5

        self.summary_sheet.write(2, start_col, "Database Object Validation Report", self.fmt_bold_16)

        self.summary_sheet.write(3, start_col, "Schema validation", self.fmt_bold_14)

        self.summary_sheet.write(4, start_col, "Missing schemas", self.fmt_bold)

        if not schema_validation[MISSING_ITEMS]:
            self.summary_sheet.write(4, start_col + 1, "None")
            row += 1

        for item in schema_validation[MISSING_ITEMS]:
            self.summary_sheet.write(row, start_col, item)
            row += 1

        # validation = ["Validation %", schema_validation[VALIDATION_PERCENT]]
        # self.summary_sheet.write_row(row, start_col, validation)
        # row += 2

        # Schema wise validation %
        self.summary_sheet.write_row(row, start_col, ["Schema", "Validation %"], self.fmt_bold)

        row += 1
        for schema in data[OBJECTS]:
            self.summary_sheet.write(row, start_col, schema)
            per = data[OBJECTS][schema][VALIDATION_PERCENT]
            if per == 'NA':
                fmt = ''
            elif per > GREEN:
                fmt = self.fmt_green
            elif YELLOW_LEFT < per < YELLOW_RIGHT:
                fmt = self.fmt_yellow
            else:
                fmt = self.fmt_red

            per = float('{:.2f}'.format(per)) if per != 'NA' else per
            self.summary_sheet.write(row, start_col + 1, per, fmt)
            row += 1

        row += 2

        # Object validation:
        self.summary_sheet.write(row, start_col, "Object validation per schema", self.fmt_bold_14)

        row += 2

        schema_objects = data[OBJECTS]
        for schema in schema_objects:
            self.summary_sheet.write(row, start_col, schema, self.fmt_bold)
            row += 1
            del(schema_objects[schema][VALIDATION_PERCENT])
            del (schema_objects[schema][DISPLAY_FLAG])
            for obj in schema_objects[schema]:
                self.summary_sheet.write(row, start_col, obj.capitalize())
                validation_percent = schema_objects[schema][obj][VALIDATION_PERCENT]
                validation_reason = schema_objects[schema][obj][REASON]

                # Disabling colour codes for avoiding confusion in summary tab: 14/03/2023
                # fmt = self.fmt_green
                # if validation_percent < YELLOW_RIGHT:
                #     fmt = self.fmt_yellow
                # if validation_percent < YELLOW_LEFT:
                #     fmt = self.fmt_red
                # else:
                #     fmt = self.fmt_green
                # if validation_reason:
                #     fmt = self.fmt_yellow

                self.summary_sheet.write(row, start_col + 1, validation_percent)
                self.summary_sheet.write(row, start_col + 2, validation_reason)
                row += 1
            row += 1

        self.add_db_detail()
        self.summary_sheet.set_column(start_col, start_col, 40)

    def add_db_detail(self):
        row = 3
        col = 7

        # Source ---------------
        self.summary_sheet.write_row(row, col, ["Source", self.db_type_source.upper()])
        row += 1

        self.summary_sheet.write_row(row, col, ["Database", self.migration_summary_object.source_db.database])
        row += 1

        self.summary_sheet.write_row(row, col, ["Host", self.migration_summary_object.source_db.host])
        row += 3

        # Target ---------------
        self.summary_sheet.write_row(row, col, ["Target", self.db_type_target.upper()])
        row += 1

        self.summary_sheet.write_row(row, col, ["Database", self.migration_summary_object.target_db.database])
        row += 1

        self.summary_sheet.write_row(row, col, ["Host", self.migration_summary_object.target_db.host])

        row += 4

        for c in self.color_reference:
            self.summary_sheet.write(row, col, '', c[0])
            self.summary_sheet.write(row, col + 1, c[1])
            row += 1

    def add_table_count_validation_sheet(self, data):
        start_row = 5
        row = 2
        col_sql = 0
        col_pg = 5

        column_widths_sql = [len(max(self.objects, key=len)) + 5, 30, 20]
        column_widths_pg = [len(max(self.objects, key=len)) + 5, 30, 20]

        colored_cell_fmt = self.book.add_format({'bold': True, 'bg_color': 'yellow'})

        worksheet = self.book.add_worksheet("Table row count")
        heading = [self.db_type_source, "", "", "", "", self.db_type_target]

        worksheet.write(0, 2, "Table row counts", self.fmt_bold_14)
        worksheet.write_row(row, col_sql, heading, colored_cell_fmt)

        row += 2
        sub_heading = ["SchemaName", "TableName", "RowCount"]
        worksheet.write_row(row, col_sql, sub_heading, self.fmt_bold)
        worksheet.write_row(row, col_pg, sub_heading, self.fmt_bold)

        row += 1

        # Print SQL data
        for data_row in data[SECTION_SOURCE]:
            worksheet.write_row(row, col_sql, [data_row[0], data_row[1]])
            worksheet.write(row, col_sql + 2, data[SECTION_SOURCE][data_row][ROW_COUNT],
                            self.color_assignment[data[SECTION_SOURCE][data_row][COLOR]])
            column_widths_sql[1] = max(len(data_row[1]) + 5, column_widths_sql[1])
            row += 1

        row, start_row = start_row, row

        # Print PG data
        for data_row in data[SECTION_TARGET]:
            worksheet.write_row(row, col_pg, [data_row[0], data_row[1], data[SECTION_TARGET][data_row][ROW_COUNT]])
            column_widths_pg[1] = max(len(data_row[1]) + 5, column_widths_pg[1])
            row += 1

        self.apply_widths(worksheet, column_widths_sql, col_sql)
        self.apply_widths(worksheet, column_widths_pg, col_pg)

        color_mappings = [
            (self.fmt_green, 'The SCHEMA, TABLE pair was found their counts matched'),
            (self.fmt_red, 'The SCHEMA, TABLE pair was not found'),
            (self.fmt_yellow, 'The SCHEMA, TABLE pair was found but their counts didn\'t match')
        ]

        row = 1
        mapping_col = 9
        for m in color_mappings:
            worksheet.write(row, mapping_col, '', m[0])
            worksheet.write(row, mapping_col + 1, m[1])
            row += 1

    def add_schema_worksheet(self, worksheet_name, data):
        worksheet_name = re.sub(r"[]\[\\]", "", worksheet_name)
        # Replace any of \  /  ?  *  [  or  ]
        worksheet = self.book.add_worksheet(worksheet_name)
        heading = [self.db_type_source, "", "", "", "", self.db_type_target]
        sub_heading = ["ObjectType", "SchemaName", "Count"]

        # Column widths:
        sql_widths = [len(max(self.objects, key=len)) + 5, len(worksheet_name) + 5, 20]
        pg_widths = [len(max(self.objects, key=len)) + 5, len(worksheet_name) + 5, 20]

        worksheet.write(0, self.col_sql, "Summary for schema - {}".format(worksheet_name.upper()),
                        self.fmt_bold_14)

        worksheet.write_row(self.row, self.col_sql, heading, self.fmt_bold)
        self.row += 1
        worksheet.write_row(self.row, self.col_sql, sub_heading, self.fmt_bold)
        worksheet.write_row(self.row, self.col_pg, sub_heading, self.fmt_bold)

        self.row += 1

        # Print the numerical summaries of the data:
        start_row = self.row

        for obj in data:
            data_row = [obj.upper(), worksheet_name, data[obj][NUM_SOURCE]]
            worksheet.write_row(self.row, self.col_sql, data_row)
            self.row += 1

        self.row = start_row

        for obj in data:
            data_row = [obj.upper(), worksheet_name, data[obj][NUM_TARGET]]
            worksheet.write_row(self.row, self.col_pg, data_row)
            self.row += 1

        self.row += 2

        # Write the individual objects

        for obj in data:
            start_row = self.row + 2
            sub_heading = ["ObjectType", "SchemaName", "ObjectName"]

            # if len(data[obj][OBJECTS_SQL]) and len(data[obj][OBJECTS_PG]):
            worksheet.write(self.row, self.col_sql, obj.upper(), self.fmt_bold)
            self.row += 1

            if len(data[obj][OBJECTS_SOURCE]):
                worksheet.write_row(self.row, self.col_sql, sub_heading, self.fmt_bold)
            else:
                worksheet.write(self.row, self.col_sql, "NA", self.fmt_bold)

            if len(data[obj][OBJECTS_TARGET]):
                worksheet.write_row(self.row, self.col_pg, sub_heading, self.fmt_bold)
            else:
                worksheet.write(self.row, self.col_sql, "NA", self.fmt_bold)

            self.row += 1
            # Write SQL objects:
            for i in data[obj][OBJECTS_SOURCE]:
                data_row = [obj.upper(), worksheet_name, i]
                # worksheet.write(self.row, self.col_sql, i)
                worksheet.write_row(self.row, self.col_sql, data_row)
                sql_widths[2] = max(len(i) + 5, sql_widths[2])
                self.row += 1

            self.row, start_row = start_row, self.row

            # Write PG objects
            for i in data[obj][OBJECTS_TARGET]:
                data_row = [obj.upper(), worksheet_name, i]
                # worksheet.write(self.row, self.col_pg, i)
                worksheet.write_row(self.row, self.col_pg, data_row)
                pg_widths[2] = max(len(i) + 5, pg_widths[2])
                self.row += 1

            self.row = max(self.row, start_row) + 2

        self.row = 2
        self.apply_widths(worksheet, sql_widths, self.col_sql)
        self.apply_widths(worksheet, pg_widths, self.col_pg)

    def add_datatype_summary(self, count, details):
        self.datatype_sheet = self.book.add_worksheet("Datatype summary")
        sql_col = 4
        pg_col = 7
        row = 5

        count_sql = {x[DATA_TYPE]: x[COUNT] for x in count[DATATYPE_COUNT_SOURCE]}
        count_pg = {x[DATA_TYPE]: x[COUNT] for x in count[DATATYPE_COUNT_TARGET]}

        formatting_dict_sql = dict()
        formatting_dict_pg = dict()

        for d_type in self.datatype_mapping:
            if d_type in count_pg:
                total_count_sql = sum([count_sql[x] if x in count_sql else 0 for x in self.datatype_mapping[d_type]])
                if total_count_sql == (count_pg[d_type] if d_type in count_pg else 0):
                    for tp in self.datatype_mapping[d_type]:
                        formatting_dict_sql[tp] = self.fmt_green
                    formatting_dict_pg[d_type] = self.fmt_green
                elif total_count_sql != (count_pg[d_type] if d_type in count_pg else 0):
                    for tp in self.datatype_mapping[d_type]:
                        formatting_dict_sql[tp] = self.fmt_red
                    formatting_dict_pg[d_type] = self.fmt_red
            else:
                for tp in self.datatype_mapping[d_type]:
                    formatting_dict_sql[tp] = self.fmt_dark_orange
                    formatting_dict_pg[d_type] = self.fmt_red

        datatype_fmt = dict()
        # Determine formatting for the datatypes
        for d_type in count_sql:
            if d_type in self.datatype_mapping_sql_to_pg and self.datatype_mapping_sql_to_pg[d_type]:
                if len(self.datatype_mapping[self.datatype_mapping_sql_to_pg[d_type]]) == 1:
                    datatype_fmt[d_type] = self.fmt_green
                else:
                    datatype_fmt[d_type] = self.fmt_purple
            else:
                datatype_fmt[d_type] = self.fmt_orange

        # Print datatype counts

        self.datatype_sheet.write(row, sql_col, "Datatype counts", self.fmt_bold_14)
        row += 1

        self.datatype_sheet.write(row, sql_col, "Source DB", self.fmt_bold)
        self.datatype_sheet.write(row, pg_col, "Converted DB", self.fmt_bold)
        row += 1

        for d_type in count_sql:
            self.datatype_sheet.write(row, sql_col, d_type, datatype_fmt[d_type])
            if d_type in formatting_dict_sql:
                self.datatype_sheet.write(row, sql_col + 1, count_sql[d_type], formatting_dict_sql[d_type])
            else:
                self.datatype_sheet.write(row, sql_col + 1, count_sql[d_type])
            row += 1

        count_section_end = row
        row = 7

        for d_type in count_pg:
            self.datatype_sheet.write(row, pg_col, d_type)
            if d_type in formatting_dict_pg:
                self.datatype_sheet.write(row, pg_col + 1, count_pg[d_type], formatting_dict_pg[d_type])
            else:
                self.datatype_sheet.write(row, pg_col + 1, count_pg[d_type])
            row += 1

        count_section_end = max(count_section_end, row) + 3

        self.apply_widths(self.datatype_sheet, [25, 20, 20, 25, 10, 25, 20, 20, 20], sql_col)

        # Print datatype details
        row = count_section_end
        self.datatype_sheet.write(row, sql_col, "Datatype details", self.fmt_bold_14)
        row += 1

        self.datatype_sheet.write(row, sql_col, "Source DB", self.fmt_bold)
        self.datatype_sheet.write(row, sql_col + 5, "Converted DB", self.fmt_bold)
        row += 1

        labels = ["Schema name", "Table name", "Column name", "Data type"]

        self.datatype_sheet.write_row(row, sql_col, labels, self.fmt_bold)
        row += 1

        for data_row in details[DATATYPE_DETAILS_SOURCE]:
            r = [data_row[SCHEMA_NAME], data_row[TABLE_NAME], data_row[COLUMN_NAME], data_row[DATA_TYPE]]
            self.datatype_sheet.write_row(row, sql_col, r)
            row += 1

        row = count_section_end + 2
        self.datatype_sheet.write_row(row, sql_col + 5, labels, self.fmt_bold)
        row += 1

        for data_row in details[DATATYPE_DETAILS_TARGET]:
            r = [data_row[SCHEMA_NAME], data_row[TABLE_NAME], data_row[COLUMN_NAME], data_row[DATA_TYPE]]
            self.datatype_sheet.write_row(row, sql_col + 5, r)
            row += 1

        self.add_datatype_references()

    def add_datatype_references(self):
        row = 5
        col = 0

        # Datatype mapping from SQL to PG color reference
        mapping = [
            (self.fmt_green, "1-1 mapping"),
            (self.fmt_purple, "Many - 1 mapping"),
            (self.fmt_orange, "Mapping unavailable")
        ]

        self.datatype_sheet.write(row, col, "Datatype mapping SQL to PG", self.fmt_bold_14)
        row += 1

        for m in mapping:
            self.datatype_sheet.write(row, col, '', m[0])
            self.datatype_sheet.write(row, col + 1, m[1])
            row += 1

        row += 2

        # Mapping for validation colors
        self.datatype_sheet.write(row, col, "Count validation", self.fmt_bold_14)

        row += 1
        mapping = [
            (self.fmt_green, "Counts matched"),
            (self.fmt_dark_orange, "Counts not matched"),
            (self.fmt_red, "Converted type not found")
        ]

        for m in mapping:
            self.datatype_sheet.write(row, col, '', m[0])
            self.datatype_sheet.write(row, col + 1, m[1])
            row += 1

        # Datatype mapping
        row += 3

        self.datatype_sheet.write(row, col, "Datatype mapping", self.fmt_bold_14)
        row += 1

        for d_type in self.datatype_mapping_sql_to_pg:
            self.datatype_sheet.write_row(
                row, col,
                [d_type, self.datatype_mapping_sql_to_pg[d_type].upper() if self.datatype_mapping_sql_to_pg[d_type]
                 else '*NA*']
            )
            row += 1

        self.apply_widths(self.datatype_sheet, [25, 35], col)

    @staticmethod
    def apply_widths(worksheet, widths, start_col):
        count = 0
        for i in range(start_col, start_col + len(widths)):
            worksheet.set_column(i, i, widths[count])
            count += 1

    def close(self):
        self.book.close()
