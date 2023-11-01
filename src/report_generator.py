import json
import os.path
from datetime import datetime
from time import strftime, gmtime
from sqlalchemy import text
from database import *
from database.database_engine import SourceDatabase, TargetDatabase
from database.database_queries import *
from src import *
from logger import get_logger
from src.utility.utils import CommonUtility


class MigrationSummaryObject:
    objects = ['table', 'view', 'procedure', 'function', 'index', 'trigger', 'constraint',
               'sequence']

    def __init__(self, file_format):
        self.database_summary = None
        self.logger = get_logger(__name__)

        self.source_db = SourceDatabase()
        self.target_db = TargetDatabase()

        self.db_type_source = self.source_db.db_type
        self.db_type_target = self.target_db.db_type

        self.output_directory = os.path.join(CommonUtility.get_project_root(), OUTPUT_DIR)
        self.output_file_name = os.path.join(self.output_directory, OUTPUT_FILE_FORMAT.format(self.source_db.db_type,
                                                                                              self.target_db.db_type,
                                                                                              strftime('%Y%m%d_%H%M%S',
                                                                                                       gmtime()),
                                                                                              file_format))

        self.object_query_mapping = {
            SCHEMA: GET_SCHEMAS,
            TABLE: GET_TABLES,
            VIEW: GET_VIEWS,
            PROCEDURE: GET_PROCEDURES,
            FUNCTION: GET_FUNCTIONS,
            INDEX: GET_INDEXES,
            TRIGGER: GET_TRIGGERS,
            CONSTRAINT: GET_CONSTRAINTS,
            SEQUENCE: GET_SEQUENCES,
            DATATYPE_COUNT: GET_DATATYPE_COUNTS,
            DATATYPE_DETAILS: GET_DATATYPES
        }

        # Data
        self.combined_row_count_data = None
        self.validation_data = None
        self.comparison_data = None
        self.missing_schemas = []

        self.database_details = {}

    def __repr__(self):
        print(f"Validation data: \n{self.validation_data}")
        print(f"Comparison data: \n{self.comparison_data}")

    def get_source_schemas(self):
        """
        Gets the names for the schemas for the source database.
        Note: If the database type is Oracle, the username for the database is used as the schema
        """
        if self.db_type_source == ORACLE:
            query = GET_SCHEMAS[self.db_type_source].format(self.source_db.username)
            source_schemas = self.source_db.execute_query(query)
        else:
            source_schemas = self.source_db.execute_query(GET_SCHEMAS[self.db_type_source])

        return source_schemas

    def get_target_schemas(self):
        """
        Gets the names for the schemas for the target database.
        Note: If the database type is Oracle, the username for the database is used as the schema
        """
        if self.db_type_target == ORACLE:
            query = GET_SCHEMAS[self.db_type_target].format(self.target_db.username)
            target_schemas = self.target_db.execute_query(query)
        else:
            target_schemas = self.target_db.execute_query(GET_SCHEMAS[self.db_type_target])

        return target_schemas

    def get_source_data(self, schema, db_object):
        self.logger.debug(f"Source data for SCHEMA: {schema} OBJECT: {db_object}")
        data = self.source_db.execute_query(
            self.object_query_mapping[db_object][self.db_type_source].format(schema))
        self.logger.debug(f"{json.dumps(data, indent=4)}")
        return data

    def get_target_data(self, schema, db_object):
        self.logger.debug(f"Target data for SCHEMA: {schema} OBJECT: {db_object}")
        data = self.target_db.execute_query(
            self.object_query_mapping[db_object][self.db_type_target].format(schema))
        self.logger.debug(f"{json.dumps(data, indent=4)}")
        return data

    def get_datatype_data(self):
        data_source = self.source_db.execute_query(self.object_query_mapping[DATATYPE_COUNT][self.db_type_source])
        data_target = self.target_db.execute_query(self.object_query_mapping[DATATYPE_COUNT][self.db_type_target])

        count_data = {
            DATATYPE_COUNT_SOURCE: data_source,
            DATATYPE_COUNT_TARGET: data_target
        }

        datatype_details_source = self.source_db.execute_query(self.object_query_mapping[DATATYPE_DETAILS]
                                                               [self.db_type_source])
        datatype_details_target = self.target_db.execute_query(self.object_query_mapping[DATATYPE_DETAILS]
                                                               [self.db_type_target])

        datatype_details = {
            DATATYPE_DETAILS_SOURCE: datatype_details_source,
            DATATYPE_DETAILS_TARGET: datatype_details_target
        }
        return count_data, datatype_details

    def get_data(self, source_schemas, target_schemas):
        # Prepares a structure similar to this:
        # {
        #     "SCHEMA_NAME": {}
        # }

        all_schemas = list(
            set([x[SCHEMA_NAME] for x in source_schemas]).union(set([x[SCHEMA_NAME] for x in target_schemas])))
        source_data = dict(
            zip([x[SCHEMA_NAME].lower() for x in source_schemas], [{} for _ in range(len(source_schemas))]))
        target_data = dict(zip(all_schemas, [{} for _ in range(len(all_schemas))]))

        schema_level_counts = dict()

        self.logger.info("\n****** Getting data for the SOURCE database ******")
        # For each schema in SQL get the different objects
        for schema in source_schemas:
            self.logger.info(f"\n\nGetting SOURCE data for Schema: {schema[SCHEMA_NAME]}")
            schema_name = schema[SCHEMA_NAME]
            schema_name_lower = schema_name.lower()

            # Using schema_name.lower() here because the case of all objects
            # in the final data structure is lower
            schema_level_counts[schema_name_lower] = dict()

            all_object_count = 0
            for obj in self.objects:
                self.logger.info(f"Getting {obj.upper()} objects for source")
                data = self.get_source_data(schema_name, obj) if obj in self.object_query_mapping else []
                final_data = []
                for row in data:
                    final_data.append(row[obj + '_name'].lower())

                source_data[schema_name_lower][obj] = {}
                source_data[schema_name_lower][obj][OBJECTS_SOURCE] = final_data
                source_data[schema_name_lower][obj][NUM_SOURCE] = len(final_data)
                all_object_count += source_data[schema_name_lower][obj][NUM_SOURCE]

            schema_level_counts[schema_name_lower][NUM_SOURCE] = all_object_count
            schema_level_counts[schema_name_lower][NUM_TARGET] = 0

        self.logger.info("\n\n****** Getting data for the TARGET database ******")
        # For each schema in PG get the different objects
        for schema in target_schemas:
            self.logger.info(f"\n\nGetting TARGET data for Schema: {schema[SCHEMA_NAME]}")
            schema_name = schema[SCHEMA_NAME]
            schema_name_lower = schema_name.lower()

            all_object_count = 0
            for obj in self.objects:
                self.logger.info(f"Getting {obj.upper()} objects for target")
                data = self.get_target_data(schema_name, obj) if obj in self.object_query_mapping else []
                final_data = []
                for row in data:
                    final_data.append(row[obj + '_name'].lower())

                target_data[schema_name_lower][obj] = {}
                target_data[schema_name_lower][obj][OBJECTS_TARGET] = final_data
                target_data[schema_name_lower][obj][NUM_TARGET] = len(final_data)
                all_object_count += target_data[schema_name_lower][obj][NUM_TARGET]

            # Only add the total count when the schema is also present in the source
            if schema_name_lower in schema_level_counts:
                schema_level_counts[schema_name_lower][NUM_TARGET] = all_object_count
            else:
                schema_level_counts[schema_name_lower] = {
                    NUM_TARGET: all_object_count,
                    NUM_SOURCE: 0
                }

        source_data = {k.lower(): v for k, v in source_data.items()}
        target_data = {k.lower(): v for k, v in target_data.items()}

        # Create the final data only for those schemas where the number of objects in both source and target
        # are more than 0
        source_data_final = dict()
        target_data_final = dict()

        empty_schema_source = dict(zip(self.objects, [{OBJECTS_SOURCE: [], NUM_SOURCE: 0}] * len(self.objects)))
        empty_schema_target = dict(zip(self.objects, [{OBJECTS_TARGET: [], NUM_TARGET: 0}] * len(self.objects)))
        for schema_name in schema_level_counts:
            # if schema_level_counts[schema_name][NUM_TARGET] != schema_level_counts[schema_name][NUM_SOURCE]:
            source_data_final[schema_name.lower()] = source_data[schema_name] if schema_name in source_data \
                else empty_schema_source
            target_data_final[schema_name.lower()] = target_data[schema_name] if schema_name in target_data \
                else empty_schema_target

        return source_data_final, target_data_final

    def get_table_row_count_data(self):
        """
        Gets data for number of rows available in each of the tables
        """

        self.logger.info(f"Getting table row counts for source")
        if self.db_type_source == ORACLE:
            data_source = self.source_db.execute_query(
                GET_TABLE_ROW_COUNTS[self.db_type_source].format(self.source_db.username)
            )
            self.logger.info(data_source)
        else:
            data_source = self.source_db.execute_query(GET_TABLE_ROW_COUNTS[self.db_type_source])

        self.logger.info("Getting table row counts for target")
        if self.db_type_target == ORACLE:
            data_target = self.target_db.execute_query(
                GET_TABLE_ROW_COUNTS[self.db_type_target].format(self.target_db.username)
            )
        else:
            data_target = self.target_db.execute_query(GET_TABLE_ROW_COUNTS[self.db_type_target])

        # schema_name , table_name, row_count
        data_source_dict = {(x[SCHEMA_NAME], x[TABLE_NAME]): {ROW_COUNT: x[ROW_COUNT]} for x in data_source}
        data_target_dict = {(x[SCHEMA_NAME], x[TABLE_NAME]): {ROW_COUNT: x[ROW_COUNT]} for x in data_target}

        for key in data_source_dict:
            if key in data_target_dict:
                if data_source_dict[key] != data_target_dict[key]:
                    data_source_dict[key][COLOR] = COLOR_YELLOW
                else:
                    data_source_dict[key][COLOR] = COLOR_GREEN
            else:
                data_source_dict[key][COLOR] = COLOR_RED

        return data_source_dict, data_target_dict

    def prepare_comparison_data(self, source_data, target_data):
        source_schemas_lower = [x.lower() for x in list(source_data.keys())]
        target_schemas_lower = [x.lower() for x in list(target_data.keys())]

        all_schemas = list(set(source_schemas_lower).union(set(target_schemas_lower)))

        final_data = dict(zip(all_schemas, [{} for _ in range(len(all_schemas))]))

        lst = []
        for schema in all_schemas:
            schema_data = {}
            source_total_object_count = 0
            target_total_object_count = 0

            for obj in self.objects:
                empty_source = {OBJECTS_SOURCE: [''], NUM_SOURCE: 0}
                empty_target = {OBJECTS_TARGET: [''], NUM_TARGET: 0}

                source_data_ = source_data[schema][obj] if schema in source_data else empty_source
                target_data_ = target_data[schema][obj] if schema in target_data and obj in \
                                                           target_data[schema] else empty_target

                # Update totals:
                source_total_object_count += source_data_[NUM_SOURCE]
                target_total_object_count += target_data_[NUM_TARGET]

                source_data_set = set(source_data_[OBJECTS_SOURCE])
                target_data_set = set(target_data_[OBJECTS_TARGET])
                intersection_set = set(source_data_set).intersection(set(target_data_set))

                # Remove the objects that occur in both
                source_data_[OBJECTS_SOURCE] = list(source_data_set - intersection_set)
                target_data_[OBJECTS_TARGET] = list(target_data_set - intersection_set)

                source_data_[OBJECTS_SOURCE].sort()
                target_data_[OBJECTS_TARGET].sort()

                source_data_.update(target_data_)
                schema_data[obj] = source_data_
            final_data[schema][
                DISPLAY_FLAG] = True if source_total_object_count and target_total_object_count else False
            final_data[schema][ALL_ITEMS] = schema_data
            lst.append((schema, schema_data))

        return final_data

    @staticmethod
    def get_validation_data(source_data, target_data):
        """
        For each schema in the data:
            for each object:
                turn all object names to lower case first
        """

        validation_data = {
            SCHEMA: {
                MISSING_ITEMS: [],
                ALL_ITEMS: [],
                VALIDATION_PERCENT: 0.0,
                TOTAL_SOURCE: 0,
                TOTAL_TARGET: 0
            },
            OBJECTS: {

            }
        }

        # Schema validation
        source_schemas = set(x.lower() for x in list(source_data.keys()))
        target_schemas = set(x.lower() for x in list(target_data.keys()))

        missing_schemas = source_schemas - target_schemas
        validation_data[SCHEMA][MISSING_ITEMS] = list(missing_schemas)
        validation_data[SCHEMA][ALL_ITEMS] = list(source_schemas)
        validation_data[SCHEMA][VALIDATION_PERCENT] = (1 - (len(missing_schemas) / len(source_schemas))) * 100
        validation_data[SCHEMA][VALIDATION_PERCENT] = round(validation_data[SCHEMA][VALIDATION_PERCENT], 2)

        # Object validation
        for schema in source_data:
            validation_data[OBJECTS][schema] = {VALIDATION_PERCENT: 0}
            num_source = 0
            num_target = 0
            num_missing_objects = 0
            for obj in source_data[schema]:
                validation_data[OBJECTS][schema][obj] = dict()
                if schema in target_data and target_data[schema]:
                    # Get the data for the particular object type
                    source_objects = source_data[schema][obj][OBJECTS_SOURCE]
                    target_objects = target_data[schema][obj][OBJECTS_TARGET]

                    num_source += len(source_objects)
                    num_target += len(target_objects)

                    # Update overall counts for the schema

                    source_objects_set = set(x.lower() for x in source_objects)
                    target_objects_set = set(x.lower() for x in target_objects)

                    missing_objects = source_objects_set - target_objects_set
                    matches = source_objects_set.intersection(target_objects_set)

                    num_missing_objects += len(missing_objects)
                    validation_data[OBJECTS][schema][obj][MISSING_ITEMS] = list(missing_objects)
                    validation_data[OBJECTS][schema][obj][ALL_ITEMS] = list(source_objects)
                    validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT] = (
                                                                                        len(matches) / len(
                                                                                    source_objects)) * 100 if len(
                        source_objects) else 0.00

                    # If the validation percent is greater than 100, set it to 100 only
                    validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT] = 100.00 if \
                        validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT] > 100 else \
                        validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT]

                    validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT] = \
                        round(validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT], 2)

                    """
                    validation_data[OBJECTS][schema][obj][REASON] = ("Count on source was 0" if len(source_objects) == 0 else
                                                                     "Names didn't match") if len(
                        missing_objects) != 0 or len(source_objects) == 0 \
                        else ''

                    
                    if len(source_objects) == 0:
                        validation_data[OBJECTS][schema][obj][REASON] = "Count on source was 0"
                    elif len(target_objects) == 0:
                        validation_data[OBJECTS][schema][obj][REASON] = "Count on target was 0"
                    elif len(missing_objects) != 0 or len(source_objects) == 0:
                        validation_data[OBJECTS][schema][obj][REASON] = "Object names didn't match"

                    """
                    if len(source_objects) == 0 and len(target_objects) > 0:
                        reason = "Only present on target"
                    elif len(source_objects) > 0 and len(target_objects) == 0:
                        reason = "No objects were migrated"
                    elif not len(source_objects) and not len(target_objects):
                        reason = "No objects at source or target"
                    elif len(source_objects) == len(target_objects) and missing_objects:
                        reason = "Count matched, names didn't"
                    elif len(source_objects) != len(target_objects) and missing_objects:
                        reason = "Partially migrated"
                    else:
                        reason = "Counts and names matched"

                    validation_data[OBJECTS][schema][obj][REASON] = reason
                else:
                    source_objects = source_data[schema][obj][OBJECTS_SOURCE]
                    num_source += len(source_objects)

                    validation_data[OBJECTS][schema][obj][MISSING_ITEMS] = list(source_objects)
                    validation_data[OBJECTS][schema][obj][ALL_ITEMS] = []

                    validation_data[OBJECTS][schema][obj][VALIDATION_PERCENT] = 0.00
                    validation_data[OBJECTS][schema][obj][REASON] = 'Schema absent on destination'

            if not num_target and not num_source:
                validation_data[OBJECTS][schema][DISPLAY_FLAG] = False
            else:
                validation_data[OBJECTS][schema][DISPLAY_FLAG] = True

            missing_fraction = num_missing_objects / num_source if num_source else 1
            validation_data[OBJECTS][schema][VALIDATION_PERCENT] = (1 - missing_fraction) * 100 if \
                num_target and num_source else 'NA'

            if validation_data[OBJECTS][schema][VALIDATION_PERCENT] != 'NA':
                validation_data[OBJECTS][schema][VALIDATION_PERCENT] = 100.00 if \
                    validation_data[OBJECTS][schema][VALIDATION_PERCENT] > 100 else \
                    round(validation_data[OBJECTS][schema][VALIDATION_PERCENT], 2)
                validation_data[OBJECTS][schema][VALIDATION_PERCENT] = \
                    round(validation_data[OBJECTS][schema][VALIDATION_PERCENT], 2)

        return validation_data

    def get_database_details(self):
        """
        Get the details such as 
        1. Database version
        2. Schema sizes
        3. Encoding
        """
        self.database_summary = {
            "source": {
                "type": self.db_type_source.upper(),
                "name": self.source_db.database,
                "host": self.source_db.host,
                "version": self.source_db.execute_query(
                    GET_VERSION[self.db_type_source]
                )[0][VERSION],
                "database_size": self.source_db.execute_query(
                    GET_DB_SIZE[self.db_type_source].format(self.source_db.database)
                )[0][DATABASE_SIZE],
                "encoding": self.source_db.execute_query(
                    GET_ENCODING[self.db_type_source].format(self.source_db.database)
                )[0][ENCODING],
            },
            "target": {
                "type": self.db_type_target.upper(),
                "name": self.target_db.database,
                "host": self.target_db.host,
                "version": self.target_db.execute_query(
                    GET_VERSION[self.db_type_target]
                )[0][VERSION],
                "database_size": self.target_db.execute_query(
                    GET_DB_SIZE[self.db_type_target].format(self.target_db.database)
                )[0][DATABASE_SIZE],
                "encoding": self.target_db.execute_query(
                    GET_ENCODING[self.db_type_target]
                )[0][ENCODING],
            }
        }

    def get_schemas(self):
        # Get source schemas
        source_schemas = self.get_source_schemas()
        target_schemas = self.get_target_schemas()
        missing_on_target = list(set([x[SCHEMA_NAME] for x in source_schemas]) - set([x[SCHEMA_NAME] for x in target_schemas]))

        return source_schemas, target_schemas, missing_on_target

    def generate_report_data(self):
        start_time = datetime.now()
        self.logger.info(f"STARTED EXECUTION: {start_time}")

        source_row_count, target_row_count = self.get_table_row_count_data()
        self.combined_row_count_data = {
            SECTION_SOURCE: source_row_count,
            SECTION_TARGET: target_row_count
        }

        self.logger.info("\n*** Getting database details ***\n")
        self.get_database_details()

        self.logger.info("\n*** Getting data for all objects ***\n")

        source_schemas, target_schemas, self.missing_schemas = self.get_schemas()

        source_data, target_data = self.get_data(source_schemas, target_schemas)

        self.logger.info("\n**Getting validation data**\n")
        # Summary page
        self.validation_data = self.get_validation_data(source_data, target_data)

        self.comparison_data = self.prepare_comparison_data(source_data, target_data)

        self.logger.info(f"\nTOTAL TIME TAKEN: {datetime.now() - start_time}")