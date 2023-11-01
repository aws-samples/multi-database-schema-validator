from database import *
from src.utility.utils import CommonUtility


class Template:
    def __init__(self):
        self.db_name_source = CommonUtility.read_configurations(SOURCE_DATABASE_NAME, CONFIG_FILE, SECTION_SOURCE)
        self.db_name_target = CommonUtility.read_configurations(TARGET_DATABASE_NAME, CONFIG_FILE, SECTION_TARGET)

        self.db_type_source = CommonUtility.read_configurations(SOURCE_DATABASE_TYPE, CONFIG_FILE, SECTION_SOURCE)
        self.db_type_target = CommonUtility.read_configurations(TARGET_DATABASE_TYPE, CONFIG_FILE, SECTION_TARGET)

        self.db_host_source = CommonUtility.read_configurations(SOURCE_HOST, CONFIG_FILE, SECTION_SOURCE)
        self.db_host_target = CommonUtility.read_configurations(TARGET_HOST, CONFIG_FILE, SECTION_TARGET)

    def create_report(self, *args):
        """
        To be implemented by child
        """
        pass
