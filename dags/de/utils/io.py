import os
import json
from decimal import Decimal
import yaml
import re

from de.utils.logger import get_logger, INFO, WARNING, ERROR, MORE, DETAIL

logger = get_logger()


def read_file(file_name, data_type="string"):
    """
        Read a file and return it in various formats
        format refers to the format of the input file.
        For example, string, JSON, YAML file formats
        Returns None if there is no file in that path.
    :param file_name:
            The name of the file to read, and it must be a file name including full_path
    :param data_type:
            Must be one of string, json, or yaml
    :return:
            string or dictionary object
    file_name = '/Users/heojaehun/IdeaProjects/airflow-api2db-exercise/dags/default_properties/conn_properties.yaml'
    data_type = "yaml"
    """
    if os.path.exists(file_name):
        with open(file_name, "r", encoding="utf-8") as f:
            if data_type == "string":
                return f.read()
            elif data_type == "json":
                return json.loads(f.read(), parse_float=Decimal)
            elif data_type == "yaml":
                return yaml.safe_load(f)
    else:
        logger.logc(ERROR, f"{file_name} does not exist !!")
        return None
