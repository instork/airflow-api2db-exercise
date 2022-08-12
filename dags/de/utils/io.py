import os
import json
from decimal import Decimal
import yaml
import re

from de.utils.logger import getLogger, INFO, WARNING, ERROR, MORE, DETAIL

logger = getLogger()


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
    else:
        logger.logc(ERROR, f"{file_name} does not exist !!")
        return None


def read_yaml(path=None, data=None, tag='!ENV'):
    """
    Load a yaml configuration file and resolve any environment variables
    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}.
    E.g.:
    database:
        host: !ENV ${HOST}
        port: !ENV ${PORT}
    app:
        log_path: !ENV '/var/${LOG_PATH}'
        something_else: !ENV '${AWESOME_ENV_VAR}/var/${A_SECOND_AWESOME_VAR}'
    :param str path: the path to the yaml file
    :param str data: the yaml data itself as a stream
    :param str tag: the tag to look for
    :return: the dict configuration
    :rtype: dict[str, T]
    """
    # pattern for global vars: look for ${word}
    pattern = re.compile('.*?\${(\w+)}.*?')
    loader = yaml.SafeLoader

    # the tag will be used to mark where to start searching for the pattern
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader, node):
        """
        Extracts the environment variable from the node's value
        :param yaml.Loader loader: the yaml loader
        :param node: the current node in the yaml
        :return: the parsed string that contains the value of the environment
        variable
        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    f'${{{g}}}', os.environ.get(g, None)
                )
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    if path:
        with open(path) as conf_data:
            return yaml.load(conf_data, Loader=loader)
    elif data:
        return yaml.load(data, Loader=loader)
    else:
        return None
