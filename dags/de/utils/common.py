import os
import sys
import socket


class AppInfo:
    """
        A class to manage the application name of the module to be executed
    """
    __app_dir = None
    __app_full_name = None

    def __init__(self, app_dir=None, app_full_name=None):
        self.app_dir(app_dir)
        self.app_full_name(app_full_name)

    @staticmethod
    def current_dir():
        return os.getcwd()

    def app_dir(self, app_dir=None):
        """
        Path of the application to run
        :param app_dir:
        :return:
        """
        if app_dir:
            AppInfo.__app_dir = app_dir
        # class 변수 app_dir 안에 있으면 그대로 반환
        elif AppInfo.__app_dir:
            return AppInfo.__app_dir
        elif sys.argv[0].endswith("pytest"):
            return self.current_dir()
        else:
            return os.getcwd()

    @staticmethod
    def app_full_name(app_full_name=None):
        """
            Extract the application app full path name
                running in argv[0]
        :param app_full_name:
            If you are forced to enter app_full_name ,
            enter full_path_app_name
        :return:
            app_full_name (string)
        """
        if app_full_name:
            AppInfo.__app_full_name = app_full_name
        elif AppInfo.__app_full_name:
            return AppInfo.__app_full_name
        else:
            return os.path.basename(sys.argv[0])

    def app_name(self):
        """
            application name from argv[0]
            If you run app.py script from the command, app is extracted
            ex) python app.py -> return app
        :return:
            app_name
        """
        return self.app_full_name().split(".")[0]  # app.py -> app 추출


def del_dict_value_is_none(_dict: dict):
    """
        A function that removes the keys of those whose
         value is None in the dictionary.
         If value is a dictionary, search inside again and remove
            only the key whose value is none.
    :param _dict:
            The dictionary object from which you want to remove the key
    :example
        ex1) del_dict_value_is_none({"A": "a", "B": None}) -> {'A': 'a'}
        ex2) del_dict_value_is_none({"A": "a", "B": {"C": None, "D": "d"}}) -> {'A': 'a', 'B': {'D': 'd'}}
    :return:
        dictionary object
    """
    if isinstance(_dict, dict):
        for key, value in list(_dict.items()):
            if isinstance(value, dict):
                del_dict_value_is_none(value)
            elif value is None:
                _dict.pop(key)
        return _dict
    else:
        return {}


class MultiDictContainer(dict):
    """
        다중 딕셔너리를 지원하는 container 를 생성하는 클래스
        a = MultiDictContainer()
        a.addkey('bb').addkey('cc')['dddd'] = 1
        a.addkey('bb').addkey('ccc').dddd = 1
    """
    def __init__(self, _dict=None):
        if _dict is None:
            _dict = {}
        super(MultiDictContainer, self).__init__(_dict)
        # value 가 dictionary 인 경우 MultiDictContainer object 로 변환
        for key in self:
            item = self[key]
            if isinstance(item, list):
                for idx, it in enumerate(item):
                    if isinstance(it, dict):
                        item[idx] = MultiDictContainer(it)
            elif isinstance(item, dict):
                self[key] = MultiDictContainer(item)

    def add_key(self, key):
        """
            A function to add a key to an object.
            The value of key is a MultiDictContainer object.
        :param key:
            key is key!
        """
        if key not in self:
            self.__setattr__(key, MultiDictContainer())

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def merge(self, _dict):
        """
            A function that merges the dict into a MultiDictContainer object.
            - Add key-value that does not exist
            - The existing key is updated with a new value(**)
            - If value is a list, replace the existing list
        :param _dict:
        :return:
        """
        self = _add_update_obj(self, _dict)


def _add_update_obj(dict_org, _dict):
    """
        - Add key-value that does not exist
        - The existing key is updated with a new value(**)
        - If value is a list, replace the existing list
    :param dict_org:
    :param _dict:
    :return:
    """
    if isinstance(_dict, dict):
        for key in _dict:
            if key in dict_org:   # 기존 object 에 해당 key 가 있는 경우,
                item = _dict[key]
                if isinstance(item, dict):
                    dict_org[key] = _add_update_obj(dict_org[item], item)
                else:
                    dict_org[key] = item
    return dict_org


def host_name():
    """
        call the host name of the execution environment
    :return:
        host name(string)
    """
    return socket.gethostname()
