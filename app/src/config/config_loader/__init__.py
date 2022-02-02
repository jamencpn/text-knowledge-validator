# coding=utf-8
from os import getcwd
from os.path import join, dirname
import sys

from yaml import safe_load as load, YAMLError

sys.path.append(getcwd())


class MetaConfigConnection(type):

    def __int__(self):
        self.mongodb = None

    @property
    def MONGODB(self):
        return self.mongodb


class ConfigConnection(metaclass=MetaConfigConnection):
    path_config_file = join(dirname(getcwd()), "src/config/connection_config.yaml")
    with open(path_config_file, 'r') as stream:
        try:
            config_parser = load(stream)
        except YAMLError as error:
            print(f"Can't load config.yaml Error:{error}")

    # MongoDB
    mongodb = config_parser["MONGODB"]


if __name__ == '__main__':
    print(join(dirname(getcwd()), "config/connection_config.yaml"))
