# -*- coding: utf-8 -*-

from configparser import ConfigParser

__CONFIGFILE="./service.config"

def get_hostname():
    new_parser = ConfigParser()
    new_parser.read(__CONFIGFILE)
    return new_parser.get("config", "HOST")

def get_hostcache():
    new_parser = ConfigParser()
    new_parser.read(__CONFIGFILE)
    return new_parser.get("config", "HOSTCACHE")

def get_port():
    new_parser = ConfigParser()
    new_parser.read(__CONFIGFILE)
    return new_parser.get("config", "PORT")

def get_index():
    new_parser = ConfigParser()
    new_parser.read(__CONFIGFILE)
    return new_parser.get("config", "INDEX")

def get_use_uc_conversion():
    new_parser = ConfigParser()
    new_parser.read(__CONFIGFILE)
    return new_parser.get("config", "UC_CONVERSION")

def get_secret():
    new_parser = ConfigParser()
    new_parser.read(__CONFIGFILE)
    return new_parser.get("config", "SECRET")

