# coding = utf-8
import os
import json
from bi import settings


def get_config_language():
    LANG_FILE = os.getcwd() + '/bi/templates/Language.' + settings.WEB_LANGUAGE + ".json"
    with open(LANG_FILE, 'r') as f:
        data = json.load(f)
    return data
    pass
