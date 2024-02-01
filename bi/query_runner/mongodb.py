# -*- coding: utf-8 -*-
# step1
import logging
from bi.query_runner import *

logger = logging.getLogger(__name__)
# step3
from bi.settings import WEB_LANGUAGE
from bi.utils import json_loads, parse_human_time  # step 6
from dateutil.parser import parse  # step 6
from bi.utils import json_dumps  # step 6
from bi.utils import JSONEncoder  # step 6
import re  # step 6
import datetime

# step2
try:
    # step2
    import pymongo

    from bson.objectid import ObjectId  # step 6
    from bson.timestamp import Timestamp  # step 6
    from bson.decimal128 import Decimal128  # step 6
    from bson.son import SON  # step 6
    from bson.json_util import object_hook as bson_object_hook  # STEP 6

    enabled = True

except ImportError:
    enabled = False

date_regex = re.compile('ISODate\("(.*)"\)', re.IGNORECASE)
# def 6-f2-D1
TYPES_MAP = {
    str: TYPE_STRING,
    bytes: TYPE_STRING,
    int: TYPE_INTEGER,
    float: TYPE_FLOAT,
    bool: TYPE_BOOLEAN,
    datetime.datetime: TYPE_DATETIME,
}


# def function 6-f1
def parse_query_json(query):
    query_data = json_loads(query, object_hook=datetime_parser)
    return query_data


# def function 6-f1-f1
def datetime_parser(dct):
    for k, v in dct.items():
        if isinstance(v, str):
            m = date_regex.findall(v)
            if len(m) > 0:
                dct[k] = parse(m[0], yearfirst=True)

    if "$humanTime" in dct:
        return parse_human_time(dct["$humanTime"])

    if "$oids" in dct:
        return parse_oids(dct["$oids"])

    return bson_object_hook(dct)


# def function 6-f1-f1-f1
def parse_oids(oids):
    if not isinstance(oids, list):
        raise Exception("$oids takes an array as input.")

    return [bson_object_hook({"$oid": oid}) for oid in oids]


# def 6-f2
def parse_results(results):
    rows = []
    columns = []

    for row in results:
        parsed_row = {}

        for key in row:
            if isinstance(row[key], dict):
                for inner_key in row[key]:
                    column_name = "{}.{}".format(key, inner_key)
                    if _get_column_by_name(columns, column_name) is None:
                        columns.append(
                            {
                                "name": column_name,
                                "friendly_name": column_name,
                                "type": TYPES_MAP.get(
                                    type(row[key][inner_key]), TYPE_STRING
                                ),
                            }
                        )

                    parsed_row[column_name] = row[key][inner_key]

            else:
                if _get_column_by_name(columns, key) is None:
                    columns.append(
                        {
                            "name": key,
                            "friendly_name": key,
                            "type": TYPES_MAP.get(type(row[key]), TYPE_STRING),
                        }
                    )

                parsed_row[key] = row[key]

        rows.append(parsed_row)

    return rows, columns


# def 6-f2-f1
def _get_column_by_name(columns, column_name):
    for c in columns:
        if "name" in c and c["name"] == column_name:
            return c

    return None


# def 6-f3
class MongoDBJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, Timestamp):
            return super(MongoDBJSONEncoder, self).default(o.as_datetime())
        elif isinstance(o, Decimal128):
            return o.to_decimal()
        return super(MongoDBJSONEncoder, self).default(o)


# step1
class MongoDB(BaseQueryRunner):
    # init method
    def __init__(self, configuration):
        self.should_annotate_query = False  # Whether to display comments in sql
        # step 1-1
        super(MongoDB, self).__init__(configuration)
        # step 1-2
        self.db_name = self.configuration["dbName"]
        # step
        self.syntax = "json"
        # step 4-1
        self.is_replica_set = (
            True
            if "replicaSetName" in self.configuration
               and self.configuration["replicaSetName"]
            else False
        )

    # step 1
    @classmethod
    def name(cls):
        return "MongoDB"

    # step 2
    @classmethod
    def enabled(cls):
        return enabled

    @classmethod  # step3
    def configuration_schema(cls):
        # base on WEB_LANGUAGE
        if 'CN' == WEB_LANGUAGE:
            return {
                "type": "object",
                "properties": {
                    "connectionString": {"type": "string", "title": "连接字符串", "default": 'mongodb://127.0.0.1:27017'},
                    "username": {"type": "string", "title": "用户名"},
                    "password": {"type": "string", "title": "密码"},
                    "dbName": {"type": "string", "title": "数据库名称"},
                    "replicaSetName": {"type": "string", "title": "副本集名称"},
                },
                "secret": ["password"],
                "required": ["connectionString", "dbName"],
            }
        else:
            return {
                "type": "object",
                "properties": {
                    "connectionString": {"type": "string", "title": "Connection name"},
                    "username": {"type": "string", "title": "User Name"},
                    "password": {"type": "string", "title": "Password"},
                    "dbName": {"type": "string", "title": "Authentication Database"},
                    "replicaSetName": {"type": "string", "title": "Replica Set Name"},
                },
                "secret": ["password"],  # this mark secret
                "required": ["connectionString", "dbName"],  # mast input
            }
        pass

    # step4
    def test_connection(self):
        db = self._get_db()
        if not db.command("connectionStatus")["ok"]:
            raise Exception("MongoDB connection error")

        return db

    # step 4-2 The method being called， return connection obj
    def _get_db(self):
        kwargs = {}
        if self.is_replica_set:
            kwargs["replicaSet"] = self.configuration["replicaSetName"]

        if "username" in self.configuration:
            kwargs["username"] = self.configuration["username"]

        if "password" in self.configuration:
            kwargs["password"] = self.configuration["password"]

        db_connection = pymongo.MongoClient(
            self.configuration["connectionString"] + "/" + self.db_name, **kwargs
        )
        # return connect obj
        return db_connection[self.db_name]

    # step 5 get schema
    def get_schema(self, get_stats=False):
        schema = {}
        db = self._get_db()  # get connect obj,
        for collection_name in db.list_collection_names():
            if collection_name.startswith("system."):
                continue
            columns = self._get_collection_fields(db, collection_name)
            if columns:
                schema[collection_name] = {
                    "name": collection_name,
                    "columns": sorted(columns),
                    "comment": sorted(columns)
                }

        return list(schema.values())

    # step 5-1 The method being called， return connection obj
    def _get_collection_fields(self, db, collection_name):
        # Since MongoDB is a document based database and each document doesn't have
        # to have the same fields as another documet in the collection its a bit hard to
        # show these attributes as fields in the schema.
        #
        # For now, the logic is to take the first and last documents (last is determined
        # by the Natural Order (http://www.mongodb.org/display/DOCS/Sorting+and+Natural+Order)
        # as we don't know the correct order. In most single server installations it would be
        # fine. In replicaset when reading from non master it might not return the really last
        # document written.
        collection_is_a_view = self._is_collection_a_view(db, collection_name)
        documents_sample = []
        try:
            if collection_is_a_view:
                for d in db[collection_name].find().limit(2):
                    documents_sample.append(d)
            else:
                for d in db[collection_name].find().sort([("$natural", 1)]).limit(1):
                    documents_sample.append(d)

                for d in db[collection_name].find().sort([("$natural", -1)]).limit(1):
                    documents_sample.append(d)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.error(message)
            return []
        columns = []
        for d in documents_sample:
            self._merge_property_names(columns, d)
        return columns

    # STEP 5-1-1
    def _is_collection_a_view(self, db, collection_name):
        if "viewOn" in db[collection_name].options():
            return True
        else:
            return False

    # STEP 5-1-2
    def _merge_property_names(self, columns, document):
        for property in document:
            if property not in columns:
                columns.append(property)

    # STEP 6
    def run_query(self, query, user):
        """
        {"collection": "users", "fields": {"_id": 1, "name": 2}}
        """
        db = self._get_db()
        logger.debug(
            "mongodb connection string: %s", self.configuration["connectionString"]
        )
        logger.debug("mongodb got query: %s", query)

        try:
            query_data = parse_query_json(query)  # call 6-1
        except ValueError:
            return None, "Invalid query format. The query is not a valid JSON."

        if "collection" not in query_data:
            return None, "'collection' must have a value to run a query"
        else:
            collection = query_data["collection"]

        q = query_data.get("query", None)
        f = None

        aggregate = query_data.get("aggregate", None)
        if aggregate:
            for step in aggregate:
                if "$sort" in step:
                    sort_list = []
                    for sort_item in step["$sort"]:
                        sort_list.append((sort_item["name"], sort_item["direction"]))

                    step["$sort"] = SON(sort_list)

        if "fields" in query_data:
            f = query_data["fields"]

        s = None
        if "sort" in query_data and query_data["sort"]:
            s = []
            for field_data in query_data["sort"]:
                s.append((field_data["name"], field_data["direction"]))

        columns = []
        rows = []

        cursor = None
        if q or (not q and not aggregate):
            if s:
                cursor = db[collection].find(q, f).sort(s)
            else:
                cursor = db[collection].find(q, f)

            if "skip" in query_data:
                cursor = cursor.skip(query_data["skip"])

            if "limit" in query_data:
                cursor = cursor.limit(query_data["limit"])

            if "count" in query_data:
                cursor = len(list(cursor))

        elif aggregate:
            allow_disk_use = query_data.get("allowDiskUse", False)
            r = db[collection].aggregate(aggregate, allowDiskUse=allow_disk_use)

            # Backwards compatibility with older pymongo versions.
            #
            # Older pymongo version would return a dictionary from an aggregate command.
            # The dict would contain a "result" key which would hold the cursor.
            # Newer ones return pymongo.command_cursor.CommandCursor.
            if isinstance(r, dict):
                cursor = r["result"]
            else:
                cursor = r

        if "count" in query_data:
            columns.append(
                {"name": "count", "friendly_name": "count", "type": TYPE_INTEGER}
            )

            rows.append({"count": cursor})
        else:
            rows, columns = parse_results(cursor)

        if f:
            ordered_columns = []
            for k in sorted(f, key=f.get):
                column = _get_column_by_name(columns, k)
                if column:
                    ordered_columns.append(column)

            columns = ordered_columns

        if query_data.get("sortColumns"):
            reverse = query_data["sortColumns"] == "desc"
            columns = sorted(columns, key=lambda col: col["name"], reverse=reverse)

        data = {"columns": columns, "rows": rows}
        error = None
        json_data = json_dumps(data, cls=MongoDBJSONEncoder)

        return json_data, error


register(MongoDB)
