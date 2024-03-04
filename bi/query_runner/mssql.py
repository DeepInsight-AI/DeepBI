import logging
import sys
import uuid

from bi.query_runner import *
from bi.utils import json_dumps, json_loads
from bi.settings import WEB_LANGUAGE

logger = logging.getLogger(__name__)

try:
    import pymssql
    import pyodbc

    enabled = True
except ImportError:
    enabled = False
print("sqlserver enable", enabled)
# from _mssql.pyx ## DB-API type definitions & http://www.freetds.org/tds.html#types ##
types_map = {
    1: TYPE_STRING,
    2: TYPE_STRING,
    # Type #3 supposed to be an integer, but in some cases decimals are returned
    # with this type. To be on safe side, marking it as float.
    3: TYPE_FLOAT,
    4: TYPE_DATETIME,
    5: TYPE_FLOAT,
}


class SqlServer(BaseSQLQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        if 'CN' == WEB_LANGUAGE:
            return {
                "type": "object",
                "properties": {
                    "user": {"type": "string", "title": "用户名"},
                    "password": {"type": "string", "title": "密码"},
                    "server": {"type": "string", "title": "服务器", "default": "127.0.0.1"},
                    "port": {"type": "number", "title": "端口", "default": 1433},
                    "tds_version": {
                        "type": "string",
                        "default": "7.0",
                        "title": "TDS版本",
                    },
                    "charset": {
                        "type": "string",
                        "default": "UTF-8",
                        "title": "字符集",
                    },
                    "db": {"type": "string", "title": "数据库名称"},
                },
                "required": ["db"],
                "secret": ["password"],
            }
        else:
            return{
                "type": "object",
                "properties": {
                    "user": {"type": "string", "title": "User Name"},
                    "password": {"type": "string", "title": "Password"},
                    "server": {"type": "string", "title": "Server", "default": "127.0.0.1"},
                    "port": {"type": "number", "title": "Port", "default": 1433},
                    "tds_version": {
                        "type": "string",
                        "default": "7.0",
                        "title": "TDS Version",
                    },
                    "charset": {
                        "type": "string",
                        "default": "UTF-8",
                        "title": "Character Set",
                    },
                    "db": {"type": "string", "title": "Authentication Database"},
                },
                "required": ["db"],
                "secret": ["password"],
            }
        pass

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def name(cls):
        return "Microsoft SQL Server"

    @classmethod
    def type(cls):
        return "mssql"

    def test_connection(self):
        params = dict(
            user=self.configuration.get("user", ""),
            password=self.configuration.get("password", ""),
            server=self.configuration.get("server", "127.0.0.1"),
            port=self.configuration.get("port", 1433),
            tds_version=self.configuration.get("tds_version","7.0"),
            charset=self.configuration.get("charset", "utf8"),
            database=self.configuration["db"],
        )
        
        try:
            conn = pymssql.connect(**params)
            cursor = conn.cursor()
            print("Successfully connected to the SQL Server database!")
#            cursor.execute("SELECT @@VERSION")  查询版本信息
#            version = cursor.fetchone()[0]
#            print("Server version:", version)
            return conn
        except pymssql.Error as e:
            print("Failed to connect to the SQL Server database.")
            print("Error:", e)
            return None
        

    def get_schema(self, get_stats=False):
       
        query = """
        SELECT 
            c.TABLE_SCHEMA AS table_schema,
            c.TABLE_NAME AS table_name,
            c.COLUMN_NAME AS column_name,
            ep.value AS column_comment
        FROM 
            INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN 
            sys.extended_properties ep ON ep.major_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
                                        AND ep.minor_id = c.ORDINAL_POSITION
        WHERE 
            c.TABLE_SCHEMA NOT IN ('guest','INFORMATION_SCHEMA','sys','db_owner','db_accessadmin',
                                   'db_securityadmin','db_ddladmin','db_backupoperator','db_datareader',
                                   'db_datawriter','db_denydatareader','db_denydatawriter')

        """
        params = dict(
            user=self.configuration.get("user", ""),
            password=self.configuration.get("password", ""),
            server=self.configuration.get("server", "127.0.0.1"),
            port=self.configuration.get("port", 1433),
            tds_version=self.configuration.get("tds_version","7.0"),
            charset=self.configuration.get("charset", "utf8"),
            database=self.configuration["db"],
        )
        connection = pymssql.connect(**params)
        
#       if isinstance(query, str):
#           query = query.encode(charset)

        cursor = connection.cursor()


        cursor.execute(query)
        data = cursor.fetchall()
 #      print(data)
        schema = {}
        for item in data:
 #          print(item[0],item[1],item[2],item[3])
            if item[1] not in schema:
               schema[item[1]] = {"name": item[1], "columns": [], 'comment': []}
                
            schema[item[1]]["columns"].append(item[2])
            schema[item[1]]["comment"].append(item[3])
            pass
        return list(schema.values())
   
    
   
    def run_query(self, query, user):
        connection = None

        try:
            server = self.configuration.get("server", "")
            user = self.configuration.get("user", "")
            password = self.configuration.get("password", "")
            db = self.configuration["db"]
            port = self.configuration.get("port", 1433)
            tds_version = self.configuration.get("tds_version", "7.0")
            charset = self.configuration.get("charset", "UTF-8")

            if port != 1433:
                server = server + ":" + str(port)

            connection = pymssql.connect(
                server=server,
                user=user,
                password=password,
                database=db,
                tds_version=tds_version,
                charset=charset,
            )

            if isinstance(query, str):
                query = query.encode(charset)

            cursor = connection.cursor()
            logger.debug("SqlServer running query: %s", query)

            cursor.execute(query)
            data = cursor.fetchall()

            if cursor.description is not None:
                columns = self.fetch_columns(
                    [(i[0], types_map.get(i[1], None)) for i in cursor.description]
                )
                rows = [
                    dict(zip((column["name"] for column in columns), row))
                    for row in data
                ]

                data = {"columns": columns, "rows": rows}
                json_data = json_dumps(data)
                error = None
            else:
                error = "No data was returned."
                json_data = None

            cursor.close()
        except pymssql.Error as e:
            print(str(e))
            try:
                # Query errors are at `args[1]`
                error = e.args[1]
            except IndexError:
                # Connection errors are `args[0][1]`
                error = e.args[0][1]
            json_data = None
        except (KeyboardInterrupt, JobTimeoutException):
            connection.cancel()
            raise
        finally:
            if connection:
                connection.close()

        return json_data, error


register(SqlServer)

