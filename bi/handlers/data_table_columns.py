from flask import request

from bi.permissions import require_admin, require_permission
from bi import models
from bi.handlers.base import BaseResource, get_object_or_404, paginate
from sqlalchemy.orm.exc import NoResultFound
from bi.utils import json_dumps


class QueryDataTableColumns(BaseResource):
    @require_admin
    def get(self, data_source_id, table_name):
        try:
            result = models.DataTableColumns.table_info(data_source_id, table_name)
        except NoResultFound:
            result = None
        # todo  add right
        if result:
            return result.to_dict()
        else:
            return {}

    @require_admin
    def post(self, data_source_id, table_name):
        # check have
        table_desc = request.json['table_desc']
        table_inuse = request.json['table_inuse']
        table_columns_info = json_dumps(request.json['table_columns_info'])
        try:
            result = models.DataTableColumns.table_info(data_source_id, table_name)
        except NoResultFound:
            result = None
        # have, update row info
        if result:
            save_data = {'table_inuse': table_inuse, 'table_desc': table_desc, 'table_columns_info': table_columns_info}
            self.update_model(result, save_data)
            models.db.session.commit()
            pass
        # add new rows
        else:
            result = models.DataTableColumns(
                table_name=table_name,
                table_desc=table_desc,
                table_inuse=table_inuse,
                table_columns_info=table_columns_info,
                data_source_id=data_source_id,
                org_id=self.current_org.id
            )
            models.db.session.add(result)
            models.db.session.commit()
            pass
        return result.to_dict()
        pass
