import React from "react";
import PropTypes from "prop-types";
import Card from "antd/lib/card";
import WarningFilledIcon from "@ant-design/icons/WarningFilled";
import Typography from "antd/lib/typography";
import Link from "@/components/Link";
import DynamicComponent from "@/components/DynamicComponent";
import { currentUser } from "@/services/auth";

import useQueryFlags from "../hooks/useQueryFlags";
import "./QuerySourceAlerts.less";

export default function QuerySourceAlerts({ query, dataSourcesAvailable }) {
  const queryFlags = useQueryFlags(query); // we don't use flags that depend on data source

  let message = null;
  if (queryFlags.isNew && !queryFlags.canCreate) {
    message = (
      <React.Fragment>
        <Typography.Title level={4}>
          {window.W_L.no_datasource_cant_create_query}
        </Typography.Title>
        <p>
          <Typography.Text type="secondary">
            {window.W_L.apply_permission}
          </Typography.Text>
        </p>
      </React.Fragment>
    );
  } else if (!dataSourcesAvailable) {
    if (currentUser.isAdmin) {
      message = (
        <React.Fragment>
          <Typography.Title level={4}>
            {window.W_L.no_datasource_or_permission}
          </Typography.Title>
          <p>
            <Typography.Text type="secondary">{window.W_L.first_create_datasource}</Typography.Text>
          </p>

          <div className="query-source-alerts-actions">
            <Link.Button type="primary" href="data_sources/new">
              {window.W_L.add_datasource}
            </Link.Button>
            <Link.Button type="default" href="groups">
              {window.W_L.mange_permissions}
            </Link.Button>
          </div>
        </React.Fragment>
      );
    } else {
      message = (
        <React.Fragment>
          <Typography.Title level={4}>
          {window.W_L.no_datasource_or_permission}
          </Typography.Title>
          <p>
            <Typography.Text type="secondary">{window.W_L.first_create_datasource}</Typography.Text>
          </p>
        </React.Fragment>
      );
    }
  }

  if (!message) {
    return null;
  }

  return (
    <div className="query-source-alerts">
      <Card>
        <DynamicComponent name="QuerySource.Alerts" query={query} dataSourcesAvailable={dataSourcesAvailable}>
          <div className="query-source-alerts-icon">
            <WarningFilledIcon />
          </div>
          {message}
        </DynamicComponent>
      </Card>
    </div>
  );
}

QuerySourceAlerts.propTypes = {
  query: PropTypes.object.isRequired,
  dataSourcesAvailable: PropTypes.bool,
};

QuerySourceAlerts.defaultProps = {
  dataSourcesAvailable: false,
};
