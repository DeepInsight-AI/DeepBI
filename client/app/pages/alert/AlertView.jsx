import React from "react";
import PropTypes from "prop-types";
import cx from "classnames";

import Link from "@/components/Link";
import TimeAgo from "@/components/TimeAgo";
import { Alert as AlertType } from "@/components/proptypes";

import Form from "antd/lib/form";
import Button from "antd/lib/button";
import Tooltip from "@/components/Tooltip";
import AntAlert from "antd/lib/alert";
import * as Grid from "antd/lib/grid";

import Title from "./components/Title";
import Criteria from "./components/Criteria";
import Rearm from "./components/Rearm";
import Query from "./components/Query";
import AlertDestinations from "./components/AlertDestinations";
import HorizontalFormItem from "./components/HorizontalFormItem";
import { STATE_CLASS } from "../alerts/AlertsList";
import DynamicComponent from "@/components/DynamicComponent";

function AlertState({ state, lastTriggered }) {
  return (
    <div className="alert-state">
      <span className={`alert-state-indicator label ${STATE_CLASS[state]}`}>{window.W_L.status}: {state}</span>
      {state === "unknown" && <div className="ant-form-item-explain">{window.W_L.alert_setting_sure}</div>}
      {lastTriggered && (
        <div className="ant-form-item-explain">
          {window.W_L.last_trigger_time} {" "}
          <span className="alert-last-triggered">
            <TimeAgo date={lastTriggered} />
          </span>
        </div>
      )}
    </div>
  );
}

AlertState.propTypes = {
  state: PropTypes.string.isRequired,
  lastTriggered: PropTypes.string,
};

AlertState.defaultProps = {
  lastTriggered: null,
};

// eslint-disable-next-line react/prefer-stateless-function
export default class AlertView extends React.Component {
  state = {
    unmuting: false,
  };

  unmute = () => {
    this.setState({ unmuting: true });
    this.props.unmute().finally(() => {
      this.setState({ unmuting: false });
    });
  };

  render() {
    const { alert, queryResult, canEdit, onEdit, menuButton } = this.props;
    const { query, name, options, rearm } = alert;

    return (
      <>
        <Title name={name} alert={alert}>
          <DynamicComponent name="AlertView.HeaderExtra" alert={alert} />
          <Tooltip title={canEdit ? "" :window.W_L.no_permission_edit_alert}>
            <Button type="default" onClick={canEdit ? onEdit : null} className={cx({ disabled: !canEdit })}>
              <i className="fa fa-edit m-r-5" aria-hidden="true" />
              {window.W_L.edit}
            </Button>
            {menuButton}
          </Tooltip>
        </Title>
        <div className="bg-white tiled p-20">
          <Grid.Row type="flex" gutter={16}>
            <Grid.Col xs={24} md={16} className="d-flex">
              <Form className="flex-fill">
                <HorizontalFormItem>
                  <AlertState state={alert.state} lastTriggered={alert.last_triggered_at} />
                </HorizontalFormItem>
                <HorizontalFormItem label={window.W_L.query}>
                  <Query query={query} queryResult={queryResult} />
                </HorizontalFormItem>
                {queryResult && options && (
                  <>
                    <HorizontalFormItem label={window.W_L.trigger_condition} className="alert-criteria">
                      <Criteria
                        columnNames={queryResult.getColumnNames()}
                        resultValues={queryResult.getData()}
                        alertOptions={options}
                      />
                    </HorizontalFormItem>
                    <HorizontalFormItem label={window.W_L.notice} className="form-item-line-height-normal">
                      <Rearm value={rearm || 0} />
                      <br />
                      {window.W_L.set} {options.custom_subject || options.custom_body ? window.W_L.custom : window.W_L.default} {window.W_L.notice_template}
                    </HorizontalFormItem>
                  </>
                )}
              </Form>
            </Grid.Col>
            <Grid.Col xs={24} md={8}>
              {options.muted && (
                <AntAlert
                  className="m-b-20"
                  message={
                    <>
                      <i className="fa fa-bell-slash-o" aria-hidden="true" /> {window.W_L.set_mute}
                    </>
                  }
                  description={
                    <>
                      {window.W_L.alert_not_sent_info}
                      <br />
                      {canEdit && (
                        <>
                          {window.W_L.reset_alert_sent_info}
                          <Button
                            size="small"
                            type="primary"
                            onClick={this.unmute}
                            loading={this.state.unmuting}
                            className="m-t-5 m-l-5">
                            {window.W_L.set_unmute}
                          </Button>
                        </>
                      )}
                    </>
                  }
                  type="warning"
                />
              )}
              <h4>
                {window.W_L.destination}{" "}
                <Tooltip title={window.W_L.set_alert_open_tab}>
                  <Link href="destinations" target="_blank">
                    <i className="fa fa-external-link f-13" aria-hidden="true" />
                    <span className="sr-only">(opens in a new tab)</span>
                  </Link>
                </Tooltip>
              </h4>
              <AlertDestinations alertId={alert.id} />
            </Grid.Col>
          </Grid.Row>
        </div>
      </>
    );
  }
}

AlertView.propTypes = {
  alert: AlertType.isRequired,
  queryResult: PropTypes.object, // eslint-disable-line react/forbid-prop-types,
  canEdit: PropTypes.bool.isRequired,
  onEdit: PropTypes.func.isRequired,
  menuButton: PropTypes.node.isRequired,
  unmute: PropTypes.func,
};

AlertView.defaultProps = {
  queryResult: null,
  unmute: null,
};
