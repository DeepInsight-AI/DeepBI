import { includes } from "lodash";
import React from "react";
import PropTypes from "prop-types";
import Alert from "antd/lib/alert";
import Button from "antd/lib/button";
import Timer from "@/components/Timer";

export default function QueryExecutionStatus({ status, updatedAt, error, isCancelling, onCancel }) {
  const alertType = status === "failed" ? "error" : "info";
  const showTimer = status !== "failed" && updatedAt;
  const isCancelButtonAvailable = includes(["waiting", "processing"], status);
  let message = isCancelling ? <React.Fragment>Cancelling&hellip;</React.Fragment> : null;

  switch (status) {
    case "waiting":
      if (!isCancelling) {
        message = <React.Fragment>{window.W_L.query_queue}&hellip;</React.Fragment>;
      }
      break;
    case "processing":
      if (!isCancelling) {
        message = <React.Fragment>{window.W_L.exec_query}&hellip;</React.Fragment>;
      }
      break;
    case "loading-result":
      message = <React.Fragment>{window.W_L.loading_query_result}&hellip;</React.Fragment>;
      break;
    case "failed":
      message = (
        <React.Fragment>
          {window.W_L.query_run_error}：<strong>{error}</strong>
        </React.Fragment>
      );
      break;
    // no default
  }

  return (
    <Alert
      data-test="QueryExecutionStatus"
      type={alertType}
      message={
        <div className="d-flex align-items-center">
          <div className="flex-fill p-t-5 p-b-5">
            {message} {showTimer && <Timer from={updatedAt} />}
          </div>
          <div>
            {isCancelButtonAvailable && (
              <Button className="m-l-10" type="primary" size="small" disabled={isCancelling} onClick={onCancel}>
                {window.W_L.cancel}
              </Button>
            )}
          </div>
        </div>
      }
    />
  );
}

QueryExecutionStatus.propTypes = {
  status: PropTypes.string,
  updatedAt: PropTypes.any,
  error: PropTypes.string,
  isCancelling: PropTypes.bool,
  onCancel: PropTypes.func,
};

QueryExecutionStatus.defaultProps = {
  status: "waiting",
  updatedAt: null,
  error: null,
  isCancelling: true,
  onCancel: () => {},
};
