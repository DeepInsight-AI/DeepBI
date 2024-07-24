import React, { useState, useMemo, useCallback } from "react";
import PropTypes from "prop-types";
import cx from "classnames";
import { find, orderBy } from "lodash";
import useMedia from "use-media";
import Tabs from "antd/lib/tabs";
import Button from "antd/lib/button";
import Modal from "antd/lib/modal";
import VisualizationRenderer from "@/components/visualizations/VisualizationRenderer";
import PlainButton from "@/components/PlainButton";

import "./QueryVisualizationTabs.less";

const { TabPane } = Tabs;

function EmptyState({ title, message, refreshButton }) {
  return (
    <div className="query-results-empty-state">
      <div className="empty-state-content">
        <div>
          <img src="static/images/illustrations/no-query-results.svg" alt={window.W_L.find_nothing} />
        </div>
        <h3>{title}</h3>
        <div className="m-b-20">{message}</div>
        {refreshButton}
      </div>
    </div>
  );
}

EmptyState.propTypes = {
  title: PropTypes.string.isRequired,
  message: PropTypes.string.isRequired,
  refreshButton: PropTypes.node,
};

EmptyState.defaultProps = {
  refreshButton: null,
};

function TabWithDeleteButton({ visualizationName, canDelete, onDelete, ...props }) {
  const handleDelete = useCallback(
    e => {
      e.stopPropagation();
      Modal.confirm({
        title: window.W_L.delete_view,
        content: window.W_L.delete_view_confirm,
        okText: window.W_L.ok_text,
        cancelText: window.W_L.cancel,
        okType: "danger",
        onOk: onDelete,
        maskClosable: true,
        autoFocusButton: null,
      });
    },
    [onDelete]
  );

  return (
    <span {...props}>
      {visualizationName}
      {canDelete && (
        <PlainButton className="delete-visualization-button" onClick={handleDelete} aria-label="Close" title="Close">
          <i className="zmdi zmdi-close" aria-hidden="true" />
        </PlainButton>
      )}
    </span>
  );
}

TabWithDeleteButton.propTypes = {
  visualizationName: PropTypes.string.isRequired,
  canDelete: PropTypes.bool,
  onDelete: PropTypes.func,
};
TabWithDeleteButton.defaultProps = { canDelete: false, onDelete: () => {} };

const defaultVisualizations = [
  {
    type: "TABLE",
    name: window.W_L.table,
    id: null,
    options: {},
  },
];

export default function QueryVisualizationTabs({
  queryResult,
  selectedTab,
  showNewVisualizationButton,
  canDeleteVisualizations,
  onChangeTab,
  onAddVisualization,
  onDeleteVisualization,
  refreshButton,
  canRefresh,
  ...props
}) {
  const visualizations = useMemo(
    () => (props.visualizations.length > 0 ? props.visualizations : defaultVisualizations),
    [props.visualizations]
  );

  const tabsProps = {};
  if (find(visualizations, { id: selectedTab })) {
    tabsProps.activeKey = `${selectedTab}`;
  }

  if (showNewVisualizationButton) {
    tabsProps.tabBarExtraContent = (
      <Button
        className="add-visualization-button"
        data-test="NewVisualization"
        type="link"
        onClick={() => onAddVisualization()}>
        <i className="fa fa-plus" aria-hidden="true" />
        <span className="m-l-5 hidden-xs">{window.W_L.add_view}</span>
      </Button>
    );
  }

  const orderedVisualizations = useMemo(() => orderBy(visualizations, ["id"]), [visualizations]);
  const isFirstVisualization = useCallback(visId => visId === orderedVisualizations[0].id, [orderedVisualizations]);
  const isMobile = useMedia({ maxWidth: 768 });

  const [filters, setFilters] = useState([]);

  return (
    <Tabs
      {...tabsProps}
      type="card"
      className={cx("query-visualization-tabs card-style")}
      data-test="QueryPageVisualizationTabs"
      animated={false}
      tabBarGutter={0}
      onChange={activeKey => onChangeTab(+activeKey)}
      destroyInactiveTabPane>
      {orderedVisualizations.map(visualization => (
        <TabPane
          key={`${visualization.id}`}
          tab={
            <TabWithDeleteButton
              data-test={`QueryPageVisualizationTab${visualization.id}`}
              canDelete={!isMobile && canDeleteVisualizations && !isFirstVisualization(visualization.id)}
              visualizationName={visualization.name}
              onDelete={() => onDeleteVisualization(visualization.id)}
            />
          }>
          {queryResult ? (
            <VisualizationRenderer
              visualization={visualization}
              queryResult={queryResult}
              context="query"
              filters={filters}
              onFiltersChange={setFilters}
            />
          ) : (
            <EmptyState
              title={window.W_L.find_nothing}
              message={
                canRefresh
                  ? window.W_L.please_exec_flush
					:window.W_L.no_permission_exec
              }
              refreshButton={refreshButton}
            />
          )}
        </TabPane>
      ))}
    </Tabs>
  );
}

QueryVisualizationTabs.propTypes = {
  queryResult: PropTypes.object, // eslint-disable-line react/forbid-prop-types
  visualizations: PropTypes.arrayOf(PropTypes.object),
  selectedTab: PropTypes.number,
  showNewVisualizationButton: PropTypes.bool,
  canDeleteVisualizations: PropTypes.bool,
  onChangeTab: PropTypes.func,
  onAddVisualization: PropTypes.func,
  onDeleteVisualization: PropTypes.func,
  refreshButton: PropTypes.node,
  canRefresh: PropTypes.bool,
};

QueryVisualizationTabs.defaultProps = {
  queryResult: null,
  visualizations: [],
  selectedTab: null,
  showNewVisualizationButton: false,
  canDeleteVisualizations: false,
  onChangeTab: () => {},
  onAddVisualization: () => {},
  onDeleteVisualization: () => {},
  refreshButton: null,
  canRefresh: true,
};
