import { isEmpty, map } from "lodash";
import React, { useState, useEffect } from "react";
import PropTypes from "prop-types";
import cx from "classnames";

import Button from "antd/lib/button";
import Checkbox from "antd/lib/checkbox";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import DynamicComponent from "@/components/DynamicComponent";
import DashboardGrid from "@/components/dashboards/DashboardGrid";
import Parameters from "@/components/Parameters";
import Filters from "@/components/Filters";

import { Dashboard } from "@/services/dashboard";
import recordEvent from "@/services/recordEvent";
import resizeObserver from "@/services/resizeObserver";
import routes from "@/services/routes";
import location from "@/services/location";
import url from "@/services/url";
import useImmutableCallback from "@/lib/hooks/useImmutableCallback";

import useDashboard from "./hooks/useDashboard";
import DashboardHeader from "./components/DashboardHeader";

import "./DashboardPage.less";

function DashboardSettings({ dashboardConfiguration }) {
  const { dashboard, updateDashboard } = dashboardConfiguration;
  return (
    <div className="m-b-10 p-15 bg-white tiled">
      <Checkbox
        checked={!!dashboard.dashboard_filters_enabled}
        onChange={({ target }) => updateDashboard({ dashboard_filters_enabled: target.checked })}
        data-test="DashboardFiltersCheckbox">
        {window.W_L.dashboard_filter}
      </Checkbox>
    </div>
  );
}

DashboardSettings.propTypes = {
  dashboardConfiguration: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
};

function AddWidgetContainer({ dashboardConfiguration, className, ...props }) {
  const { showAddTextboxDialog, showAddWidgetDialog } = dashboardConfiguration;
  return (
    <div className={cx("add-widget-container", className)} {...props}>
      <h2>
        <i className="zmdi zmdi-widgets" aria-hidden="true" />
        <span className="hidden-xs hidden-sm">
          {window.W_L.dashboard_widget_info}
        </span>
      </h2>
      <div>
        <Button className="m-r-15" onClick={showAddTextboxDialog} data-test="AddTextboxButton">
          {window.W_L.add_text}
        </Button>
        <Button type="primary" onClick={showAddWidgetDialog} data-test="AddWidgetButton">
          {window.W_L.add_widget}
        </Button>
      </div>
    </div>
  );
}

AddWidgetContainer.propTypes = {
  dashboardConfiguration: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
  className: PropTypes.string,
};

function DashboardComponent(props) {
  const dashboardConfiguration = useDashboard(props.dashboard);
  const {
    dashboard,
    filters,
    setFilters,
    loadDashboard,
    loadWidget,
    removeWidget,
    saveDashboardLayout,
    globalParameters,
    updateDashboard,
    refreshDashboard,
    refreshWidget,
    editingLayout,
    setGridDisabled,
  } = dashboardConfiguration;
  const {isShowReport} = props;
  const [pageContainer, setPageContainer] = useState(null);
  const [bottomPanelStyles, setBottomPanelStyles] = useState({});
  const onParametersEdit = parameters => {
    const paramOrder = map(parameters, "name");
    updateDashboard({ options: { globalParamOrder: paramOrder } });
  };

  useEffect(() => {
    if (pageContainer) {
      const unobserve = resizeObserver(pageContainer, () => {
        if (editingLayout) {
          const style = window.getComputedStyle(pageContainer, null);
          const bounds = pageContainer.getBoundingClientRect();
          const paddingLeft = parseFloat(style.paddingLeft) || 0;
          const paddingRight = parseFloat(style.paddingRight) || 0;
          setBottomPanelStyles({
            left: Math.round(bounds.left) + paddingRight,
            width: pageContainer.clientWidth - paddingLeft - paddingRight,
          });
        }

        // reflow grid when container changes its size
        window.dispatchEvent(new Event("resize"));
      });
      return unobserve;
    }
  }, [pageContainer, editingLayout]);

  return (
    <div className="container" ref={setPageContainer} data-test={`DashboardId${dashboard.id}Container`}>
      {
        isShowReport&&(<DashboardHeader
        dashboardConfiguration={dashboardConfiguration}
        headerExtra={
          <DynamicComponent
            name="Dashboard.HeaderExtra"
            dashboard={dashboard}
            dashboardConfiguration={dashboardConfiguration}
          />
        }
      />)
      }
      {isShowReport&&!isEmpty(globalParameters) && (
        <div className="dashboard-parameters m-b-10 p-15 bg-white tiled" data-test="DashboardParameters">
          <Parameters
            parameters={globalParameters}
            onValuesChange={refreshDashboard}
            sortable={editingLayout}
            onParametersEdit={onParametersEdit}
          />
        </div>
      )}
      {isShowReport&&!isEmpty(filters) && (
        <div className="m-b-10 p-15 bg-white tiled" data-test="DashboardFilters">
          <Filters filters={filters} onChange={setFilters} />
        </div>
      )}
      {isShowReport&&editingLayout && <DashboardSettings dashboardConfiguration={dashboardConfiguration} />}
      <div id="dashboard-container">
        <DashboardGrid
          isShowReport={isShowReport}
          dashboard={dashboard}
          widgets={dashboard.widgets}
          filters={filters}
          isEditing={editingLayout}
          onLayoutChange={editingLayout ? saveDashboardLayout : () => {}}
          onBreakpointChange={setGridDisabled}
          onLoadWidget={loadWidget}
          onRefreshWidget={refreshWidget}
          onRemoveWidget={removeWidget}
          onParameterMappingsChange={loadDashboard}
        />
      </div>
      {isShowReport&&editingLayout && (
        <AddWidgetContainer dashboardConfiguration={dashboardConfiguration} style={bottomPanelStyles} />
      )}
    </div>
  );
}

DashboardComponent.propTypes = {
  dashboard: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
};

function DashboardPage({ dashboardSlug, dashboardId, onError,isShowReport }) {
  const [dashboard, setDashboard] = useState(null);
  const handleError = useImmutableCallback(onError);
  
  useEffect(() => {
    Dashboard.get({ id: dashboardId, slug: dashboardSlug })
      .then(dashboardData => {
        recordEvent("view", "dashboard", dashboardData.id);
        setDashboard(dashboardData);

        // if loaded by slug, update location url to use the id
        if (!dashboardId) {
          location.setPath(url.parse(dashboardData.url).pathname, true);
        }
        let timeoutId = null;
  const scrollToBottom = () => {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
    const messageContainer = document.querySelector('.dashbord_page');
    if (messageContainer) {
        const scrollHeight = messageContainer.scrollHeight;
      const clientHeight = messageContainer.clientHeight;
      messageContainer.scrollTo({
        top: scrollHeight - clientHeight,
        behavior: 'smooth',
      });
    }
  }, 80);
  };
  scrollToBottom();
      })
      .catch(handleError);
  }, [dashboardId, dashboardSlug, handleError]);

  return <div className="dashboard-page">{dashboard && <DashboardComponent isShowReport={isShowReport} dashboard={dashboard} />}</div>;
}

DashboardPage.propTypes = {
  dashboardSlug: PropTypes.string,
  dashboardId: PropTypes.string,
  onError: PropTypes.func,
  isShowReport: PropTypes.bool,
};

DashboardPage.defaultProps = {
  dashboardSlug: null,
  dashboardId: null,
  onError: PropTypes.func,
  isShowReport: true,
};

export default DashboardPage;
// route kept for backward compatibility
routes.register(
  "Dashboards.LegacyViewOrEdit",
  routeWithUserSession({
    path: "/dashboard/:dashboardSlug",
    render: pageProps => <DashboardPage {...pageProps} />,
  })
);

routes.register(
  "Dashboards.ViewOrEdit",
  routeWithUserSession({
    path: "/dashboards/:dashboardId([^-]+)(-.*)?",
    render: pageProps => <DashboardPage {...pageProps} />,
  })
);
