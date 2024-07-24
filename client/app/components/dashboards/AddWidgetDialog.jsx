import { map, includes, groupBy, first, find } from "lodash";
import React, { useState, useMemo, useCallback } from "react";
import PropTypes from "prop-types";
import Select from "antd/lib/select";
import Modal from "antd/lib/modal";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import { MappingType, ParameterMappingListInput } from "@/components/ParameterMappingInput";
import QuerySelector from "@/components/QuerySelector";
import notification from "@/services/notification";
import { Query } from "@/services/query";
import { useUniqueId } from "@/lib/hooks/useUniqueId";

function VisualizationSelect({ query, visualization, onChange }) {
  const visualizationGroups = useMemo(() => {
    return query ? groupBy(query.visualizations, "type") : {};
  }, [query]);

  const vizSelectId = useUniqueId("visualization-select");

  const handleChange = useCallback(
    visualizationId => {
      const selectedVisualization = query ? find(query.visualizations, { id: visualizationId }) : null;
      onChange(selectedVisualization || null);
    },
    [query, onChange]
  );

  if (!query) {
    return null;
  }

  const groups = {"BOXPLOT":window.W_L.BOXPLOT,
				  "CHART":window.W_L.CHART,
				  "CHOROPLETH":window.W_L.CHOROPLETH,
				  "COHORT":window.W_L.COHORT,
				  "COUNTER":window.W_L.COUNTER,
				  "DETAILS":window.W_L.DETAILS,
				  "FUNNEL":window.W_L.FUNNEL,
				  "MAP":window.W_L.MAP,
				  "PIVOT":window.W_L.PIVOT,
				  "SANKEY":window.W_L.SANKEY,
				  "SUNBURST_SEQUENCE":window.W_L.SUNBURST_SEQUENCE,
				  "TABLE":window.W_L.TABLE,
				  "WORD_CLOUD":window.W_L.WORD_CLOUD
	};

  return (
    <div>
      <div className="form-group">
        <label htmlFor={vizSelectId}>{window.W_L.select_view}</label>
        <Select
          id={vizSelectId}
          className="w-100"
          value={visualization ? visualization.id : undefined}
          onChange={handleChange}>
          {map(visualizationGroups, (visualizations, groupKey) => (
            <Select.OptGroup key={groupKey} label={groups[groupKey]}>
              {map(visualizations, visualization => (
                <Select.Option key={`${visualization.id}`} value={visualization.id}>
                  {visualization.name}
                </Select.Option>
              ))}
            </Select.OptGroup>
          ))}
        </Select>
      </div>
    </div>
  );
}

VisualizationSelect.propTypes = {
  query: PropTypes.object,
  visualization: PropTypes.object,
  onChange: PropTypes.func,
};

VisualizationSelect.defaultProps = {
  query: null,
  visualization: null,
  onChange: () => {},
};

function AddWidgetDialog({ dialog, dashboard }) {
  const [selectedQuery, setSelectedQuery] = useState(null);
  const [selectedVisualization, setSelectedVisualization] = useState(null);
  const [parameterMappings, setParameterMappings] = useState([]);

  const selectQuery = useCallback(
    queryId => {
      // Clear previously selected query (if any)
      setSelectedQuery(null);
      setSelectedVisualization(null);
      setParameterMappings([]);

      if (queryId) {
        Query.get({ id: queryId }).then(query => {
          if (query) {
            const existingParamNames = map(dashboard.getParametersDefs(), param => param.name);
            setSelectedQuery(query);
            setParameterMappings(
              map(query.getParametersDefs(), param => ({
                name: param.name,
                type: includes(existingParamNames, param.name)
                  ? MappingType.DashboardMapToExisting
                  : MappingType.DashboardAddNew,
                mapTo: param.name,
                value: param.normalizedValue,
                title: "",
                param,
              }))
            );
            if (query.visualizations.length > 0) {
              setSelectedVisualization(first(query.visualizations));
            }
          }
        });
      }
    },
    [dashboard]
  );

  const saveWidget = useCallback(() => {
    dialog.close({ visualization: selectedVisualization, parameterMappings }).catch(() => {
      notification.error(window.W_L.widget_not_add);
    });
  }, [dialog, selectedVisualization, parameterMappings]);

  const existingParams = dashboard.getParametersDefs();
  const parameterMappingsId = useUniqueId("parameter-mappings");

  return (
    <Modal
      {...dialog.props}
      title={window.W_L.add_widget}
      onOk={saveWidget}
      okButtonProps={{
        ...dialog.props.okButtonProps,
        disabled: !selectedQuery || dialog.props.okButtonProps.disabled,
      }}
      okText={window.W_L.add_widget_to_dashboard}
      cancelText={window.W_L.cancel}
      width={700}>
      <div data-test="AddWidgetDialog">
        <QuerySelector onChange={query => selectQuery(query ? query.id : null)} />

        {selectedQuery && (
          <VisualizationSelect
            query={selectedQuery}
            visualization={selectedVisualization}
            onChange={setSelectedVisualization}
          />
        )}

        {parameterMappings.length > 0 && [
          <label key="parameters-title" htmlFor={parameterMappingsId}>
           {window.W_L.params}
          </label>,
          <ParameterMappingListInput
            key="parameters-list"
            id={parameterMappingsId}
            mappings={parameterMappings}
            existingParams={existingParams}
            onChange={setParameterMappings}
          />,
        ]}
      </div>
    </Modal>
  );
}

AddWidgetDialog.propTypes = {
  dialog: DialogPropType.isRequired,
  dashboard: PropTypes.object.isRequired,
};

export default wrapDialog(AddWidgetDialog);
