/* eslint-disable react/prop-types */
import React from "react";
import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import XAxisSettings from "./XAxisSettings";
import YAxisSettings from "./YAxisSettings";
import SeriesSettings from "./SeriesSettings";
import ColorsSettings from "./ColorsSettings";
import DataLabelsSettings from "./DataLabelsSettings";
import CustomChartSettings from "./CustomChartSettings";

import "./editor.less";

const isCustomChart = (options: any) => options.globalSeriesType === "custom";
const isPieChart = (options: any) => options.globalSeriesType === "pie";

export default createTabbedEditor([
  {
    key: "General",
    title: window.W_L.general,
    component: (props: any) => (
      <React.Fragment>
        <GeneralSettings {...props} />
        {isCustomChart(props.options) && <CustomChartSettings {...props} />}
      </React.Fragment>
    ),
  },
  {
    key: "XAxis",
    title: ({ swappedAxes }: any) => (!swappedAxes ? window.W_L.x_column : window.W_L.y_column),
    component: XAxisSettings,
    isAvailable: (options: any) => !isCustomChart(options) && !isPieChart(options),
  },
  {
    key: "YAxis",
    title: ({ swappedAxes }: any) => (!swappedAxes ? window.W_L.y_column : window.W_L.x_column),
    component: YAxisSettings,
    isAvailable: (options: any) => !isCustomChart(options) && !isPieChart(options),
  },
  {
    key: "Series",
    title:window.W_L.series,
    component: SeriesSettings,
    isAvailable: (options: any) => !isCustomChart(options),
  },
  {
    key: "Colors",
    title: window.W_L.color,
    component: ColorsSettings,
    isAvailable: (options: any) => !isCustomChart(options),
  },
  {
    key: "DataLabels",
    title: window.W_L.data_labels,
    component: DataLabelsSettings,
    isAvailable: (options: any) => !isCustomChart(options),
  },
]);
