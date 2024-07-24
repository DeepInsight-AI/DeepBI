import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import ColumnsSettings from "./ColumnsSettings";
import GridSettings from "./GridSettings";

import "./editor.less";

export default createTabbedEditor([
  { key: "Columns", title: window.W_L.columns, component: ColumnsSettings },
  { key: "Grid", title: window.W_L.grid, component: GridSettings },
]);
