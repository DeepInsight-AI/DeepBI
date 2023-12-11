import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import ColumnsSettings from "./ColumnsSettings";
import OptionsSettings from "./OptionsSettings";
import ColorsSettings from "./ColorsSettings";
import AppearanceSettings from "./AppearanceSettings";

export default createTabbedEditor([
  { key: "Columns", title: window.W_L.column, component: ColumnsSettings },
  { key: "Options", title: window.W_L.option, component: OptionsSettings },
  { key: "Colors", title: window.W_L.color, component: ColorsSettings },
  { key: "Appearance", title: window.W_L.exterior, component: AppearanceSettings },
]);
