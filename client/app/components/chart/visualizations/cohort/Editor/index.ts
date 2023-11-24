import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import ColumnsSettings from "./ColumnsSettings";
import OptionsSettings from "./OptionsSettings";
import ColorsSettings from "./ColorsSettings";
import AppearanceSettings from "./AppearanceSettings";

export default createTabbedEditor([
  { key: "Columns", title: "列", component: ColumnsSettings },
  { key: "Options", title: "选项", component: OptionsSettings },
  { key: "Colors", title: "颜色", component: ColorsSettings },
  { key: "Appearance", title: "外观", component: AppearanceSettings },
]);
