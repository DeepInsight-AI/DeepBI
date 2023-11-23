import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import AppearanceSettings from "./AppearanceSettings";

export default createTabbedEditor([
  { key: "General", title: "通用", component: GeneralSettings },
  { key: "Appearance", title: "外观", component: AppearanceSettings },
]);
