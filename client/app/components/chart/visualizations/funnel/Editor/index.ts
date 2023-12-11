import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import AppearanceSettings from "./AppearanceSettings";

export default createTabbedEditor([
  { key: "General", title: window.W_L.general, component: GeneralSettings },
  { key: "Appearance", title: window.W_L.exterior, component: AppearanceSettings },
]);
