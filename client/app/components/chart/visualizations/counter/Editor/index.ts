import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import FormatSettings from "./FormatSettings";

export default createTabbedEditor([
  { key: "General", title: window.W_L.general, component: GeneralSettings },
  { key: "Format", title: window.W_L.formatting, component: FormatSettings },
]);
