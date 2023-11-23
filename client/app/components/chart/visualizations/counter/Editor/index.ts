import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import FormatSettings from "./FormatSettings";

export default createTabbedEditor([
  { key: "General", title: "通用", component: GeneralSettings },
  { key: "Format", title: "格式化", component: FormatSettings },
]);
