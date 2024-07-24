import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import GroupsSettings from "./GroupsSettings";
import FormatSettings from "./FormatSettings";
import StyleSettings from "./StyleSettings";

export default createTabbedEditor([
  { key: "General", title: window.W_L.general, component: GeneralSettings },
  { key: "Groups", title: window.W_L.role, component: GroupsSettings },
  { key: "Format", title: window.W_L.formatting, component: FormatSettings },
  { key: "Style", title: window.W_L.style, component: StyleSettings },
]);
