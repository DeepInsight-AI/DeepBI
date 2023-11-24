import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import GroupsSettings from "./GroupsSettings";
import FormatSettings from "./FormatSettings";
import StyleSettings from "./StyleSettings";

export default createTabbedEditor([
  { key: "General", title: "通用", component: GeneralSettings },
  { key: "Groups", title: "角色", component: GroupsSettings },
  { key: "Format", title: "格式化", component: FormatSettings },
  { key: "Style", title: "样式", component: StyleSettings },
]);
