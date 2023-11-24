import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import ColorsSettings from "./ColorsSettings";
import FormatSettings from "./FormatSettings";
import BoundsSettings from "./BoundsSettings";

export default createTabbedEditor([
  { key: "General", title: "通用", component: GeneralSettings },
  { key: "Colors", title: "颜色", component: ColorsSettings },
  { key: "Format", title: "格式化", component: FormatSettings },
  { key: "Bounds", title: "边界", component: BoundsSettings },
]);
