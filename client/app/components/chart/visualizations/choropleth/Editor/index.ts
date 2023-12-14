import createTabbedEditor from "@/components/chart/components/visualizations/editor/createTabbedEditor";

import GeneralSettings from "./GeneralSettings";
import ColorsSettings from "./ColorsSettings";
import FormatSettings from "./FormatSettings";
import BoundsSettings from "./BoundsSettings";

export default createTabbedEditor([
  { key: "General", title: window.W_L.general, component: GeneralSettings },
  { key: "Colors", title: window.W_L.color, component: ColorsSettings },
  { key: "Format", title: window.W_L.formatting, component: FormatSettings },
  { key: "Bounds", title: window.W_L.border, component: BoundsSettings },
]);
