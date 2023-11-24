import Renderer from "./Renderer";
import Editor from "./Editor";

export default {
  type: "SUNBURST_SEQUENCE",
  name: "旭辉图(Sunburst Sequence)",
  getOptions: (options: any) => ({
    ...options,
  }),
  Renderer,
  Editor,

  defaultRows: 7,
};
