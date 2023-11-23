import getOptions from "./getOptions";
import Renderer from "./Renderer";
import Editor from "./Editor";

export default {
  type: "FUNNEL",
  name: "漏斗分析(Funnel)",
  getOptions,
  Renderer,
  Editor,

  defaultRows: 10,
};
