import getOptions from "./getOptions";
import Renderer from "./Renderer";
import Editor from "./Editor";

export default {
  type: "COHORT",
  name: "同期群分析(Cohort)",
  getOptions,
  Renderer,
  Editor,

  autoHeight: true,
  defaultRows: 8,
};
