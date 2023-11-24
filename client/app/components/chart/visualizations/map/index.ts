import getOptions from "./getOptions";
import Renderer from "./Renderer";
import Editor from "./Editor";

export default {
  type: "MAP",
  name: "地理标记(Map Markers)",
  getOptions,
  Renderer,
  Editor,

  defaultColumns: 3,
  defaultRows: 8,
  minColumns: 2,
};
