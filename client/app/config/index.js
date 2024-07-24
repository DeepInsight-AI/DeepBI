// import moment from "moment";
import { isFunction } from "lodash";

// Ensure that this image will be available in assets folder
import "@/assets/images/avatar.svg";

// Register visualizations
// import "@redash/viz/lib";

// Register routes before registering extensions as they may want to override some
import "@/pages";

import "./antd-spinner";

// moment.updateLocale("en", {
//   relativeTime: {
//     future: "%s",
//     past: "%s",
//     s: "刚刚",
//     m: "1分钟前",
//     mm: "%d 分钟前",
//     h: "1小时前",
//     hh: "%d 小时前",
//     d: "1天前",
//     dd: "%d 天前",
//     M: "1个月前",
//     MM: "%d 个月前",
//     y: "1年前",
//     yy: "%d 年前",
//   },
// });

function requireImages() {
  // client/app/assets/images/<path> => /images/<path>
  const ctx = require.context("@/assets/images/", true, /\.(png|jpe?g|gif|svg)$/);
  ctx.keys().forEach(ctx);
}

function registerExtensions() {
  const context = require.context("extensions", true, /^((?![\\/.]test[\\./]).)*\.jsx?$/);
  const modules = context
    .keys()
    .map(context)
    .map(module => module.default);

  return modules
    .filter(isFunction)
    .filter(f => f.init)
    .map(f => f());
}

requireImages();
registerExtensions();
