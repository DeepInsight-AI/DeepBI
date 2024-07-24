import React from "react";
import { pick } from "lodash";
import HelpTrigger from "@/components/HelpTrigger";
import Link from "@/components/Link";
import { Renderer as VisRenderer, Editor as VisEditor } from "@/components/chart/visualizations";
import {updateVisualizationsSettings} from "@/components/chart/visualizations/visualizationsSettings"
import { clientConfig } from "@/services/auth";

import countriesDataUrl from "./countries.geo.json";
import usaDataUrl from "./usa-albers.geo.json";
import subdivJapanDataUrl from "./japan.prefectures.geo.json";
import chinaDataUrl from "./china/china.geo.json";
import BeiJing from "./china/110000.geo.json";
import TianJing from "./china/120000.geo.json";
import HeBei from "./china/130000.geo.json";
import ShanX from "./china/140000.geo.json";
import NeiMengGu from "./china/150000.geo.json";
import LiaoNing from "./china/210000.geo.json";
import JiLin from "./china/220000.geo.json";
import HeiLongJiang from "./china/230000.geo.json";
import ShangHi from "./china/310000.geo.json";
import JiangSu from "./china/320000.geo.json";
import ZheJiang from "./china/330000.geo.json";
import AnHui from "./china/340000.geo.json";
import FuJian from "./china/350000.geo.json";
import JiangXi from "./china/360000.geo.json";
import ShanDong from "./china/370000.geo.json";
import HeNan from "./china/410000.geo.json";
import HuBei from "./china/420000.geo.json";
import HuNan from "./china/430000.geo.json";
import GuangDong from "./china/440000.geo.json";
import GuangXi from "./china/450000.geo.json";
import HaiNan from "./china/460000.geo.json";
import ChongQing from "./china/500000.geo.json";
import SiChuan from "./china/510000.geo.json";
import GuiZhou from "./china/520000.geo.json";
import YunNan from "./china/530000.geo.json";
import XiZang from "./china/540000.geo.json";
import ShanXi from "./china/610000.geo.json";
import GanSu from "./china/620000.geo.json";
import QingHai from "./china/630000.geo.json";
import NingXia from "./china/640000.geo.json";
import XinJiang from "./china/650000.geo.json";
import TaiWan from "./china/710000.geo.json";
import HongKong from "./china/810000.geo.json";
import AoMen from "./china/820000.geo.json";
function wrapComponentWithSettings(WrappedComponent) {
  return function VisualizationComponent(props) {
    updateVisualizationsSettings({
      HelpTriggerComponent: HelpTrigger,
      LinkComponent: Link,
      choroplethAvailableMaps: {
        countries: {
          name: window.W_L.country,
          url: countriesDataUrl,
          fieldNames: {
            name: window.W_L.short_name,
            name_long: window.W_L.full_name,
            abbrev: window.W_L.abbreviated_name,
            iso_a2: "ISO code(2 letters)",
            iso_a3: "ISO code(3 letters)",
            iso_n3: "ISO code(3 digits)",
          },
        },
        china: {
          name: "中国行政区划图",
          url: chinaDataUrl,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      // china_all_adcode: {
      //     name: "(100000)中国省级地图(动态选择)",
      //     url: YZ,
      //     fieldNames: {
      //         name: "简称",
      //         adcode: "行政区划编码(6字母)"
      //     }
      // },
      china_110000: {
          name: "(110000)北京市行政区划图",
          url: BeiJing,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_120000: {
          name: "(120000)天津市行政区划图",
          url: TianJing,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_130000: {
          name: "(130000)河北省行政区划图",
          url: HeBei,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_140000: {
          name: "(140000)山西省行政区划图",
          url: ShanX,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_150000: {
          name: "(150000)内蒙古行政区划图",
          url: NeiMengGu,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_210000: {
          name: "(210000)辽宁省行政区划图",
          url: LiaoNing,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_220000: {
          name: "(220000)吉林省行政区划图",
          url: JiLin,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_230000: {
          name: "(230000)黑龙江省行政区划图",
          url: HeiLongJiang,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_310000: {
          name: "(310000)上海市行政区划图",
          url: ShangHi,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_320000: {
          name: "(320000)江苏省行政区划图",
          url: JiangSu,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_330000: {
          name: "(330000)浙江省行政区划图",
          url: ZheJiang,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_340000: {
          name: "(340000)安徽省行政区划图",
          url: AnHui,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_350000: {
          name: "(350000)福建省行政区划图",
          url: FuJian,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_360000: {
          name: "(360000)江西省行政区划图",
          url: JiangXi,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_370000: {
          name: "(370000)山东省行政区划图",
          url: ShanDong,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_410000: {
          name: "(410000)河南省行政区划图",
          url: HeNan,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_420000: {
          name: "(420000)湖北省行政区划图",
          url: HuBei,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_430000: {
          name: "(430000)湖南省行政区划图",
          url: HuNan,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_440000: {
          name: "(440000)广东省行政区划图",
          url: GuangDong,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_450000: {
          name: "(450000)广西行政区划图",
          url: GuangXi,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_460000: {
          name: "(460000)海南省行政区划图",
          url: HaiNan,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_500000: {
          name: "(500000)重庆市行政区划图",
          url: ChongQing,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_510000: {
          name: "(510000)四川省行政区划图",
          url: SiChuan,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_520000: {
          name: "(520000)贵州省行政区划图",
          url: GuiZhou,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_530000: {
          name: "(530000)云南省行政区划图",
          url: YunNan,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_540000: {
          name: "(540000)西藏行政区划图",
          url: XiZang,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_610000: {
          name: "(610000)陕西省行政区划图",
          url: ShanXi,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_620000: {
          name: "(620000)甘肃省行政区划图",
          url: GanSu,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_630000: {
          name: "(630000)青海省行政区划图",
          url: QingHai,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_640000: {
          name: "(640000)宁夏行政区划图",
          url: NingXia,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_650000: {
          name: "(650000)新疆行政区划图",
          url: XinJiang,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_710000: {
          name: "(710000)台湾行政区划图",
          url: TaiWan,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_810000: {
          name: "(810000)香港行政区划图",
          url: HongKong,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
      china_820000: {
          name: "(820000)澳门行政区划图",
          url: AoMen,
          fieldNames: {
              name: "简称",
              adcode: "行政区划编码(6字母)"
          }
      },
        usa: {
          name: "USA",
          url: usaDataUrl,
          fieldNames: {
            name: "Name",
            ns_code: "USA ANSI Code(8 character)",
            geoid: "Geographic ID",
            usps_abbrev: "USPS Abbreviation",
            fips_code: "FIPS Code (2-character)",
          },
        },
        subdiv_japan: {
          name: "Japan/Prefectures", 
          url: subdivJapanDataUrl,
          fieldNames: {
            name: "Name",
            name_alt: "Name (alternative)",
            name_local: "Name (local)",
            iso_3166_2: "ISO-3166-2",
            postal: "Postal Code",
            type: "Type",
            type_en: "Type (EN)",
            region: "Region",
            region_code: "Region Code",
          },
        },
      },
      ...pick(clientConfig, [
        "dateFormat",
        "dateTimeFormat",
        "integerFormat",
        "floatFormat",
        "booleanValues",
        "tableCellMaxJSONSize",
        "allowCustomJSVisualizations",
        "hidePlotlyModeBar",
      ]),
    });

    return <WrappedComponent {...props} />;
  };
}

export const Renderer = wrapComponentWithSettings(VisRenderer);
export const Editor = wrapComponentWithSettings(VisEditor);
