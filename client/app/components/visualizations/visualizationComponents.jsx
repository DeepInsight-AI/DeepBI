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
