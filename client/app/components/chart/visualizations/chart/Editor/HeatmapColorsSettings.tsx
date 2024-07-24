import { map } from "lodash";
import React from "react";
import { Section, Select, ColorPicker } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";
import ColorPalette from "@/components/chart/visualizations/ColorPalette";

const ColorSchemes = [
  "Blackbody",
  "Bluered",
  "Blues",
  "Earth",
  "Electric",
  "Greens",
  "Greys",
  "Hot",
  "Jet",
  "Picnic",
  "Portland",
  "Rainbow",
  "RdBu",
  "Reds",
  "Viridis",
  "YlGnBu",
  "YlOrRd",
  "Custom...",
];

export default function HeatmapColorsSettings({ options, onOptionsChange }: any) {
  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Select
          label={window.W_L.color_scheme}
          data-test="Chart.Colors.Heatmap.ColorScheme"
          placeholder={window.W_L.choose_color_scheme}
          allowClear
          value={options.colorScheme || undefined}
          onChange={(value: any) => onOptionsChange({ colorScheme: value || null })}>
          {map(ColorSchemes, scheme => (
            // @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message
            <Select.Option key={scheme} value={scheme} data-test={`Chart.Colors.Heatmap.ColorScheme.${scheme}`}>
              {scheme}
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
            </Select.Option>
          ))}
        </Select>
      </Section>

      {options.colorScheme === "Custom..." && (
        <React.Fragment>
          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            <ColorPicker
              layout="horizontal"
              label={window.W_L.min_color}
              data-test="Chart.Colors.Heatmap.MinColor"
              interactive
              placement="topLeft"
              presetColors={ColorPalette}
              color={options.heatMinColor}
              onChange={(heatMinColor: any) => onOptionsChange({ heatMinColor })}
              // @ts-expect-error ts-migrate(2339) FIXME: Property 'Label' does not exist on type '({ classN... Remove this comment to see the full error message
              addonAfter={<ColorPicker.Label color={options.heatMinColor} presetColors={ColorPalette} />}
            />
          </Section>
          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            <ColorPicker
              layout="horizontal"
              label={window.W_L.max_color}
              data-test="Chart.Colors.Heatmap.MaxColor"
              interactive
              placement="topRight"
              presetColors={ColorPalette}
              color={options.heatMaxColor}
              onChange={(heatMaxColor: any) => onOptionsChange({ heatMaxColor })}
              // @ts-expect-error ts-migrate(2339) FIXME: Property 'Label' does not exist on type '({ classN... Remove this comment to see the full error message
              addonAfter={<ColorPicker.Label color={options.heatMaxColor} presetColors={ColorPalette} />}
            />
          </Section>
        </React.Fragment>
      )}
    </React.Fragment>
  );
}

HeatmapColorsSettings.propTypes = EditorPropTypes;
