import { isNil, map } from "lodash";
import React, { useMemo } from "react";
import { useDebouncedCallback } from "use-debounce";
import { Section, Select, Checkbox, Input, ColorPicker, ContextHelp } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";
import ColorPalette from "@/components/chart/visualizations/ColorPalette";

const mapTiles = [
  {
    name: "OpenStreetMap",
    url: "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
  },
  {
    name: "OpenStreetMap BW",
    url: "//{s}.tiles.wmflabs.org/bw-mapnik/{z}/{x}/{y}.png",
  },
  {
    name: "OpenStreetMap DE",
    url: "//{s}.tile.openstreetmap.de/tiles/osmde/{z}/{x}/{y}.png",
  },
  {
    name: "OpenStreetMap FR",
    url: "//{s}.tile.openstreetmap.fr/osmfr/{z}/{x}/{y}.png",
  },
  {
    name: "OpenStreetMap Hot",
    url: "//{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
  },
  {
    name: "Thunderforest",
    url: "//{s}.tile.thunderforest.com/cycle/{z}/{x}/{y}.png",
  },
  {
    name: "Thunderforest Spinal",
    url: "//{s}.tile.thunderforest.com/spinal-map/{z}/{x}/{y}.png",
  },
  {
    name: "OpenMapSurfer",
    url: "//korona.geog.uni-heidelberg.de/tiles/roads/x={x}&y={y}&z={z}",
  },
  {
    name: "Stamen Toner",
    url: "//stamen-tiles-{s}.a.ssl.fastly.net/toner/{z}/{x}/{y}.png",
  },
  {
    name: "Stamen Toner Background",
    url: "//stamen-tiles-{s}.a.ssl.fastly.net/toner-background/{z}/{x}/{y}.png",
  },
  {
    name: "Stamen Toner Lite",
    url: "//stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.png",
  },
  {
    name: "OpenTopoMap",
    url: "//{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
  },
];

const CustomColorPalette = {
  White: "#ffffff",
  ...ColorPalette,
};

function getCustomIconOptionFields(iconShape: any) {
  switch (iconShape) {
    case "doughnut":
      return { showIcon: false, showBackgroundColor: true, showBorderColor: true };
    case "circle-dot":
    case "rectangle-dot":
      return { showIcon: false, showBackgroundColor: false, showBorderColor: true };
    default:
      return { showIcon: true, showBackgroundColor: true, showBorderColor: true };
  }
}

export default function StyleSettings({ options, onOptionsChange }: any) {
  const [debouncedOnOptionsChange] = useDebouncedCallback(onOptionsChange, 200);

  const { showIcon, showBackgroundColor, showBorderColor } = useMemo(
    () => getCustomIconOptionFields(options.iconShape),
    [options.iconShape]
  );

  const isCustomMarkersStyleAllowed = isNil(options.classify);

  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Select
          label={window.W_L.Map_Tiles}
          data-test="Map.Editor.Tiles"
          value={options.mapTileUrl}
          onChange={(mapTileUrl: any) => onOptionsChange({ mapTileUrl })}>
          {map(mapTiles, ({ name, url }) => (
            // @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message
            <Select.Option key={url} data-test={"Map.Editor.Tiles." + name}>
              {name}
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
            </Select.Option>
          ))}
        </Select>
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section.Title>{window.W_L.Markers}</Section.Title>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Checkbox
          data-test="Map.Editor.ClusterMarkers"
          defaultChecked={options.clusterMarkers}
          onChange={event => onOptionsChange({ clusterMarkers: event.target.checked })}>
          {window.W_L.Cluster_Markers}
        </Checkbox>
      </Section>

      {/* @ts-expect-error ts-migrate(2746) FIXME: This JSX tag's 'children' prop expects a single ch... Remove this comment to see the full error message */}
      <Section>
        <Checkbox
          data-test="Map.Editor.CustomizeMarkers"
          disabled={!isCustomMarkersStyleAllowed}
          defaultChecked={options.customizeMarkers}
          onChange={event => onOptionsChange({ customizeMarkers: event.target.checked })}>
          {window.W_L.Override_default_style}
        </Checkbox>
        {!isCustomMarkersStyleAllowed && (
          // @ts-expect-error ts-migrate(2746) FIXME: This JSX tag's 'children' prop expects a single ch... Remove this comment to see the full error message
          <ContextHelp placement="topLeft" arrowPointAtCenter>
            Custom marker styles are not available
            <br />
            when <b>Group By</b> column selected.
          </ContextHelp>
        )}
      </Section>

      {isCustomMarkersStyleAllowed && options.customizeMarkers && (
        <React.Fragment>
          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            <Select
              layout="horizontal"
              label={window.W_L.Shape}
              data-test="Map.Editor.MarkerShape"
              value={options.iconShape}
              onChange={(iconShape: any) => onOptionsChange({ iconShape })}>
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              <Select.Option key="marker" data-test="Map.Editor.MarkerShape.marker">
              {window.W_L.Markers} + {window.W_L.icon}
                {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              </Select.Option>
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              <Select.Option key="doughnut" data-test="Map.Editor.MarkerShape.doughnut">
              {window.W_L.Circle}
                {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              </Select.Option>
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              <Select.Option key="circle-dot" data-test="Map.Editor.MarkerShape.circle-dot">
              {window.W_L.Circle_Dot}
                {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              </Select.Option>
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              <Select.Option key="circle" data-test="Map.Editor.MarkerShape.circle">
              {window.W_L.Circle} + {window.W_L.icon}
                {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              </Select.Option>
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              <Select.Option key="rectangle-dot" data-test="Map.Editor.MarkerShape.rectangle-dot">
              {window.W_L.Square_Dot}
                {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              </Select.Option>
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              <Select.Option key="rectangle" data-test="Map.Editor.MarkerShape.rectangle">
              {window.W_L.Square} + {window.W_L.icon}
                {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
              </Select.Option>
            </Select>
          </Section>

          {showIcon && (
            // @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message
            <Section>
              <Input
                layout="horizontal"
                label={
                  <React.Fragment>
                    {window.W_L.icon}
                    {/* @ts-expect-error ts-migrate(2746) FIXME: This JSX tag's 'children' prop expects a single ch... Remove this comment to see the full error message */}
                    <ContextHelp placement="topLeft" arrowPointAtCenter>
                      <div style={{ marginBottom: 5 }}>
                      {window.W_L.Enter_an_icon_name_from}{" "}
                        <a href="https://fontawesome.com/v4.7.0/icons/" target="_blank" rel="noopener noreferrer">
                          Font-Awesome 4.7
                        </a>
                      </div>
                      <div style={{ marginBottom: 5 }}>
                      {window.W_L.Examples}：<code>check</code>, <code>times-circle</code>, <code>flag</code>
                      </div>
                      <div>{window.W_L.Leave_blank_to_remove}</div>
                    </ContextHelp>
                  </React.Fragment>
                }
                data-test="Map.Editor.MarkerIcon"
                defaultValue={options.iconFont}
                onChange={(event: any) => debouncedOnOptionsChange({ iconFont: event.target.value })}
              />
            </Section>
          )}

          {showIcon && (
            // @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message
            <Section>
              <ColorPicker
                layout="horizontal"
                label={window.W_L.Icon_Color}
                interactive
                presetColors={CustomColorPalette}
                placement="topRight"
                color={options.foregroundColor}
                triggerProps={{ "data-test": "Map.Editor.MarkerIconColor" }}
                onChange={(foregroundColor: any) => onOptionsChange({ foregroundColor })}
                // @ts-expect-error ts-migrate(2339) FIXME: Property 'Label' does not exist on type '({ classN... Remove this comment to see the full error message
                addonAfter={<ColorPicker.Label color={options.foregroundColor} presetColors={CustomColorPalette} />}
              />
            </Section>
          )}

          {showBackgroundColor && (
            // @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message
            <Section>
              <ColorPicker
                layout="horizontal"
                label={window.W_L.Background_Color}
                interactive
                presetColors={CustomColorPalette}
                placement="topRight"
                color={options.backgroundColor}
                triggerProps={{ "data-test": "Map.Editor.MarkerBackgroundColor" }}
                onChange={(backgroundColor: any) => onOptionsChange({ backgroundColor })}
                // @ts-expect-error ts-migrate(2339) FIXME: Property 'Label' does not exist on type '({ classN... Remove this comment to see the full error message
                addonAfter={<ColorPicker.Label color={options.backgroundColor} presetColors={CustomColorPalette} />}
              />
            </Section>
          )}

          {showBorderColor && (
            // @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message
            <Section>
              <ColorPicker
                layout="horizontal"
                label={window.W_L.Borders_Color}
                interactive
                presetColors={CustomColorPalette}
                placement="topRight"
                color={options.borderColor}
                triggerProps={{ "data-test": "Map.Editor.MarkerBorderColor" }}
                onChange={(borderColor: any) => onOptionsChange({ borderColor })}
                // @ts-expect-error ts-migrate(2339) FIXME: Property 'Label' does not exist on type '({ classN... Remove this comment to see the full error message
                addonAfter={<ColorPicker.Label color={options.borderColor} presetColors={CustomColorPalette} />}
              />
            </Section>
          )}
        </React.Fragment>
      )}
    </React.Fragment>
  );
}

StyleSettings.propTypes = EditorPropTypes;
