import React from "react";
import { Section, Switch } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";

import AxisSettings from "./AxisSettings";

export default function YAxisSettings({ options, onOptionsChange }: any) {
  const [leftYAxis, rightYAxis] = options.yAxis;

  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section.Title>{!options.swappedAxes ? window.W_L.left_y_axis : window.W_L.x_axis}</Section.Title>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <AxisSettings
          id="LeftYAxis"
          features={{ range: true }}
          options={leftYAxis}
          // @ts-expect-error ts-migrate(2322) FIXME: Type '(axis: any) => any' is not assignable to typ... Remove this comment to see the full error message
          onChange={(axis: any) => onOptionsChange({ yAxis: [axis, rightYAxis] })}
        />
      </Section>

      {options.globalSeriesType !== "heatmap" && !options.swappedAxes && (
        <React.Fragment>
          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section.Title>{window.W_L.right_y_axis}</Section.Title>

          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            <AxisSettings
              id="RightYAxis"
              features={{ range: true }}
              options={rightYAxis}
              // @ts-expect-error ts-migrate(2322) FIXME: Type '(axis: any) => any' is not assignable to typ... Remove this comment to see the full error message
              onChange={(axis: any) => onOptionsChange({ yAxis: [leftYAxis, axis] })}
            />
          </Section>

          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
            <Switch
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
              id="chart-editor-y-axis-align-at-zero"
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
              data-test="Chart.YAxis.AlignAtZero"
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'any' is not assignable to type 'never'.
              defaultChecked={options.alignYAxesAtZero}
              // @ts-expect-error ts-migrate(2322) FIXME: Type '(alignYAxesAtZero: any) => any' is not assig... Remove this comment to see the full error message
              onChange={(alignYAxesAtZero: any) => onOptionsChange({ alignYAxesAtZero })}>
              {window.W_L.align_Y_axes_at_zero}
            </Switch>
          </Section>
        </React.Fragment>
      )}

      {options.globalSeriesType === "heatmap" && (
        <React.Fragment>
          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
            <Switch
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
              id="chart-editor-y-axis-sort"
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
              data-test="Chart.LeftYAxis.Sort"
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'any' is not assignable to type 'never'.
              defaultChecked={options.sortY}
              // @ts-expect-error ts-migrate(2322) FIXME: Type '(sortY: any) => any' is not assignable to ty... Remove this comment to see the full error message
              onChange={(sortY: any) => onOptionsChange({ sortY })}>
              {window.W_L.sort_values}
            </Switch>
          </Section>

          {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
          <Section>
            {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
            <Switch
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
              id="chart-editor-y-axis-reverse"
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
              data-test="Chart.LeftYAxis.Reverse"
              // @ts-expect-error ts-migrate(2322) FIXME: Type 'any' is not assignable to type 'never'.
              defaultChecked={options.reverseY}
              // @ts-expect-error ts-migrate(2322) FIXME: Type '(reverseY: any) => any' is not assignable to... Remove this comment to see the full error message
              onChange={(reverseY: any) => onOptionsChange({ reverseY })}>
               {window.W_L.reverse_order}
            </Switch>
          </Section>
        </React.Fragment>
      )}
    </React.Fragment>
  );
}

YAxisSettings.propTypes = EditorPropTypes;
