import React from "react";
import { Section, Switch } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";

import AxisSettings from "./AxisSettings";

export default function XAxisSettings({ options, onOptionsChange }: any) {
  return (
    <React.Fragment>
      <AxisSettings
        id="XAxis"
        features={{ autoDetectType: true }}
        options={options.xAxis}
        // @ts-expect-error ts-migrate(2322) FIXME: Type '(xAxis: any) => any' is not assignable to ty... Remove this comment to see the full error message
        onChange={(xAxis: any) => onOptionsChange({ xAxis })}
      />

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
        <Switch
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
          data-test="Chart.XAxis.Sort"
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'any' is not assignable to type 'never'.
          defaultChecked={options.sortX}
          // @ts-expect-error ts-migrate(2322) FIXME: Type '(sortX: any) => any' is not assignable to ty... Remove this comment to see the full error message
          onChange={(sortX: any) => onOptionsChange({ sortX })}>
          {window.W_L.sort_values}
        </Switch>
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
        <Switch
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
          data-test="Chart.XAxis.Reverse"
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'any' is not assignable to type 'never'.
          defaultChecked={options.reverseX}
          // @ts-expect-error ts-migrate(2322) FIXME: Type '(reverseX: any) => any' is not assignable to... Remove this comment to see the full error message
          onChange={(reverseX: any) => onOptionsChange({ reverseX })}>
        {window.W_L.reverse_order}
        </Switch>
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
        <Switch
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'string' is not assignable to type 'never'.
          data-test="Chart.XAxis.ShowLabels"
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'any' is not assignable to type 'never'.
          defaultChecked={options.xAxis.labels.enabled}
          // @ts-expect-error ts-migrate(2322) FIXME: Type '(enabled: any) => any' is not assignable to ... Remove this comment to see the full error message
          onChange={(enabled: any) => onOptionsChange({ xAxis: { labels: { enabled } } })}>
          {window.W_L.Show_Labels}
        </Switch>
      </Section>
    </React.Fragment>
  );
}

XAxisSettings.propTypes = EditorPropTypes;
