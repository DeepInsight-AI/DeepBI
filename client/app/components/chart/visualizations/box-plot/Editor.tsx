import React from "react";
import { Section, Input } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";

export default function Editor({ options, onOptionsChange }: any) {
  const onXAxisLabelChanged = (xAxisLabel: any) => {
    const newOptions = { ...options, xAxisLabel };
    onOptionsChange(newOptions);
  };

  const onYAxisLabelChanged = (yAxisLabel: any) => {
    const newOptions = { ...options, yAxisLabel };
    onOptionsChange(newOptions);
  };

  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          label={window.W_L.x_axis_label}
          data-test="BoxPlot.XAxisLabel"
          value={options.xAxisLabel}
          onChange={(event: any) => onXAxisLabelChanged(event.target.value)}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          label={window.W_L.y_axis_label}
          data-test="BoxPlot.YAxisLabel"
          value={options.yAxisLabel}
          onChange={(event: any) => onYAxisLabelChanged(event.target.value)}
        />
      </Section>
    </React.Fragment>
  );
}

Editor.propTypes = EditorPropTypes;
