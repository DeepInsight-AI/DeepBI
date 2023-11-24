import { map } from "lodash";
import React from "react";
import { Section, Select } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";

const CohortTimeIntervals = {
  daily: window.W_L.Daily,
  weekly: window.W_L.Weekly,
  monthly: window.W_L.Monthly,
};

const CohortModes = {
  diagonal: window.W_L.Fill_gaps_with_zeros,
  simple: window.W_L.Show_data_as_is,
};

export default function OptionsSettings({ options, onOptionsChange }: any) {
  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Select
          layout="horizontal"
          label={window.W_L.Time_Interval}
          data-test="Cohort.TimeInterval"
          value={options.timeInterval}
          onChange={(timeInterval: any) => onOptionsChange({ timeInterval })}>
          {map(CohortTimeIntervals, (name, value) => (
            // @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message
            <Select.Option key={value} data-test={"Cohort.TimeInterval." + value}>
              {name}
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
            </Select.Option>
          ))}
        </Select>
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Select
          layout="horizontal"
          label={window.W_L.Mode}
          data-test="Cohort.Mode"
          value={options.mode}
          onChange={(mode: any) => onOptionsChange({ mode })}>
          {map(CohortModes, (name, value) => (
            // @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message
            <Select.Option key={value} data-test={"Cohort.Mode." + value}>
              {name}
              {/* @ts-expect-error ts-migrate(2339) FIXME: Property 'Option' does not exist on type '({ class... Remove this comment to see the full error message */}
            </Select.Option>
          ))}
        </Select>
      </Section>
    </React.Fragment>
  );
}

OptionsSettings.propTypes = EditorPropTypes;
