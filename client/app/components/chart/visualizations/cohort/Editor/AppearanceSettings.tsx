import React from "react";
import { useDebouncedCallback } from "use-debounce";
import { Section, Input, Checkbox, ContextHelp } from "@/components/chart/components/visualizations/editor";
import { EditorPropTypes } from "@/components/chart/visualizations/prop-types";

export default function AppearanceSettings({ options, onOptionsChange }: any) {
  const [debouncedOnOptionsChange] = useDebouncedCallback(onOptionsChange, 200);

  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          layout="horizontal"
          label={window.W_L.Time_Column_Title}
          defaultValue={options.timeColumnTitle}
          onChange={(e: any) => debouncedOnOptionsChange({ timeColumnTitle: e.target.value })}
        />
      </Section>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          layout="horizontal"
          label={window.W_L.People_Column_Title}
          defaultValue={options.peopleColumnTitle}
          onChange={(e: any) => debouncedOnOptionsChange({ peopleColumnTitle: e.target.value })}
        />
      </Section>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          layout="horizontal"
          label={
            <React.Fragment>
              {window.W_L.Stage_Column_Title}
              <ContextHelp placement="topRight" arrowPointAtCenter>
                {/* @ts-expect-error ts-migrate(2322) FIXME: Type 'Element' is not assignable to type 'null | u... Remove this comment to see the full error message */}
                <div>
                  Use <code>{"{{ @ }}"}</code> to insert a stage number
                </div>
              </ContextHelp>
            </React.Fragment>
          }
          defaultValue={options.stageColumnTitle}
          onChange={(e: any) => debouncedOnOptionsChange({ stageColumnTitle: e.target.value })}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          layout="horizontal"
          label={
            <React.Fragment>
              {window.W_L.Number_format}
              <ContextHelp.NumberFormatSpecs />
            </React.Fragment>
          }
          defaultValue={options.numberFormat}
          onChange={(e: any) => debouncedOnOptionsChange({ numberFormat: e.target.value })}
        />
      </Section>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          layout="horizontal"
          label={
            <React.Fragment>
              {window.W_L.Percent_Values_Format}
              <ContextHelp.NumberFormatSpecs />
            </React.Fragment>
          }
          defaultValue={options.percentFormat}
          onChange={(e: any) => debouncedOnOptionsChange({ percentFormat: e.target.value })}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          layout="horizontal"
          label={window.W_L.No_Value_Placeholder}
          defaultValue={options.noValuePlaceholder}
          onChange={(e: any) => debouncedOnOptionsChange({ noValuePlaceholder: e.target.value })}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Checkbox
          defaultChecked={options.showTooltips}
          onChange={event => onOptionsChange({ showTooltips: event.target.checked })}>
          {window.W_L.Show_Tooltips}
        </Checkbox>
      </Section>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Checkbox
          defaultChecked={options.percentValues}
          onChange={event => onOptionsChange({ percentValues: event.target.checked })}>
          {window.W_L.Normalize_values_to_percentage}
        </Checkbox>
      </Section>
    </React.Fragment>
  );
}

AppearanceSettings.propTypes = EditorPropTypes;
