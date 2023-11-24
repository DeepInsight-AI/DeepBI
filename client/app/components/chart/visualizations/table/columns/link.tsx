import { extend, trim } from "lodash";
import React from "react";
import { useDebouncedCallback } from "use-debounce";
import { Section, Input, Checkbox, ContextHelp } from "@/components/chart/components/visualizations/editor";
import { formatSimpleTemplate } from "@/components/chart/lib/value-format";

type Props = {
  column: {
    name: string;
    linkUrlTemplate?: string;
    linkTextTemplate?: string;
    linkTitleTemplate?: string;
    linkOpenInNewTab?: boolean;
  };
  onChange: (...args: any[]) => any;
};

function Editor({ column, onChange }: Props) {
  const [onChangeDebounced] = useDebouncedCallback(onChange, 200);

  return (
    <React.Fragment>
      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          label={window.W_L.URL_template}
          data-test="Table.ColumnEditor.Link.UrlTemplate"
          defaultValue={column.linkUrlTemplate}
          onChange={(event: any) => onChangeDebounced({ linkUrlTemplate: event.target.value })}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          label={window.W_L.Text_template}
          data-test="Table.ColumnEditor.Link.TextTemplate"
          defaultValue={column.linkTextTemplate}
          onChange={(event: any) => onChangeDebounced({ linkTextTemplate: event.target.value })}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Input
          label={window.W_L.Title_template}
          data-test="Table.ColumnEditor.Link.TitleTemplate"
          defaultValue={column.linkTitleTemplate}
          onChange={(event: any) => onChangeDebounced({ linkTitleTemplate: event.target.value })}
        />
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        <Checkbox
          data-test="Table.ColumnEditor.Link.OpenInNewTab"
          checked={column.linkOpenInNewTab}
          onChange={event => onChange({ linkOpenInNewTab: event.target.checked })}>
          {window.W_L.Open_in_new_tab}
        </Checkbox>
      </Section>

      {/* @ts-expect-error ts-migrate(2745) FIXME: This JSX tag's 'children' prop expects type 'never... Remove this comment to see the full error message */}
      <Section>
        {/* @ts-expect-error ts-migrate(2746) FIXME: This JSX tag's 'children' prop expects a single ch... Remove this comment to see the full error message */}
        <ContextHelp
          placement="topLeft"
          arrowPointAtCenter
          // @ts-expect-error ts-migrate(2322) FIXME: Type 'Element' is not assignable to type 'null | u... Remove this comment to see the full error message
          icon={<span style={{ cursor: "default" }}>Format specs {ContextHelp.defaultIcon}</span>}>
          <div>
          {window.W_L.All_columns_can_be_referenced_using}<code>{"{{ column_name }}"}</code> {window.W_L.syntax}
          </div>
          <div>
          {window.W_L.Use} <code>{"{{ @ }}"}</code> {window.W_L.to_reference_current_this_column}
          </div>
          <div>{window.W_L.This_syntax_is_applicable_to_URL_Text_and_Title_options}</div>
        </ContextHelp>
      </Section>
    </React.Fragment>
  );
}

export default function initLinkColumn(column: any) {
  function prepareData(row: any) {
    row = extend({ "@": row[column.name] }, row);

    const href = trim(formatSimpleTemplate(column.linkUrlTemplate, row));
    if (href === "") {
      return {};
    }

    const title = trim(formatSimpleTemplate(column.linkTitleTemplate, row));
    const text = trim(formatSimpleTemplate(column.linkTextTemplate, row));

    const result = {
      href,
      text: text !== "" ? text : href,
    };

    if (title !== "") {
      // @ts-expect-error ts-migrate(2339) FIXME: Property 'title' does not exist on type '{ href: s... Remove this comment to see the full error message
      result.title = title;
    }
    if (column.linkOpenInNewTab) {
      // @ts-expect-error ts-migrate(2339) FIXME: Property 'target' does not exist on type '{ href: ... Remove this comment to see the full error message
      result.target = "_blank";
    }

    return result;
  }

  function LinkColumn({ row }: any) {
    // @ts-expect-error ts-migrate(2339) FIXME: Property 'text' does not exist on type '{}'.
    // eslint-disable-line react/prop-types
    const { text, ...props } = prepareData(row);
    return <a {...props}>{text}</a>;
  }

  LinkColumn.prepareData = prepareData;

  return LinkColumn;
}

initLinkColumn.friendlyName = window.W_L.Link;
initLinkColumn.Editor = Editor;
