import React from "react";
import { Section } from "@/components/chart/components/visualizations/editor";

export default function Editor() {
  return (
    <React.Fragment>
      <p>{window.W_L.This_visualization_expects_the_query_result_to_have_rows_in_one_of_the_following_formats}：</p>
      {/* @ts-expect-error ts-migrate(2746) FIXME: This JSX tag's 'children' prop expects a single ch... Remove this comment to see the full error message */}
      <Section>
        <p>
          <strong>{window.W_L.Option}1：</strong>
        </p>
        <ul>
          <li>
            <strong>sequence</strong> - {window.W_L.sequence_id}
          </li>
          <li>
            <strong>stage</strong> - {window.W_L.what_stage_in_sequence_this_is_1_2}
          </li>
          <li>
            <strong>node</strong> - {window.W_L.stage_name}
          </li>
          <li>
            <strong>value</strong> - {window.W_L.number_of_times_this_sequence_occurred}
          </li>
        </ul>
      </Section>
      {/* @ts-expect-error ts-migrate(2746) FIXME: This JSX tag's 'children' prop expects a single ch... Remove this comment to see the full error message */}
      <Section>
        <p>
          <strong>{window.W_L.Option}2：</strong>
        </p>
        <ul>
          <li>
            <strong>stage1</strong> - {window.W_L.stage_1_value}
          </li>
          <li>
            <strong>stage2</strong> - {window.W_L.stage_2_value_or_null}
          </li>
          <li>
            <strong>stage3</strong> - {window.W_L.stage_3_value_or_null}
          </li>
          <li>
            <strong>stage4</strong> - {window.W_L.stage_4_value_or_null}
          </li>
          <li>
            <strong>stage5</strong> - {window.W_L.stage_5_value_or_null}
          </li>
          <li>
            <strong>value</strong> - {window.W_L.number_of_times_this_sequence_occurred}
          </li>
        </ul>
      </Section>
    </React.Fragment>
  );
}
