import React from "react";

export default function Editor() {
  return (
    <React.Fragment>
      <p>{window.W_L.This_visualization_expects_the_query_result_to_have_rows_in_one_of_the_following_formats}：</p>
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
          <strong>value</strong> -{window.W_L.number_of_times_this_sequence_occurred}
        </li>
      </ul>
    </React.Fragment>
  );
}
