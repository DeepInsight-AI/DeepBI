import React, { useCallback } from "react";
import PropTypes from "prop-types";
import recordEvent from "@/services/recordEvent";
import Checkbox from "antd/lib/checkbox";
import Tooltip from "@/components/Tooltip";

export default function AutoLimitCheckbox({ available, checked, onChange }) {
  const handleClick = useCallback(() => {
    recordEvent("checkbox_auto_limit", "screen", "query_editor", { state: !checked });
    onChange(!checked);
  }, [checked, onChange]);

  let tooltipMessage = null;
  if (!available) {
    tooltipMessage = window.W_L.not_limit;
  } else {
    tooltipMessage = window.W_L.limit_1000;
  }

  return (
    <Tooltip placement="top" title={tooltipMessage}>
      <Checkbox
        className="query-editor-controls-checkbox"
        disabled={!available}
        onClick={handleClick}
        checked={available && checked}>
        {window.W_L.return_1000}
      </Checkbox>
    </Tooltip>
  );
}

AutoLimitCheckbox.propTypes = {
  available: PropTypes.bool,
  checked: PropTypes.bool.isRequired,
  onChange: PropTypes.func.isRequired,
};
