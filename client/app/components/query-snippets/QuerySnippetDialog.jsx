import { isNil, get } from "lodash";
import React, { useCallback } from "react";
import PropTypes from "prop-types";
import Button from "antd/lib/button";
import Modal from "antd/lib/modal";
import DynamicForm from "@/components/dynamic-form/DynamicForm";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import { useUniqueId } from "@/lib/hooks/useUniqueId";

function QuerySnippetDialog({ querySnippet, dialog, readOnly }) {
  const handleSubmit = useCallback(
    (values, successCallback, errorCallback) => {
      const querySnippetId = get(querySnippet, "id");

      if (isNil(values.description)) {
        values.description = "";
      }

      dialog
        .close(querySnippetId ? { id: querySnippetId, ...values } : values)
        .then(() => successCallback(window.W_L.save_success))
        .catch(() => errorCallback(window.W_L.save_failed));
    },
    [dialog, querySnippet]
  );

  const isEditing = !!get(querySnippet, "id");

  const formFields = [
    { name: "trigger", title: window.W_L.code, type: "text", required: true, autoFocus: !isEditing },
    { name: "description", title: window.W_L.desc, type: "text" },
    { name: "snippet", title: window.W_L.query_code, type: "ace", required: true },
  ].map(field => ({ ...field, readOnly, initialValue: get(querySnippet, field.name, "") }));

  const querySnippetsFormId = useUniqueId("querySnippetForm");

  return (
    <Modal
      {...dialog.props}
      title={isEditing ? querySnippet.trigger : window.W_L.create_query}
      footer={[
        <Button key="cancel" {...dialog.props.cancelButtonProps} onClick={dialog.dismiss}>
          {readOnly ? window.W_L.close : window.W_L.cancel}
        </Button>,
        !readOnly && (
          <Button
            key="submit"
            {...dialog.props.okButtonProps}
            disabled={readOnly || dialog.props.okButtonProps.disabled}
            htmlType="submit"
            type="primary"
            form={querySnippetsFormId}
            data-test="SaveQuerySnippetButton">
            {isEditing ? window.W_L.save : window.W_L.create}
          </Button>
        ),
      ]}
      wrapProps={{
        "data-test": "QuerySnippetDialog",
      }}>
      <DynamicForm
        id={querySnippetsFormId}
        fields={formFields}
        onSubmit={handleSubmit}
        hideSubmitButton
        feedbackIcons
      />
    </Modal>
  );
}

QuerySnippetDialog.propTypes = {
  dialog: DialogPropType.isRequired,
  querySnippet: PropTypes.object,
  readOnly: PropTypes.bool,
};

QuerySnippetDialog.defaultProps = {
  querySnippet: null,
  readOnly: false,
};

export default wrapDialog(QuerySnippetDialog);
