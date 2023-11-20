import { includes, words, capitalize, clone, isNull } from "lodash";
import React, { useState, useEffect } from "react";
import PropTypes from "prop-types";
import Checkbox from "antd/lib/checkbox";
import Modal from "antd/lib/modal";
import Form from "antd/lib/form";
import Button from "antd/lib/button";
import Select from "antd/lib/select";
import Input from "antd/lib/input";
import Divider from "antd/lib/divider";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import QuerySelector from "@/components/QuerySelector";
import { Query } from "@/services/query";
import { useUniqueId } from "@/lib/hooks/useUniqueId";

const { Option } = Select;
const formItemProps = { labelCol: { span: 6 }, wrapperCol: { span: 16 } };

function getDefaultTitle(text) {
  return capitalize(words(text).join(" ")); // humanize
}

function isTypeDateRange(type) {
  return /-range/.test(type);
}

function joinExampleList(multiValuesOptions) {
  const { prefix, suffix } = multiValuesOptions;
  return [window.W_L.value+"1", window.W_L.value+"2", window.W_L.value+"3"].map(value => `${prefix}${value}${suffix}`).join(",");
}

function NameInput({ name, type, onChange, existingNames, setValidation }) {
  let helpText = "";
  let validateStatus = "";

  if (!name) {
    helpText = window.W_L.params_name;
    setValidation(false);
  } else if (includes(existingNames, name)) {
    helpText = window.W_L.params_ready_exist;
    setValidation(false);
    validateStatus = "error";
  } else {
    if (isTypeDateRange(type)) {
      helpText = (
        <React.Fragment>
          {window.W_L.query_show_as}{" "}
          <code style={{ display: "inline-block", color: "inherit" }}>{`{{${name}.start}} {{${name}.end}}`}</code>
        </React.Fragment>
      );
    }
    setValidation(true);
  }

  return (
    <Form.Item required label={window.W_L.name} help={helpText} validateStatus={validateStatus} {...formItemProps}>
      <Input onChange={e => onChange(e.target.value)} autoFocus />
    </Form.Item>
  );
}

NameInput.propTypes = {
  name: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  existingNames: PropTypes.arrayOf(PropTypes.string).isRequired,
  setValidation: PropTypes.func.isRequired,
  type: PropTypes.string.isRequired,
};

function EditParameterSettingsDialog(props) {
  const [param, setParam] = useState(clone(props.parameter));
  const [isNameValid, setIsNameValid] = useState(true);
  const [initialQuery, setInitialQuery] = useState();

  const isNew = !props.parameter.name;

  // fetch query by id
  useEffect(() => {
    const queryId = props.parameter.queryId;
    if (queryId) {
      Query.get({ id: queryId }).then(setInitialQuery);
    }
  }, [props.parameter.queryId]);

  function isFulfilled() {
    // name
    if (!isNameValid) {
      return false;
    }

    // title
    if (param.title === "") {
      return false;
    }

    // query
    if (param.type === "query" && !param.queryId) {
      return false;
    }

    return true;
  }

  function onConfirm() {
    // update title to default
    if (!param.title) {
      // forced to do this cause param won't update in time for save
      param.title = getDefaultTitle(param.name);
      setParam(param);
    }

    props.dialog.close(param);
  }

  const paramFormId = useUniqueId("paramForm");

  return (
    <Modal
      {...props.dialog.props}
      title={isNew ? window.W_L.new_params : param.name}
      width={600}
      footer={[
        <Button key="cancel" onClick={props.dialog.dismiss}>
          {window.W_L.cancel}
        </Button>,
        <Button
          key="submit"
          htmlType="submit"
          disabled={!isFulfilled()}
          type="primary"
          form={paramFormId}
          data-test="SaveParameterSettings">
          {isNew ? window.W_L.new_params : window.W_L.ok_text}
        </Button>,
      ]}>
      <Form layout="horizontal" onFinish={onConfirm} id={paramFormId}>
        {isNew && (
          <NameInput
            name={param.name}
            onChange={name => setParam({ ...param, name })}
            setValidation={setIsNameValid}
            existingNames={props.existingParams}
            type={param.type}
          />
        )}
        <Form.Item required label={window.W_L.title} {...formItemProps}>
          <Input
            value={isNull(param.title) ? getDefaultTitle(param.name) : param.title}
            onChange={e => setParam({ ...param, title: e.target.value })}
            data-test="ParameterTitleInput"
          />
        </Form.Item>
        <Form.Item label={window.W_L.type} {...formItemProps}>
          <Select value={param.type} onChange={type => setParam({ ...param, type })} data-test="ParameterTypeSelect">
            <Option value="text" data-test="TextParameterTypeOption">
              {window.W_L.text}
            </Option>
            <Option value="number" data-test="NumberParameterTypeOption">
               {window.W_L.number}
            </Option>
            <Option value="enum">{window.W_L.select}</Option>
            <Option value="query">{window.W_L.query_result}</Option>
            <Option disabled key="dv1">
              <Divider className="select-option-divider" />
            </Option>
            <Option value="date" data-test="DateParameterTypeOption">
				{window.W_L.date_day}
            </Option>
            <Option value="datetime-local" data-test="DateTimeParameterTypeOption">
              {window.W_L.date_datetime}
            </Option>
            <Option value="datetime-with-seconds">{window.W_L.date_time_second}</Option>
            <Option disabled key="dv2">
              <Divider className="select-option-divider" />
            </Option>
            <Option value="date-range" data-test="DateRangeParameterTypeOption">
              {window.W_L.date_range_day}
            </Option>
            <Option value="datetime-range"> {window.W_L.date_range_datetime}</Option>
            <Option value="datetime-range-with-seconds">{window.W_L.date_range_time_second}</Option>
          </Select>
        </Form.Item>
        {param.type === "enum" && (
          <Form.Item label={window.W_L.value} help={window.W_L.list_select_help} {...formItemProps}>
            <Input.TextArea
              rows={3}
              value={param.enumOptions}
              onChange={e => setParam({ ...param, enumOptions: e.target.value })}
            />
          </Form.Item>
        )}
        {param.type === "query" && (
          <Form.Item label={window.W_L.query_result} help={window.W_L.wait_query} {...formItemProps}>
            <QuerySelector
              selectedQuery={initialQuery}
              onChange={q => setParam({ ...param, queryId: q && q.id })}
              type="select"
            />
          </Form.Item>
        )}
        {(param.type === "enum" || param.type === "query") && (
          <Form.Item className="m-b-0" label=" " colon={false} {...formItemProps}>
            <Checkbox
              defaultChecked={!!param.multiValuesOptions}
              onChange={e =>
                setParam({
                  ...param,
                  multiValuesOptions: e.target.checked
                    ? {
                        prefix: "",
                        suffix: "",
                        separator: ",",
                      }
                    : null,
                })
              }
              data-test="AllowMultipleValuesCheckbox">
              {window.W_L.allow_multi}
            </Checkbox>
          </Form.Item>
        )}
        {(param.type === "enum" || param.type === "query") && param.multiValuesOptions && (
          <Form.Item
            label={window.W_L.quotation}
            help={
              <React.Fragment>
               {window.W_L.replace_query_info}: <code>{joinExampleList(param.multiValuesOptions)}</code>
              </React.Fragment>
            }
            {...formItemProps}>
            <Select
              value={param.multiValuesOptions.prefix}
              onChange={quoteOption =>
                setParam({
                  ...param,
                  multiValuesOptions: {
                    ...param.multiValuesOptions,
                    prefix: quoteOption,
                    suffix: quoteOption,
                  },
                })
              }
              data-test="QuotationSelect">
              <Option value="">{window.W_L.no_quotation}({window.W_L.default})</Option>
              <Option value="'">{window.W_L.single_quotation}</Option>
              <Option value={'"'} data-test="DoubleQuotationMarkOption">
                {window.W_L.double_quotation}
              </Option>
            </Select>
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}

EditParameterSettingsDialog.propTypes = {
  parameter: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
  dialog: DialogPropType.isRequired,
  existingParams: PropTypes.arrayOf(PropTypes.string),
};

EditParameterSettingsDialog.defaultProps = {
  existingParams: [],
};

export default wrapDialog(EditParameterSettingsDialog);
