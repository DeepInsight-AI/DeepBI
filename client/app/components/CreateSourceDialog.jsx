import React from "react";
import PropTypes from "prop-types";
import { isEmpty, toUpper, includes, get, uniqueId } from "lodash";
import Button from "antd/lib/button";
import List from "antd/lib/list";
import Modal from "antd/lib/modal";
import Input from "antd/lib/input";
import Steps from "antd/lib/steps";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import Link from "@/components/Link";
import { PreviewCard } from "@/components/PreviewCard";
import EmptyState from "@/components/items-list/components/EmptyState";
import DynamicForm from "@/components/dynamic-form/DynamicForm";
import helper from "@/components/dynamic-form/dynamicFormHelper";
import HelpTrigger, { TYPES as HELP_TRIGGER_TYPES } from "@/components/HelpTrigger";

const { Step } = Steps;
const { Search } = Input;

const StepEnum = {
  SELECT_TYPE: 0,
  CONFIGURE_IT: 1,
  DONE: 2,
};

class CreateSourceDialog extends React.Component {
  static propTypes = {
    dialog: DialogPropType.isRequired,
    types: PropTypes.arrayOf(PropTypes.object),
    sourceType: PropTypes.string.isRequired,
    imageFolder: PropTypes.string.isRequired,
    helpTriggerPrefix: PropTypes.string,
    onCreate: PropTypes.func.isRequired,
  };

  static defaultProps = {
    types: [],
    helpTriggerPrefix: null,
  };

  state = {
    searchText: "",
    selectedType: null,
    savingSource: false,
    currentStep: StepEnum.SELECT_TYPE,
  };

  formId = uniqueId("sourceForm");

  selectType = selectedType => {
    this.setState({ selectedType, currentStep: StepEnum.CONFIGURE_IT });
  };

  resetType = () => {
    if (this.state.currentStep === StepEnum.CONFIGURE_IT) {
      this.setState({ searchText: "", selectedType: null, currentStep: StepEnum.SELECT_TYPE });
    }
  };

  createSource = (values, successCallback, errorCallback) => {
    const { selectedType, savingSource } = this.state;
    if (!savingSource) {
      this.setState({ savingSource: true, currentStep: StepEnum.DONE });
      this.props
        .onCreate(selectedType, values)
        .then(data => {
          successCallback(window.W_L.save_success);
          this.props.dialog.close({ success: true, data });
        })
        .catch(error => {
          this.setState({ savingSource: false, currentStep: StepEnum.CONFIGURE_IT });
          errorCallback(get(error, "response.data.message", window.W_L.save_failed));
        });
    }
  };
  renderTypeSelector() {
    const { types } = this.props;
    const { searchText } = this.state;
    const filteredTypes = types.filter(
      type => isEmpty(searchText) || includes(type.name.toLowerCase(), searchText.toLowerCase())
    );
    return (
      <div className="m-t-10"> 
        <Search
          placeholder={window.W_L.search}
          aria-label={window.W_L.search}
          onChange={e => this.setState({ searchText: e.target.value })}
          autoFocus
          data-test="SearchSource"
        />
        <div className="scrollbox p-5 m-t-10" style={{ minHeight: "30vh", maxHeight: "40vh" }}>
          {isEmpty(filteredTypes) ? (
            <EmptyState className="" />
          ) : (
            <List size="small" dataSource={filteredTypes} renderItem={item => this.renderItem(item)} />
          )}
        </div>
      </div>
    );
  }

  renderForm() {
    const { imageFolder, helpTriggerPrefix } = this.props;
    const { selectedType } = this.state;
    const fields = helper.getFields(selectedType);
    const helpTriggerType = `${helpTriggerPrefix}${toUpper(selectedType.type)}`;
    return (
      <div>
        <div className="d-flex justify-content-center align-items-center">
          <img className="p-5" src={`${imageFolder}/${selectedType.type}.png`} alt={selectedType.name} width="48" />
          <h4 className="m-0">{selectedType.name}</h4>
        </div>
        <div className="text-right">
          {HELP_TRIGGER_TYPES[helpTriggerType] && (
            <HelpTrigger className="f-13" type={helpTriggerType}>
              {window.W_L.setting_info} <i className="fa fa-question-circle" aria-hidden="true" />
              <span className="sr-only">({window.W_L.help})</span>
            </HelpTrigger>
          )}
        </div>
        <DynamicForm id={this.formId} fields={fields} onSubmit={this.createSource} feedbackIcons hideSubmitButton />
        {selectedType.type === "databricks" && (
          <small>
            {window.W_L.jdbc_info}{" "}
            <Link href="https://databricks.com/spark/odbc-driver-download" target="_blank" rel="noopener noreferrer">
              {window.W_L.jdbc_download_name}
            </Link>
            .
          </small>
        )}
      </div>
    );
  }

  renderItem(item) {
    const { imageFolder } = this.props;
    return (
      <List.Item className="p-l-10 p-r-10 clickable" onClick={() => this.selectType(item)}>
        <PreviewCard
          title={item.name}
          imageUrl={`${imageFolder}/${item.type}.png`}
          roundedImage={false}
          data-test="PreviewItem"
          data-test-type={item.type}>
          <i className="fa fa-angle-double-right" aria-hidden="true" />
        </PreviewCard>
      </List.Item>
    );
  }

  render() {
    const { currentStep, savingSource } = this.state;
    const { dialog, sourceType } = this.props;
    return (
      <Modal
        {...dialog.props}
        title={window.W_L.create + ` ${sourceType}`}
        footer={
          currentStep === StepEnum.SELECT_TYPE
            ? [
                <Button key="cancel" onClick={() => dialog.dismiss()} data-test="CreateSourceCancelButton">
                  {window.W_L.cancel}
                </Button>,
                <Button key="submit" type="primary" disabled>
                  {window.W_L.create}
                </Button>,
              ]
            : [
                <Button key="previous" onClick={this.resetType}>
                  {window.W_L.previous}
                </Button>,
                <Button
                  key="submit"
                  htmlType="submit"
                  form={this.formId}
                  type="primary"
                  loading={savingSource}
                  data-test="CreateSourceSaveButton">
                  {window.W_L.create}
                </Button>,
              ]
        }>
        <div data-test="CreateSourceDialog">
          <Steps className="hidden-xs m-b-10" size="small" current={currentStep} progressDot>
            {currentStep === StepEnum.CONFIGURE_IT ? (
              <Step title={window.W_L.type_select} className="clickable" onClick={this.resetType} />
            ) : (
              <Step title={window.W_L.type} />
            )}
            <Step title={window.W_L.setting} />
            <Step title={window.W_L.done} />
          </Steps>
          {currentStep === StepEnum.SELECT_TYPE && this.renderTypeSelector()}
          {currentStep !== StepEnum.SELECT_TYPE && this.renderForm()}
        </div>
      </Modal>
    );
  }
}

export default wrapDialog(CreateSourceDialog);
