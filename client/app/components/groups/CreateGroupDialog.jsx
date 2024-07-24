import React from "react";
import Modal from "antd/lib/modal";
import Input from "antd/lib/input";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";

class CreateGroupDialog extends React.Component {
  static propTypes = {
    dialog: DialogPropType.isRequired,
  };

  state = {
    name: "",
  };

  save = () => {
    this.props.dialog.close({
      name: this.state.name,
    });
  };

  render() {
    const { dialog } = this.props;
    return (
      <Modal {...dialog.props} title={window.W_L.create_role} okText={window.W_L.create} cancelText={window.W_L.cancel} onOk={() => this.save()}>
        <Input
          className="form-control"
          defaultValue={this.state.name}
          onChange={event => this.setState({ name: event.target.value })}
          onPressEnter={() => this.save()}
          placeholder={window.W_L.role_name}
          aria-label={window.W_L.role_name}
          autoFocus
        />
      </Modal>
    );
  }
}

export default wrapDialog(CreateGroupDialog);
