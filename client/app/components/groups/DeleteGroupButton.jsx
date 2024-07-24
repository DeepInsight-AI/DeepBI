import { isString } from "lodash";
import React from "react";
import PropTypes from "prop-types";
import Button from "antd/lib/button";
import Modal from "antd/lib/modal";
import Tooltip from "@/components/Tooltip";
import notification from "@/services/notification";
import Group from "@/services/group";

function deleteGroup(event, group, onGroupDeleted) {
  Modal.confirm({
    title: window.W_L.del_role,
    content:window.W_L.del_role_confirm,
    okText: window.W_L.ok_text,
    okType: "danger",
    cancelText: window.W_L.cancel,
    onOk: () => {
      Group.delete(group).then(() => {
        notification.success(window.W_L.del_role_success);
        onGroupDeleted();
      });
    },
  });
}

export default function DeleteGroupButton({ group, title, onClick, children, ...props }) {
  if (!group) {
    return null;
  }
  const button = (
    <Button {...props} type="danger" onClick={event => deleteGroup(event, group, onClick)}>
      {children}
    </Button>
  );

  if (isString(title) && title !== "") {
    return (
      <Tooltip placement="top" title={title} mouseLeaveDelay={0}>
        {button}
      </Tooltip>
    );
  }

  return button;
}

DeleteGroupButton.propTypes = {
  group: PropTypes.object, // eslint-disable-line react/forbid-prop-types
  title: PropTypes.string,
  onClick: PropTypes.func,
  children: PropTypes.node,
};

DeleteGroupButton.defaultProps = {
  group: null,
  title: null,
  onClick: () => {},
  children: null,
};
