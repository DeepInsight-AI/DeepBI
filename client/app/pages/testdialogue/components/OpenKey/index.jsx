import React, { useState, forwardRef, useImperativeHandle,useRef } from "react";
import Modal from "antd/lib/modal";
import Input from "antd/lib/input";
import { axios } from "@/services/axios";
import notification from "@/services/notification";
const OpenKey = forwardRef((props, ref) => {
  const { onSuccess,setConfirmLoadingState,updateHolmestableDate} = props;
  const [visible, setVisible] = useState(false);
    const [KeyValue ,setKeyValue] = useState("");
  const showModal = () => {
    setVisible(true);
  };
  const hideModal = () => {
    setVisible(false);
  };
  const onOk =async () => {

  };

  useImperativeHandle(ref, () => ({
    showModal,
    hideModal,
  }));

  return (
    <div className="addSql-content">
      <Modal
        title="Set Your Own API Keys"
        visible={visible}
        onOk={onOk}
        onCancel={() => { setVisible(false)}}
        closeIcon={true}
        centered
      >
        <div className="data-sheet">
            <span>OpenAI:</span>
            <Input
            placeholder=""
            value={KeyValue}
            />
        </div>
      </Modal>
    </div>
  );
});

export default OpenKey;
