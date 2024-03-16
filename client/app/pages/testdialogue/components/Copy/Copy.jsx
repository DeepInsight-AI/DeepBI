import React, { useContext } from 'react';
import copy from "@/assets/images/copy.png";
import toast from "react-hot-toast";
// import Retry from "@/assets/images/retry.png";
import CopyOutlinedIcon from "@ant-design/icons/CopyOutlined";
import PauseCircleOutlinedIcon from "@ant-design/icons/PauseCircleOutlined";
import "./index.less";
import DialogueContext from '../../context/DialogueContext';
const Copy = props => {
  const { cancelRequest } = useContext(DialogueContext); // 在C中使用useContext

  const { source, message } = props;
  const copyCentent = () => {
    try {
      const input = document.createElement("input");
      input.setAttribute("readonly", "readonly");
      input.setAttribute("value", source);
      document.body.appendChild(input);
      input.select();
      input.setSelectionRange(0, 9999);
      if (document.execCommand("copy")) {
        document.execCommand("copy");
      }
      document.body.removeChild(input);
      toast.success(window.W_L.copy_success);
    } catch (error) {
      toast.error(window.W_L.copy_failed);
    }
  };
  // const cancelRequest = (index) => {
  //   // retry(index)
  // }
  const isBotLoading = message.sender === "bot" && message.Cardloading;
  return (
    <div className={`copy${message.sender}`}>
      {
        <div className="copy-item" onClick={isBotLoading ? () => cancelRequest(message) : copyCentent}>
          {isBotLoading ? (
            <>
              <PauseCircleOutlinedIcon className="gpt-btn-item-img"></PauseCircleOutlinedIcon>
              <div className="copy-text">{window.W_L.stop_generation}</div>
            </>
          ) : (
            <>
              <CopyOutlinedIcon className="gpt-btn-item-img"></CopyOutlinedIcon>
              <div className="copy-text">{window.W_L.copy}</div>
            </>
          )}
        </div>
      }
    </div>
  );
};

export default Copy;
