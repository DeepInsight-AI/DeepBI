import React from "react";
import copy from "@/assets/images/copy.png";
import toast from "react-hot-toast";
// import Retry from "@/assets/images/retry.png";
import "./index.less";

const Copy = props => {
  const { sender, source } = props;
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
  // const retryCentent = () => {
  //   retry(index)
  // }
  return (
    <div className={`copy${sender}`}>
      {/* <div className="copt-item" onClick={copyCentent}>
        <img src={copy} alt="copy" />
        <div className="copy-text">{window.W_L.copy}</div>
      </div> */}
      {/* {
            sender=="bot"?
            <div className="copt-item" onClick={retryCentent}>
            <img src={Retry} alt="" />
            <div className="copy-text">{window.W_L.retry}</div>
        </div>
        :
        null
        } */}
    </div>
  );
};

export default Copy;
