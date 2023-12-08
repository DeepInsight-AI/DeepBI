import React from "react";
import Button from "antd/lib/button";
import Modal from "antd/lib/modal";
import "./index.css";

const DialogueTop = (props) => {
    const {HolmestableItem,chat_type,Holmestable} = props;
    const close = () => {
        const doDelete = () => {
            props.closeDialogue()
          };
        Modal.confirm({
            title: window.W_L.reset_dialogue,
            content: window.W_L.reset_dialogue_confirm,
            okText: window.W_L.ok_text,
            cancelText: window.W_L.cancel,
            okType: "danger",
            onOk: doDelete,
            onCancel: null,
            maskClosable: true,
            autoFocusButton: null,
          });
    }

  return (
   <>
   {
    Holmestable&&HolmestableItem&& HolmestableItem.label&&
     <div className="dialogue-top"> 
    <div className="dialogue-top-flex">
         <div>{chat_type==="chat"?window.W_L.data_analysis:chat_type==="report"?window.W_L.query_builder:chat_type==="autopilot"?window.W_L.auto_pilot:""}</div>
     </div>
     <div className="dialogue-top-flex">
         <div>{window.W_L.data_source}:</div>
         <span>{HolmestableItem.label}</span>
     </div>
     {
        chat_type!=="viewConversation"&&
        (
            <Button type="primary" size="small" onClick={() => close()} style={{borderRadius: "20px",fontSize: "13px",paddingLeft: "15px",paddingRight: "15px"}} ghost>
             <i className="fa fa-plus m-r-5" aria-hidden="true" />
            {chat_type==="autopilot"?window.W_L.auto_pilot:window.W_L.new_dialogue}
          </Button>
        )
     }
 </div>
   }
   </>
  );
};

export default DialogueTop;