import React ,{useRef,forwardRef, useImperativeHandle}from 'react';
import Input from "antd/lib/input";
import Overlay from "../Overlay";
import SelectSource from "../SelectSource";
import TypingCard from "../TypingCard";

import SendOutlinedIcon from "@ant-design/icons/SendOutlined";
import PauseCircleOutlinedIcon from "@ant-design/icons/PauseCircleOutlined";
const { TextArea } = Input;
const DialogueContent = forwardRef(({
  Charttable,
  confirmLoading,
  onChange,
  sendTableDate,
  loadingMask,
  onUse,
  messages, 
  ChangeScrollTop, 
  loadingState, 
  stopSend, 
  inputMessage, 
  newInputMessage,
  setState, 
  handleSendMessage,
  chat_type,
  retry,
  onOpenKeyClick,
  onSuccess,
  percent,
  databases_type,
},ref) => {
  const selectSourceRef = useRef(null);
  const sourceEdit = (data) => {
    selectSourceRef.current.editTableData(data);
  }
  useImperativeHandle(ref, () => ({
    sourceEdit,
  }));
  return (
    <>
    <div className="dialogue-content-all">
      <div className="dialogue-content-message">
        <div className="dialogue-content-message-auto">
        <SelectSource ref={selectSourceRef} onSuccess={onSuccess} chat_type={chat_type} Charttable={Charttable} confirmLoading={confirmLoading} onChange={onChange} percent={percent}></SelectSource>
            {
          sendTableDate!==1 && messages.length<=0&&Charttable?
          (<Overlay loadingMask={loadingMask} Charttable={Charttable} onUse={onUse}></Overlay>)
          :
          null
            }
          {messages.map((message, index) => (
            <div key={index} className="chat-content" style={{margin:chat_type==="report"?'0 30px':'',marginTop:index===0?"30px":""}}>
                  <TypingCard databases_type={databases_type} chat_type={chat_type} autopilot={message.autopilot} chart={message.chart} logData={message.logData} sender={message.sender} time={message.time} Cardloading={message.Cardloading} source={message.content} index={index} ChangeScrollTop={ChangeScrollTop} retry={retry} />
            </div>
          ))}
        </div>
      </div>
      </div>
     
      {
      chat_type!=="viewConversation" &&
      (
        <div className="main-all" style={{width:chat_type==="report"?"90%":"80%"}}>
           {loadingState && messages.length>0 &&
      <div className="gpt-section-btn-list">
      <button className="gpt-btn-item" onClick={stopSend}>
          <PauseCircleOutlinedIcon className="gpt-btn-item-img"></PauseCircleOutlinedIcon>
          <div className="gpt-btn-item-txt">{window.W_L.stop_generation}</div>
      </button>
      </div>
       }
       {
            chat_type==="autopilot"?
            ""
            :
        <div className="dialogue-content-bottom">
        <div className="open-key" style={{display:"none"}} onClick={onOpenKeyClick}>
        </div>
          
            <TextArea
            bordered={false}
            style={{ resize: 'none', maxHeight: '100px !important', fontSize: '15px', border: 'none !important' }}
            value={inputMessage}
            className="gpt-input"
            onChange={(e) => {
              if (e && e.target) {
                const newValue = e.target.value;
                setState(prevState => ({ ...prevState, inputMessage: newValue ,newInputMessage:newValue}));
              }
            }}
            placeholder={window.W_L.send_me_instructions}
            autoSize={{ minRows: 1, maxRows: 4 }}
            onPressEnter={(e) => {
              e.preventDefault();
              handleSendMessage();
            }}
          />
          <div className="gpt-input-middle">
          <SendOutlinedIcon onClick={handleSendMessage} style={{color:inputMessage ? "" : "#ccc",fontSize:"20px",marginRight:"15px"}}></SendOutlinedIcon>
          </div>
         
        </div>
      }
      </div>
      )
      }
    </>
  );
});

export default DialogueContent;