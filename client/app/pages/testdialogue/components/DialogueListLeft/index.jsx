import React, { useState,useEffect} from 'react';
import icon_small from "@/assets/images/icon_small.png";
import './index.css';
import MessageOutlinedIcon from "@ant-design/icons/MessageOutlined";
const DialogueLeftList = (props) => {
  const {chat_type,AutoPilotList} = props;
  const [arr ,setArr] = useState([]);
  const [arr_item ,setArrItem] = useState({});
useEffect(()=>{
  if(chat_type==="autopilot"){
    setArr(AutoPilotList)
  }else{
    setArr(JSON.parse(localStorage.getItem("HoImes_All")) || [])
  }
},[])
const modeSwitch = (item) => () => {
    setArrItem(item)
    props.switchMode(item)
}
  const DiaTop = () => {
    return (
      <div className="dia-top">
        <img src={icon_small} alt="" />
        <span>Holmes</span>
      </div>
    )
  }
  const DiaBottom = () => {
    return (
      <div className="dia-content">
      {arr.map((item,index)=>{
        return <div className="flex-d dia-info" key={index}>
        <div className="dia-chats" >
        
          <div className="dia-chat" onClick={modeSwitch(item)}>
            <MessageOutlinedIcon style={{color:arr_item.uuid===item.uuid?"#4974d1":"#fff",fontSize:"21px"}} ></MessageOutlinedIcon>
              <div className="dia-chat-name">{chat_type==="autopilot"?item.report_name:item.messages[1].content || item.title}</div>
          </div>
        </div>
        </div>

      })}
      </div>
    )
  }
  return (
     
        <>
         {
          arr && arr.length>0&&
        <div className="dia-main">
          <DiaTop></DiaTop>
        <DiaBottom></DiaBottom>
      </div>
      }
        </>
      
  );
};

export default DialogueLeftList;