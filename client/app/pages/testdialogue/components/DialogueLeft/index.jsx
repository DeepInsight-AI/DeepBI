import React, { useRef,useCallback,useState,useEffect} from 'react';
import { first } from "lodash";
import settingsMenu from "@/services/settingsMenu";
import Button from "antd/lib/button";
import icon_small from "@/assets/images/icon_small.png";
import analysis from "@/assets/images/analysis.png";
import { Auth, currentUser } from "@/services/auth";
import Link from "@/components/Link";
import Dropdown from "antd/lib/dropdown";
import Menu from "antd/lib/menu";
import Resize from "../Resize"
import {lockReconnect,websocket,lockReconnectEvent} from "../Dialogue/websocket.js"
import './index.css';

const DialogueLeft = (props) => {
  const {switchMode ,chat_type} = props;
  const firstSettingsTab = first(settingsMenu.getAvailableItems());
  const mobileNavbarContainerRef = useRef();
  const [ReSize,setReSize] = useState(false)
  const [socketType,setSocketType] = useState(0)
  const getMobileNavbarPopupContainer = useCallback(() => mobileNavbarContainerRef.current, []);
  const arr=[{
    id : 1,
    type:"chat",
    title:"Chats",
    path:"/",
    analysisList:[{
      title:"chat",
      icon:"zmdi-comment-outline"
    }]
  },
  {
    id : 2,
    type:"report",
    title:"Queries",
    path:"/report-route",
    analysisList:[{
      title:"Queries",
      icon:"zmdi-chart"
    }]
  },
  {
    id : 4,
    type:"queries",
    title:"queries",
    path:"/queriesList",
    analysisList:[{
      title:"queriesList",
      icon:"zmdi-format-list-bulleted"
    }]
  },
  {
    id : 3,
    type:"dashboard",
    title:"Dashboards",
    path:"/dashboard-route",
    analysisList:[{
      title:"Dashboards",
      icon:"zmdi-view-dashboard"
    }]
  },

]
useEffect(() => {
  const handleLockReconnectChange = (newLockReconnect) => {
    setSocketType(newLockReconnect)
  };

  lockReconnectEvent.on('change', handleLockReconnectChange);

  return () => {
    lockReconnectEvent.off('change', handleLockReconnectChange);
  };
}, []);
  const modeSwitch = (item) => () => {
    props.switchMode(item)
  }
  const resize = (type) => {
    setReSize(type)
  }
  const DiaTop = () => {
    return (
      <div className="dia-top">
        {!ReSize&&<img src={icon_small} alt="" />}
        <span>DeepBI</span>
      </div>
    )
  }
  const DropdownMenu = () => {
    return (
      <Dropdown
      overlayStyle={{ minWidth: 200 }}
      trigger={["click"]}
      getPopupContainer={getMobileNavbarPopupContainer} // so the overlay menu stays with the fixed header when page scrolls
      overlay={
        <Menu mode="vertical" theme="dark" selectable={false} className="mobile-navbar-menu" style={{background: "#fff",borderRadius: "10px"}}>
        {firstSettingsTab && (
            <Menu.Item key="settings">
              <Link href={firstSettingsTab.path} target="_blank" style={{color:"#333"}}>{window.W_L.setting}</Link>
            </Menu.Item>
          )}
           <Menu.Item key="logout" onClick={() => Auth.logout()} style={{color:"#333"}}>
            {window.W_L.log_out}
          </Menu.Item>
        </Menu>
      }>
        <div style={{padding: "10px",position: "relative",display:"flex",alignItems: "center"}}>
         <img src={currentUser.profile_image_url} alt="" />
         {!ReSize?
          <div style={{marginLeft: "10px"}}>

          <div className="socket-type">
          <div className="socket-type-bg" style={{"background":socketType==0?"red":socketType==1?"green":"yellow"}}></div>
           <span>{socketType==0?"":socketType==1?"":""}</span>
          </div>
          <span >{currentUser.email}</span>
          </div>
          :
          <div className="socket-type-bg bg-resize" style={{"background":socketType==0?"red":socketType==1?"green":"yellow"}}></div>
        }
      {/* {!ReSize&&} */}

      </div>
      </Dropdown>
    )
  }
  const DiaBottom = () => {
    return (
      <div className="dia-content">
      {arr.map((item,index)=>{
        return <div className="flex-d h-30 dia-info" key={index}>
        <div className="dia-title">
        <span>{item.title}</span>
        <span>+</span>
        </div>
        <div className="dia-chats" >
          {item.analysisList.map((i,index)=>{
            return <Link href={item.path}>
            <div className="dia-chat" key={index} onClick={modeSwitch(item)}>
            <i style={{borderColor:chat_type==item.type?"red":"#fff"}} class={`zmdi ${i.icon}`}></i>
              <div className="dia-chat-name">{i.title}</div>
            {/* <div class="dia-chat-del"></div> */}
          </div>
          </Link>
          })}
        </div>
        </div>

      })}
      {/* Queries  */}
      <div className="dia-queries">
      <Link href="/queries">
      <div className="dia-title">
        <span>QueriesAll</span>
        <span> 》 </span>
        </div>
        </Link>
      </div>
      {/* logout */}
      <div className="dia-bottom" ref={mobileNavbarContainerRef}>
        <DropdownMenu />

        </div>
      </div>
    )
  }
  const ReSizeWeight = () => {
    return (
      <div className="dia-resize">
        <DiaTop></DiaTop>
        <div className="dia-content">
        <div className="dia-bottom" ref={mobileNavbarContainerRef}>
        <DropdownMenu></DropdownMenu>
        </div>
      </div>
      </div>
    )
  }
  return (
      <div className={`dia-main ${ReSize?'dia-main-resize':""}`}>
        <Resize resize={resize}></Resize>
        {
          ReSize?<ReSizeWeight></ReSizeWeight>
          :
          <>
          <DiaTop></DiaTop>
        <DiaBottom></DiaBottom>
          </>
        }


      </div>
  );
};

export default DialogueLeft;
