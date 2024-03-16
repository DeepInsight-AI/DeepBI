import React, { useRef, useEffect, useState, useMemo } from "react";
import { PropTypes } from "prop-types";
import { currentUser } from "@/services/auth";
import EChartsChart from "../Echarts/Echarts";
import LogWorkflow from "../LogWorkflow"
import Copy from "../Copy/Copy.jsx";
import AutoPilotInfo from "../AutoPilotInfo/AutoPilotInfo.jsx";
import "./index.less";
import icon_small from "@/assets/images/icon_small.png";

const TypingCard = (props) => {
  const {databases_type, chat_type, index,message,source, ChangeScrollTop } = props;
  const sourceEl = useRef();
  const [sourceText, setSourceText] = useState("");
  const [showComponent, setShowComponent] = useState(false);


  useEffect(() => {
    const reg = /\n/g;
    const isHas = reg.test(source);
    if (isHas) {
      const newSource = source.replace(/\n/g, '<br />');
      setSourceText(newSource);
    } else {
      setSourceText(source);
    }
    setTimeout(() => {
      ChangeScrollTop();
    }, 0);
  }, [source , ChangeScrollTop]);

  const renderLogWorkflow = useMemo(() => {
    return index === 0 ? null : <LogWorkflow Cardloading={message.Cardloading} logData={message.logData} />;
  }, [index, message]);

  const renderChart = useMemo(() => {
    return message.chart ? <EChartsChart content={message.chart} /> : null;
  }, [message]);

  const renderChatContent = useMemo(() => {
    return (
      <div
        style={{ wordBreak: "break-all" }}
        ref={sourceEl}
        dangerouslySetInnerHTML={{ __html: sourceText }}
      />
    );
  }, [sourceText]);
  const renderUser = useMemo(() => {
    return chat_type === "autopilot" ? <AutoPilotInfo databases_type={databases_type}/> : <div className={`Chat ${message.sender}`}>{source}</div>;
  }, [chat_type, message, source]);

  return (
    <div className={`message ${chat_type}`} onMouseLeave={() => setShowComponent(false)}>
      <>
        <div className={`info${message.sender}-time`}>{chat_type==="autopilot"?"":message.time}</div>
        <div className={`chat${message.sender}`} onMouseEnter={() => setShowComponent(true)}>
          {
            chat_type!=="autopilot"&&
            <img src={message.sender === "user" ? currentUser.profile_image_url : icon_small} alt="" />
          }
          {message.sender === "user" ?  
          renderUser
          : 
          (
            <div className={`Chat ${message.sender}`}>
              {renderLogWorkflow}
              {renderChart}
              {renderChatContent}
            </div>
          )}
        </div>

        {showComponent && !message.Cardloading&&chat_type!=="autopilot" && <Copy index={index} source={source} message={message} />}
        {message.Cardloading && chat_type === "chat" && <Copy index={index} source={source} message={message} />}
      </>
    </div>
  );
};

TypingCard.propTypes = {
  title: PropTypes.string,
  source: PropTypes.string,
};

TypingCard.defaultProps = {
  title: "",
  source: "",
};

export default TypingCard;
