import React, { useRef, useEffect, useState, useMemo } from "react";
import { PropTypes } from "prop-types";
import { currentUser } from "@/services/auth";
import EChartsChart from "../Echarts/Echarts";
import AutoPilot from "../AutoPilot/AutoPilot";
import LogWorkflow from "../LogWorkflow"
import Copy from "../Copy/Copy.jsx";
import "./index.less";
import icon_small from "@/assets/images/icon_small.png";

const TypingCard = (props) => {
  const { autopilot,chart, source, logData, index, Cardloading, sender, time, ChangeScrollTop,retry } = props;
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
    return index === 0 ? null : <LogWorkflow Cardloading={Cardloading} logData={logData} />;
  }, [index, Cardloading, logData]);

  const renderChart = useMemo(() => {
    return chart ? <EChartsChart content={chart} /> : null;
  }, [chart]);

  const renderChatContent = useMemo(() => {
    return (
      <div
        style={{ wordBreak: "break-all" }}
        ref={sourceEl}
        dangerouslySetInnerHTML={{ __html: sourceText }}
      />
    );
  }, [sourceText]);

  const renderAutoPilot = useMemo(() => {
    return autopilot ? <AutoPilot content={autopilot} /> : null;
  }, [autopilot]);
  return (
    <div className="message" onMouseLeave={() => setShowComponent(false)}>
      <>
        <div className={`info${sender}-time`}>{time}</div>
        <div className={`chat${sender}`} onMouseEnter={() => setShowComponent(true)}>
          <img src={sender === "user" ? currentUser.profile_image_url : icon_small} alt="" />
          {sender === "user" ? (
            <div className={`chat ${sender}`}>{source}</div>
          ) : (
            <div className={`chat ${sender}`}>
              {renderLogWorkflow}
              {renderChart}
              {renderAutoPilot}
              {renderChatContent}
            </div>
          )}
        </div>

        {showComponent && !Cardloading && <Copy index={index} source={source} sender={sender} retry={retry} />}
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
