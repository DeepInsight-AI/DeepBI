import React from 'react';
import Button from "antd/lib/button";
import Loading from "../loading";
import LoadingOutlinedIcon from "@ant-design/icons/LoadingOutlined";
import './index.css';

const LogWorkflow = (props) => {
  const { logData ,Cardloading} = props;
  const [sourceList, setSourceList] = React.useState([]);
  // workTitle
  const [workTitle, setWorkTitle] = React.useState("Show work");
  const logTerminal = React.useRef(null);
  React.useEffect(()=>{
    if(logData && logData.length>0){
      const newLogData = logData.slice(sourceList.length);
      const log_terminal = logTerminal.current;
      const reg = /\n/g;
      const isHas = newLogData.some(item => reg.test(item));
      if(isHas){
        const newSource = newLogData.map(item => item.replace(/\n\n/g, '<br />'));
        setSourceList(prevSourceList => [...prevSourceList, ...newSource]);
      }else{
        setSourceList(prevSourceList => [...prevSourceList, ...newLogData]);
      }
      setTimeout(() => {
        log_terminal.scrollTop = log_terminal.scrollHeight;
      }, 0);
    }
  },[logData])
  const changeWork = () => {
    if(logData&&logData.length == 0){
      return;
    }
    if(workTitle == "Show work"){
      setWorkTitle("Hide work");
      logTerminal.current.style.height = "170px";
    }else{
      setWorkTitle("Show work");
      logTerminal.current.style.height = "0px";
    }
  }
  return (
    
    <div className={`log_terminal`}>
      <div className="log_terminal_header">
      {Cardloading?
      <Loading></Loading>
      // <LoadingOutlinedIcon />
      :
      <div></div>  
    }
        <div className="log_terminal_header_title" onClick={changeWork}>{workTitle}</div>
      </div>
      <div className="log_terminal_content" ref={logTerminal}>
      {sourceList && sourceList.length>0&&sourceList.map((item,index)=>{
        return <div key={index} className="log-item"  dangerouslySetInnerHTML={{ __html: item }}></div>
      })
      }
      </div>
      </div>
  );
};

export default LogWorkflow;