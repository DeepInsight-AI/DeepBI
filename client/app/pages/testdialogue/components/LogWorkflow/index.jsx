import React from "react";
import Loading from "../loading";
import Button from "antd/lib/button";
import CopyOutlinedIcon from "@ant-design/icons/CopyOutlined";
import CompressOutlinedIcon from "@ant-design/icons/CompressOutlined";
import Modal from "antd/lib/modal";
import { markdown } from "markdown";
import HtmlContent from "@/components/chart/components/HtmlContent";
import toast from "react-hot-toast";
import "./index.css";
import Tooltip from "@/components/Tooltip";

const LogWorkflow = props => {
  const { logData, Cardloading } = props;
  const [isModalVisible, setIsModalVisible] = React.useState(false);
  const [sourceList, setSourceList] = React.useState([]);
  // workTitle
  const [workTitle, setWorkTitle] = React.useState("Show work");
  const logTerminal = React.useRef(null);
  React.useEffect(() => {
    if (logData && logData.length > 0) {
      const newLogData = logData.slice(sourceList.length);
      const log_terminal = logTerminal.current;
      const reg = /\n/g;
      const isHas = newLogData.some(item => reg.test(item));
      if (isHas) {
        const newSource = newLogData.map(item => item.replace(/\n\n/g, "<br />"));
        // const newSource = newLogData.map(item => markdown.toHTML(item));
        setSourceList(prevSourceList => [...prevSourceList, ...newSource]);
      } else {
        setSourceList(prevSourceList => [...prevSourceList, ...newLogData]);
      }
      setTimeout(() => {
        if (log_terminal) {
          log_terminal.scrollTop = log_terminal.scrollHeight;
        }

      }, 0);
    }
  }, [logData, sourceList.length]);
  const changeWork = () => {
    if (logData && logData.length === 0) {
      return;
    }
    if (workTitle === "Show work") {
      setWorkTitle("Hide work");
      logTerminal.current.style.height = "170px";
    } else {
      setWorkTitle("Show work");
      logTerminal.current.style.height = "0px";
    }
  };
  const copy = () => {
    try {
      const log_terminal = logTerminal.current;
      const range = document.createRange();
      range.selectNode(log_terminal);
      const selection = window.getSelection();
      if (selection.rangeCount > 0) selection.removeAllRanges();
      selection.addRange(range);
      document.execCommand("copy");
      selection.removeAllRanges();
      toast.success(window.W_L.copy_success);
    } catch (error) {
      toast.error(window.W_L.copy_failed);
    }
  };

  const compress = () => {
    setIsModalVisible(true);
  };

  const showModel = () => {
    return (
      <Modal
        visible={isModalVisible}
        onCancel={() => setIsModalVisible(false)}
        footer={null}
        closable={false}
        width="80%"
        className="log_terminal_modal"
      >
        <div className="log_terminal_content_div">
        <Tooltip title={window.W_L.copy}>
          <Button
            icon={<CopyOutlinedIcon />}
            style={{ position: "absolute", color: "#9d9d9d" }}
            type="text"
            size="small"
            className="copy_btn"
            onClick={copy}>
            {/* {window.W_L.copy} */}
          </Button>
        </Tooltip>
        <div className="log_terminal_content" style={{height:"auto",maxHeight:"470px"}}>
        {sourceList &&
            sourceList.length > 0 &&
            sourceList.map((item, index) => {
              return <div key={index} className="log-item" dangerouslySetInnerHTML={{ __html: item }}></div>;
            })}
        </div>
        </div>
      </Modal>
    );
  };

  return (
    <div className={`log_terminal`}>
      <div className="log_terminal_header">
        {Cardloading ? (
          <Loading></Loading>
        ) : (
          // <LoadingOutlinedIcon />
          <div></div>
        )}
        <div className="log_terminal_header_title" onClick={changeWork}>
          {workTitle}
        </div>
      </div>
      <div className="log_terminal_content_div">
        <Tooltip title={window.W_L.copy}>
          <Button
            icon={<CopyOutlinedIcon />}
            style={{ position: "absolute", color: "#9d9d9d" }}
            type="text"
            size="small"
            className="copy_btn"
            onClick={copy}>
            {/* {window.W_L.copy} */}
          </Button>
        </Tooltip>
        <Tooltip title="全屏">
          <Button
            icon={<CompressOutlinedIcon />}
            style={{ position: "absolute", color: "#9d9d9d" }}
            type="text"
            size="small"
            className="compress_btn"
            onClick={compress}>
            {/* {window.W_L.copy} */}
          </Button>
        </Tooltip>
        {showModel()}
        <div className="log_terminal_content" ref={logTerminal}>
          {sourceList &&
            sourceList.length > 0 &&
            sourceList.map((item, index) => {
              return <div key={index} className="log-item" dangerouslySetInnerHTML={{ __html: item }}></div>;
            })}

          {/* {sourceList &&
            sourceList.length > 0 &&
            sourceList.map((item, index) => {
              return <HtmlContent key={index} className="log-item preview markdown">{item}</HtmlContent>;
            })} */}
        </div>
      </div>
    </div>
  );
};

export default LogWorkflow;
