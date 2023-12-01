import React, { memo, useEffect, useRef, useState } from 'react';
import FilePptOutlinedIcon from "@ant-design/icons/FilePptOutlined";
import html2pdf from 'html2pdf.js';
import * as echarts from 'echarts';
const AutoPilot = memo(({ content }) => {
  const autopilotRef = useRef(null);
  // const [AutoPilotJson, setAutoPilotJson] = useState({});

  useEffect(() => {
    if(!content) return;
    const data = content;
    let div = document.createElement("div")
    div.innerHTML = data
    autopilotRef.current.append(div)
    // eval解析
  let scripts = div.querySelectorAll("script")
  scripts.forEach(item => {
    window.eval(item.innerText);
  });
  }, [content]);
  const exportPdf = () => {
   html2pdf(autopilotRef.current, {
      filename: 'autopilot.pdf'
      });
  }
  return <div id="auto_pilot" ref={autopilotRef}>
    <div className="auto_pilot_icon" onClick={exportPdf}>
    <FilePptOutlinedIcon />
    <p>导出PDF</p>
    </div>
  </div>
});

export default AutoPilot;