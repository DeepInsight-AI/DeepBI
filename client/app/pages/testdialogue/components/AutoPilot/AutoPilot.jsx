import React, { memo, useEffect, useRef, useState } from 'react';
import FilePptOutlinedIcon from "@ant-design/icons/FilePptOutlined";
import html2pdf from 'html2pdf.js';
import * as echarts from 'echarts';
import "./index.css";
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
      filename: 'autopilot.pdf',
      image: { type: 'jpeg', quality: 1 },
      html2canvas: {
        dpi: 192,
        scale:2,
        letterRendering: true,
        useCORS: true
      },
      });
  }
  return <div className="auto_pilot" >
    <div className="auto_pilot_icon" onClick={exportPdf}>
    <FilePptOutlinedIcon />
    <p>导出PDF</p>
    </div>
    <div ref={autopilotRef}></div>
  </div>
});

export default AutoPilot;