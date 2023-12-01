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
  //  html2pdf(autopilotRef.current, {
  //     filename: 'autopilot.pdf',
  //     image: { type: 'jpeg', quality: 1 },
  //     html2canvas: {
  //       dpi: 192,
  //       scale:2,
  //       letterRendering: true,
  //       useCORS: true
  //     },
  //     });
      const opt = {
        margin: 10,
        filename: 'converted_pdf.pdf',
        image: { type: 'jpeg', quality: 0.98 },
        html2canvas: { scale: 2 },
        jsPDF: { unit: 'mm', format: 'a2', orientation: 'portrait' } // 设置 A2 纸张大小
      };
  
      // 使用 html2pdf 库生成 PDF
      html2pdf()
        .from(autopilotRef.current)
        .set(opt)
        .save();
  
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