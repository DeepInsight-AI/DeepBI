import React, { memo, useEffect, useRef, useState } from 'react';
import FilePptOutlinedIcon from "@ant-design/icons/FilePptOutlined";
import html2pdf from 'html2pdf.js';
import * as echarts from 'echarts';
import { useExportPDF } from './useExportPDF.js';
import LoadingOutlinedIcon from '@ant-design/icons/LoadingOutlined';
import "./index.css";
const AutoPilot = memo(({ title,content }) => {
  const autopilotRef = useRef(null);
  // const [AutoPilotJson, setAutoPilotJson] = useState({});

  useEffect(() => {
    if(!content) return;
    const data = content;
    let div = document.createElement("div")
    div.innerHTML = data
    autopilotRef.current.append(div)
    // eval解析
  try {
    let scripts = div.querySelectorAll("script")
  scripts.forEach(item => {
    window.eval(item.innerText);
  });
  } catch (error) {
    
  }
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
      // const opt = {
      //   margin: 10,
      //   filename: 'converted_pdf.pdf',
      //   image: { type: 'jpeg', quality: 0.98 },
      //   html2canvas: { scale: 2 },
      //   jsPDF: { unit: 'mm', format: 'a2', orientation: 'portrait' } // 设置 A2 纸张大小
      // };
  
      // // 使用 html2pdf 库生成 PDF
      // html2pdf()
      //   .from(autopilotRef.current)
      //   .set(opt)
      //   .save();
  
      exportPDF(title, autopilotRef.current)
  }
  const { loading, exportPDF } = useExportPDF();
  return(
    <div className="auto_pilot" >
    <div className="auto_pilot_icon" onClick={exportPdf}>
      
        <svg t="1701955188289" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="3387" width="2rem" height="2rem"><path d="M704 0H192c-35.328 0-64 28.672-64 64v320h576c35.328 0 64 28.672 64 64v415.744c0 35.328-28.672 64-64 64H128v31.744c0 35.328 28.672 64 64 64h768c35.328 0 64-28.672 64-64v-640L704 0z" fill="#EAEAEA" p-id="3388"></path><path d="M704 0v256c0 35.328 28.672 64 64 64h256L704 0z" fill="#434854" p-id="3389"></path><path d="M768 320l256 256V320z" opacity=".1" p-id="3390"></path><path d="M704 832c0 17.92-14.336 31.744-31.744 31.744H31.744C13.824 863.744 0 849.408 0 832V480.256c0-17.92 14.336-31.744 31.744-31.744h640c17.92 0 31.744 14.336 31.744 31.744V832z" fill="#CD4050" p-id="3391"></path><path d="M192 544.256h-48.128c-8.704 0-15.872 7.168-15.872 15.872v192c0 8.704 7.168 15.872 15.872 15.872s15.872-7.168 15.872-15.872v-79.872h31.744c35.328 0 64-28.672 64-64s-28.16-64-63.488-64z m158.72 0c34.816 0 62.976 27.648 64 62.464v97.792c0 34.816-27.648 62.976-62.464 64h-49.664c-8.704 0-15.36-6.656-15.872-14.848V560.64c0-8.704 6.656-15.36 14.848-15.872h49.152z m227.328 0c8.704 0 15.872 7.168 15.872 15.872S586.752 576 578.048 576h-80.384v64h48.128c8.704 0 15.872 7.168 15.872 15.872s-7.168 15.872-15.872 15.872h-48.128v79.872c0 8.704-7.168 15.872-15.872 15.872s-15.872-7.168-15.872-15.872v-192c0-8.704 7.168-15.872 15.872-15.872h96.256zM350.72 576h-31.744v159.744h31.744c17.408 0 31.232-13.824 31.744-30.72v-97.28C382.976 590.336 368.64 576 350.72 576zM192 576c17.92 0 31.744 14.336 31.744 31.744S209.92 640 192 640h-31.744v-64H192z" fill="#FFFFFF" p-id="3392"></path></svg> 
    <p>{window.W_L.download}</p>
    {
        loading&&
        <LoadingOutlinedIcon />
      }
    </div>
    <div ref={autopilotRef}></div>
  </div>
  )
});

export default AutoPilot;