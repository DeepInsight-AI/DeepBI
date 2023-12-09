import React,{ useState } from 'react';
import html2canvas from 'html2canvas';
import jsPDF from 'jspdf';

/**
 * 导出PDF
 * @param {导出后的文件名} title 
 * @param {要导出的dom节点：react使用ref} ele 
 */
export const useExportPDF = () => {
  let [loading, setLoading] = useState(false);

  const exportPDF = async (title, element) => {
    if (element) {
      setLoading(true);
      let width = element.offsetWidth / 4;
      let height = element.offsetHeight / 4;
      let limit = 14400;
      if (height > limit) {
        let contentScale = limit / height;
        height = limit;
        width *= contentScale;
      }
      html2canvas(element, {
        scale: 2,
        useCORS: true,
        allowTaint: false,
        ignoreElements: (element) => {
          if (element.id === 'ignoreBtnElement') return true;
          return false;
        }
      }).then(canvas => {
        let context = canvas.getContext('2d');
        let orientation;
        if (context) {
          context.mozImageSmoothingEnabled = false;
          context.webkitImageSmoothingEnabled = false;
          context.msImageSmoothingEnabled = false;
          context.imageSmoothingEnabled = false;
        }
        // let pageData = canvas.toDataURL('image/jpg', 1.0);
        let pageData = canvas.toDataURL('image/webp', 0.7);
        let img = new Image();
        img.src = pageData;
        img.onload = function () {
          img.width /= 2;
          img.height /= 2;
          img.style.transform = 'scale(0.5)';
          let pdf;
          orientation = width > height ? 'l' : 'p'
          // eslint-disable-next-line
          pdf = new jsPDF(orientation , 'mm', [
            width,
            height
          ]);
          pdf.addImage(
            pageData,
            'jpeg',
            0,
            0,
            width,
            height
          );
          pdf.save(`${title}.pdf`);
          setLoading(false);
        };
      });
    }
  }

  return { loading, exportPDF };
}
export const exportPDF = async (title, element) => {
   
}