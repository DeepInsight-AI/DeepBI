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

      try {
        const canvas = await html2canvas(element, {
          scale: 2,
          useCORS: true,
          allowTaint: false,
          ignoreElements: (element) => {
            if (element.id === 'ignoreBtnElement') return true;
            return false;
          }
        });

        const width = canvas.width / 4;
        let height = canvas.height / 4;
        const limit = 14400;
        if (height > limit) {
          const contentScale = limit / height;
          height = limit;
          width *= contentScale;
        }

        const pageData = canvas.toDataURL('image/webp', 0.7);

        const img = new Image();
        img.src = pageData;

        img.onload = function () {
          img.width /= 2;
          img.height /= 2;
          img.style.transform = 'scale(0.5)';
          const orientation = width > height ? 'l' : 'p';

          const pdf = new jsPDF(orientation, 'mm', [width, height]);
          pdf.addImage(pageData, 'jpeg', 0, 0, width, height);
          pdf.save(`${title}.pdf`);
          setLoading(false);
        };
      } catch (error) {
        console.error('导出PDF时出错：', error);
        setLoading(false);
      }
    }
  };

  return { loading, exportPDF };
};
