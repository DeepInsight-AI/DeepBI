import html2canvas from 'html2canvas';
import jsPDF from 'jspdf';

/**
 * 导出PDF
 * @param {导出后的文件名} title 
 * @param {要导出的dom节点：react使用ref} ele 
 */
export const exportPDF = async (title, element) => {
  // // 根据dpi放大，防止图片模糊
  // const scale = window.devicePixelRatio > 1 ? window.devicePixelRatio : 2;
  // // 下载尺寸 a4 纸 比例
  // let pdf = new jsPDF('p', 'pt', 'a4');
  // let width = ele.offsetWidth;
  // let height = ele.offsetHeight;
  // console.log('height', height)
  // console.log('aa', width, height, scale)

  // const canvas = document.createElement('canvas');
  // canvas.width = width * scale;
  // canvas.height = height * scale;
  // var contentWidth = canvas.width;
  // var contentHeight = canvas.height;

  // console.log('contentWidth', contentWidth, contentHeight)
  // //一页pdf显示html页面生成的canvas高度;
  // var pageHeight = contentWidth / 592.28 * 841.89;
  // //未生成pdf的html页面高度
  // var leftHeight = contentHeight;
  // console.log('leftHeight', leftHeight)
  // //页面偏移
  // var position = 0;
  // //a4纸的尺寸[595.28,841.89]，html页面生成的canvas在pdf中图片的宽高
  // var imgWidth = 595.28;
  // var imgHeight = 592.28 / contentWidth * contentHeight;
  // const pdfCanvas = await html2canvas(ele, {
  //   useCORS: true,
  //   canvas,
  //   scale,
  //   width,
  //   height,
  //   x: 0,
  //   y: 0,
  // });
  // const imgDataUrl = pdfCanvas.toDataURL();

  // if (height > 14400) { // 超出jspdf高度限制时
  //   const ratio = 14400 / height;
  //   // height = 14400;
  //   width = width * ratio;
  // }

  // // 缩放为 a4 大小  pdfpdf.internal.pageSize 获取当前pdf设定的宽高
  // height = height * pdf.internal.pageSize.getWidth() / width;
  // width = pdf.internal.pageSize.getWidth();
  // if (leftHeight < pageHeight) {
  //   pdf.addImage(imgDataUrl, 'png', 0, 0, imgWidth, imgHeight);
  // } else {    // 分页
  //   while (leftHeight > 0) {
  //     pdf.addImage(imgDataUrl, 'png', 0, position, imgWidth, imgHeight)
  //     leftHeight -= pageHeight;
  //     position -= 841.89;
  //     //避免添加空白页
  //     if (leftHeight > 0) {
  //       pdf.addPage();
  //     }
  //   }
  // }
  // // 导出下载 
  // await pdf.save(`${title}.pdf`);



  // let element = document.getElementById('elementToPrint');
    if (element) {
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
        let pageData = canvas.toDataURL('image/jpg', 1.0);
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
        };
      });
    }
}