import React,{useState,useEffect} from 'react';
import Button from "antd/lib/button";
import Upload from "antd/lib/upload";
// import * as XLSX from 'xlsx';
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import LoadingState from "@/components/items-list/components/LoadingState";
import wrapSettingsTab from "@/components/SettingsWrapper";
import ExcelList from "./components/excelList";
import routes from "@/services/routes";
import { axios } from "@/services/axios";
import UploadOutlinedIcon from "@ant-design/icons/UploadOutlined";
import "./index.css"
const ExcelUpload = () => {
  const [excelList, setExcelList] = useState([]);
  const [disabled, setDisabled] = useState(false);
  const getExcelList = async () => {
    const {data} = await axios.get(`api/upload`);
    setExcelList(data);
  };
  useEffect(() => {
    getExcelList();
  }, []);
  const fileUpload = (info)=>{
    setDisabled(true)

    const formData = new FormData()
    formData.append('file', info.file)
    axios.post("/api/upload",formData).then((res) => {
        info.onSuccess()
        setDisabled(false)
        getExcelList();
    }).catch((err) => {
      info.onError() 
      setDisabled(false)
    })

  }
  const delList = async (item) => {
    await axios.delete(`api/upload/${item.id}`);
    getExcelList();
  };
  const props = {
    name: 'file',
    accept:".csv,.xlsx,.xls",
    customRequest: fileUpload,
    onChange(info) {
    },
    // progress: {
    //   strokeColor: {
    //     '0%': '#108ee9',
    //     '100%': '#87d068',
    //   },
    //   strokeWidth: 3,
    //   format: (percent) => percent && `${parseFloat(percent.toFixed(2))}%`,
    // },
  };
  
  
  return (
   
     <React.Fragment>
     <div className="row1">
       {!excelList && <LoadingState className="" />}
       {excelList && (
         <>
         <Upload {...props}>
         <Button type="primary" disabled={disabled}> <UploadOutlinedIcon />{window.W_L.upload}</Button>
       </Upload>
         <ExcelList excelList={excelList} delList={delList}></ExcelList>
         </>
       )}
     </div>
   </React.Fragment>
  );
};

export default ExcelUpload;

const ExcelUploadPage = wrapSettingsTab(
  "Upload.ExcelUpload",
  {
    title: window.W_L.excel_upload,
    path: "upload/excel",
    order: 8,
  },
  ExcelUpload
);

routes.register(
  "Upload.ExcelUpload",
  routeWithUserSession({
    path: "/upload/excel",
    title: window.W_L.excel_upload,
    render: pageProps => <ExcelUploadPage {...pageProps} />,
  })
);