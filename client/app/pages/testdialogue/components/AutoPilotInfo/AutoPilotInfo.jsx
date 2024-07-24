import React, { useRef, useEffect, useState, useMemo } from "react";
import Descriptions from "antd/lib/descriptions";
import Input from "antd/lib/input";
import Button from "antd/lib/button";
import Tooltip from "antd/lib/tooltip";
import { axios } from "@/services/axios";
import routes from "@/services/routes";
import { dialogueStorage } from "../Dialogue/method/dialogueStorage";
import InfoCircleOutlinedIcon from "@ant-design/icons/InfoCircleOutlined";
import toast from "react-hot-toast";
import "./index.css";

const { TextArea } = Input;
const AutoPilotInfo =({databases_type})=>{
const [btn_disabled, setBtn_disabled] = useState(false);
const [report_name, setReportName] = useState('');
const [report_desc, setReportDesc] = useState('');
const [btn_isShow, setBtn_isShow] = useState(true);
// /api/auto_pilot
const autoPilot =async (databases_id,db_comment) => {
    const data={
        databases_id,
        databases_type:databases_type.current,
        report_name,
        report_desc,
        db_comment
    }
   await axios.post("/api/auto_pilot",data).then((res)=>{
        setBtn_disabled(false);
        if(res.code===200){
        setBtn_isShow(false);
        toast.success(window.W_L.submit_success+" "+window.W_L.submit_success_tip);
        routes.navigate("Dialogue.List.autopilot_list");
        }else{
        toast.error(res.data);
        }
    }).catch((err)=>{
        setBtn_disabled(false);
        toast.error(window.W_L.submit_fail);
    })
};
const CreateAutoPilot = () => {
    if (!report_name || !report_desc) {
        return;
    }
    const Chart_Dialogue = getAutopilotStorage();
    if(Chart_Dialogue && Chart_Dialogue.length>0){
        if (Chart_Dialogue[0].table_name &&Chart_Dialogue[0].table_name.tableName && Chart_Dialogue[0].table_name.tableName.length > 0) {
            setBtn_disabled(true);
              let promisesList = [];
              const promises = Chart_Dialogue[0].table_name.tableName.map(async (item) => {
                const res = await axios.get(`/api/data_table/columns/${Chart_Dialogue[0].Charttable_id}/${item.name}`);
                promisesList.push({
                  table_name: res.table_name,
                  table_comment: res.table_desc,
                  field_desc: res.table_columns_info.field_desc
                });
              });

              
              Promise.all(promises).then(() => {
                autoPilot(Chart_Dialogue[0].Charttable_id,{
                    databases_desc: "",
                    table_desc: promisesList
                  });
              }).catch((err) => {
                console.log(err, 'first_error');
                setBtn_disabled(false);
              });
        
          }
    }
}
const { getAutopilotStorage}=dialogueStorage();
return (
    <div style={{width:"100%"}}>
        <Descriptions  
            size="small" 
            column={1} 
            bordered
            className="my-custom-label"
        >
            <Descriptions.Item label={window.W_L.report_name}>
                <Input bordered={false}  placeholder={window.W_L.report_name_tip}
                value={report_name}
                onChange={e => setReportName(e.target.value)}
                />
            </Descriptions.Item>
            <Descriptions.Item label={window.W_L.report_desc}>
                <TextArea style={{ resize: 'none'}} rows={10} placeholder={window.W_L.report_desc_tip} bordered={false}
                value={report_desc}
                onChange={e => setReportDesc(e.target.value)}
                />
            </Descriptions.Item>
        </Descriptions>
        <div className="gpt-descriptions-btn">
        <div style={{fontSize:"14px"}}>
            <Tooltip title={window.W_L.report_name_and_report_desc_tip}>
              <InfoCircleOutlinedIcon style={{marginRight:"3px"}} />
            </Tooltip>
              {window.W_L.please_complete_report_name_and_report_desc}
            </div>
            {
                btn_isShow&&
                <Button style={{width: "100px"}} block type="primary" disabled={btn_disabled} onClick={() => CreateAutoPilot()}>
                <i className="fa fa-check m-r-5" aria-hidden="true" />
                {window.W_L.submit}
              </Button>
            }
        </div>
    </div>
)
};
export default AutoPilotInfo;