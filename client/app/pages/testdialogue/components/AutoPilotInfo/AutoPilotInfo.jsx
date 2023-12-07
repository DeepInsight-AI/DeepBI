import React, { useRef, useEffect, useState, useMemo } from "react";
import Descriptions from "antd/lib/descriptions";
import Input from "antd/lib/input";
import Button from "antd/lib/button";
import Tooltip from "antd/lib/tooltip";
import { axios } from "@/services/axios";
import { dialogueStorage } from "../Dialogue/method/dialogueStorage";
import InfoCircleOutlinedIcon from "@ant-design/icons/InfoCircleOutlined";
import "./index.css";

const { TextArea } = Input;
const AutoPilotInfo =()=>{
const [btn_disabled, setBtn_disabled] = useState(false);
const [report_name, setReportName] = useState('');
const [report_desc, setReportDesc] = useState('');
const [btn_isShow, setBtn_isShow] = useState(true);
// /api/auto_pilot
const autoPilot =async (databases_id,db_comment) => {
    const data={
        databases_id,
        report_name,
        report_desc,
        db_comment
    }
   await axios.post("/api/auto_pilot",data).then((res)=>{
        console.log(res);
        setBtn_disabled(false);
        setBtn_isShow(false);
    }).catch((err)=>{
        console.log(err);
        setBtn_disabled(false);
    })
};
const CreateAutoPilot = () => {
    if (!report_name || !report_desc) {
        return;
    }
    const HoImes_Dialogue = getDialogueStorage();
    console.log(HoImes_Dialogue, 'HoImes_Dialogue');
    if(HoImes_Dialogue && HoImes_Dialogue.length>0){
        if (HoImes_Dialogue[0].table_name &&HoImes_Dialogue[0].table_name.tableName && HoImes_Dialogue[0].table_name.tableName.length > 0) {
            setBtn_disabled(true);
              let promisesList = [];
              const promises = HoImes_Dialogue[0].table_name.tableName.map(async (item) => {
                const res = await axios.get(`/api/data_table/columns/${HoImes_Dialogue[0].Holmestable_id}/${item.name}`);
                promisesList.push({
                  table_name: res.table_name,
                  table_comment: res.table_desc,
                  field_desc: res.table_columns_info.field_desc
                });
              });

              
              Promise.all(promises).then(() => {
                autoPilot(HoImes_Dialogue[0].Holmestable_id,{
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
const { getDialogueStorage}=dialogueStorage();
return (
    <div style={{width:"90%"}}>
        <Descriptions  
            size="small" 
            column={1} 
            bordered
        >
            <Descriptions.Item label={window.W_L.report_name}>
                <Input bordered={false}  placeholder={window.W_L.report_name_placeholder}
                value={report_name}
                onChange={e => setReportName(e.target.value)}
                />
            </Descriptions.Item>
            <Descriptions.Item label={window.W_L.report_desc}>
                <TextArea rows={10} placeholder={window.W_L.report_desc_placeholder} bordered={false}
                value={report_desc}
                onChange={e => setReportDesc(e.target.value)}
                />
            </Descriptions.Item>
        </Descriptions>
        <div className="gpt-descriptions-btn">
        <div style={{fontSize:"14px"}}>
            <Tooltip title={window.W_L.source_tooltip}>
              <InfoCircleOutlinedIcon style={{marginRight:"3px"}} />
            </Tooltip>
              {window.W_L.source_first_tooltip}
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