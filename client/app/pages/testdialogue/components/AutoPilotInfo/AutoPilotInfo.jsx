import React, { useRef, useEffect, useState, useMemo } from "react";
import Descriptions from "antd/lib/descriptions";
import Input from "antd/lib/input";
import Button from "antd/lib/button";
// import "./index.less";

const { TextArea } = Input;
const AutoPilotInfo =()=>{
const inputMessage = useRef(null);
return (
    <div>
        <Descriptions  
            size="small" 
            column={1} 
            bordered
            extra={<Button type="primary">Edit</Button>}
        >
            <Descriptions.Item label={window.W_L.report_name}>
                <Input placeholder={window.W_L.report_name_placeholder} />
            </Descriptions.Item>
            <Descriptions.Item label={window.W_L.report_desc}>
                <TextArea rows={4} placeholder={window.W_L.report_desc_placeholder} value={inputMessage.current} bordered={false} />
            </Descriptions.Item>
        </Descriptions>
    </div>
)
};
export default AutoPilotInfo;