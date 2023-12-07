import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import Tag from "antd/lib/tag";
import { axios } from "@/services/axios";
import AutoPilot from "./components/AutoPilot/AutoPilot";
import LogWorkflow from "./components/LogWorkflow"
import Skeleton from "antd/lib/skeleton";
import "./components/TypingCard/index.less";

function AutopilotView(props) {
    const [autoPilot, setAutoPilot] = useState({});
    const [loading, setLoading] = useState(true);
    const getAutoPilot = async (id) => {
        const res = await axios.get(`/api/auto_pilot/${id}`);
        if(res.code === 200){
            setAutoPilot(res.data);
        }else{
            setAutoPilot({});
        }
        setLoading(false);
    };
    useEffect(() => {
        getAutoPilot(props.autopilotId);
    }, []);

  return (
    <div className="auto-pilot-list">
        <Skeleton loading={loading} active paragraph={{ rows: 10 }}>
        <div className="message" >
        <div className={`autopilot`} >
            <div className={`chat user autopilot-item`}>
                <p>报告名称:{autoPilot.report_name}</p>
                <p>报告描述:{autoPilot.report_desc}</p>
                {/* <LogWorkflow Cardloading={false} logData={[...(autoPilot.chat_log || [])]} /> */}
                <LogWorkflow Cardloading={false} logData={[autoPilot.chat_log]} />
              <AutoPilot content={autoPilot.html_code}></AutoPilot>
            </div>
        </div>
    </div>
        </Skeleton>
    </div>
  );
}

function registerAutopilotViewRoute() {
  routes.register(
    "Dialogue.List.autopilot_view",
    routeWithUserSession({
      path: "/autopilot/:autopilotId",
      render: (pageProps) => <AutopilotView {...pageProps} />,
    })
  );
}

registerAutopilotViewRoute();
