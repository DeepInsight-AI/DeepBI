import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import Tag from "antd/lib/tag";
import { axios } from "@/services/axios";
import AutoPilot from "./components/AutoPilot/AutoPilot";
import LogWorkflow from "./components/LogWorkflow"
import "./components/TypingCard/index.less";

function AutopilotView(props) {
    const [autoPilot, setAutoPilot] = useState({});
    const getAutoPilot = async (id) => {
        const res = await axios.get(`/api/auto_pilot/${id}`);
        if(res.code === 200){
            setAutoPilot(res.data);
        }else{
            setAutoPilot({});
        }
    };
    useEffect(() => {
        getAutoPilot(props.autopilotId);
    }, []);

  return (
    <div className="auto-pilot-list">
        <div className="message" >
        <div className={`chatuser`} >
            <div className={`chat user`}>
                <LogWorkflow Cardloading={false} logData={[autoPilot.chat_log]} />;
              <AutoPilot content={autoPilot.html_code}></AutoPilot>
            </div>
        </div>
    </div>
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
