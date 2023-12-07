import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import DialogueListLeft from "./components/DialogueListLeft";
import NoDashboardData from "./components/NoDashboardData";
import { axios } from "@/services/axios";
import "./index.less";

function AutopilotHistory() {
  const { chatType } = props;
  const [AutoPilotList, setAutoPilotList] = useState([]);
  const  getAutoPilotList = async () => {
    const res = await axios.get("/api/auto_pilot");
    setAutoPilotList(res.data);
    console.log(res);
    };
    useEffect(() => {
        getAutoPilotList();
    }, []);
  return (
    <div className="page-alerts-list">
        (<DialogueListLeft chat_type={chatType} switchMode={switchMode} AutoPilotList={AutoPilotList}></DialogueListLeft>)
      <div style={{ display: 'flex', flexDirection: 'column', flex: 1, position: "relative", background: "rgb(250, 250, 250)",overflow: "auto"}}>
      {
        AutoPilotList&&AutoPilotList.lenght>0?
        <NoDashboardData chatType={chatType} />
        :
          ""
      }
      </div>
    </div>
  );
}

function registerAutopilotHistoryRoute() {
  routes.register(
    "Dialogue.List.autopilot_list",
    routeWithUserSession({
      path: "/autopilot_list",
      title: "autopilot_list",
      render: (pageProps) => <AutopilotHistory {...pageProps} chatType="autopilot" />,
    })
  );
}

registerAutopilotHistoryRoute();
