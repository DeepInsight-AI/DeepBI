import React from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import DashboardPage from "@/pages/dashboards/DashboardPage";
import Dialogue from "./components/Dialogue";
import DialogueListLeft from "./components/DialogueListLeft";
import NoDashboardData from "./components/NoDashboardData/NoDashboardData.jsx";
import "./index.less";

function DialogueList(props) {
  const { chatType } = props;
  // const [chatType, setChatType] = React.useState("chat");
  const [dashboardId, setDashboardId] = React.useState("");
  const [dashboardKey, setDashboardKey] = React.useState(0);
  const [uuid, setUuid] = React.useState("");
  const [isShowNoData, setIsShowNoData] = React.useState(false);
  const switchMode = (item) => {
    setUuid(item.uuid);
  };
  const handleError = () => {
  };
  const sendUrl = (item) => {
    if(item==="new_report"){
      setIsShowNoData(true);
      setDashboardId("");
      setDashboardKey((prevKey) => prevKey + 1);
      return;
    }
    setIsShowNoData(false);
    setDashboardId(item);
    setDashboardKey((prevKey) => prevKey + 1);
  }
  return (
    <div className="page-alerts-list">
      {
        chatType === "viewConversation"&&
        (<DialogueListLeft chat_type={chatType} switchMode={switchMode}></DialogueListLeft>)
      }
      <div style={{ display: 'flex', flexDirection: 'column', flex: 1, position: "relative", background: "rgb(250, 250, 250)",overflow: "auto"}}>
      {
        chatType === "viewConversation"&& !uuid?
        <NoDashboardData chatType={chatType} />
        :
          <Dialogue chat_type={chatType} sendUrl={sendUrl} key={chatType} uuid={uuid} /> 
      }
      </div>
      {
        chatType === "report" && dashboardId?
          <div style={{ flex: "1",overflow: "auto" }} className="dashbord_page">
            <DashboardPage key={dashboardKey} isShowReport={false} dashboardId={dashboardId} dashboardSlug="-" onError={handleError} />
          </div>
          : null
      }
      {
        chatType === "report" && isShowNoData&&
        (
          <NoDashboardData chatType={chatType} />
        )
      }
    </div>
  );
}

function registerDialogueListRoute() {
  routes.register(
    "Dialogue.List.autopilot",
    routeWithUserSession({
      path: "/autopilot",
      title: "autopilot",
      render: (pageProps) => <DialogueList {...pageProps} chatType="autopilot" />,
    })
  );

  routes.register(
    "Dialogue.List.Dialogue",
    routeWithUserSession({
      path: "/dialogue-list",
      title: "Dialogue",
      render: (pageProps) => <DialogueList {...pageProps} chatType="viewConversation" />,
    })
  );


  routes.register(
    "Dialogue.List.Report",
    routeWithUserSession({
      path: "/report-route",
      title: "Report",
      render: (pageProps) => <DialogueList {...pageProps} chatType="report" />,
    })
  );
  
  routes.register(
    "Dialogue.List",
    routeWithUserSession({
      path: "/",
      title: window.W_L.dialogue,
      render: (pageProps) => <DialogueList {...pageProps} currentPage="testdialogue" chatType="chat" />,
    })
  );
  
}

registerDialogueListRoute();
