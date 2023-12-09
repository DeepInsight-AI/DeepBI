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
        <div className={`auto-pilot`} >
            <div className={`chat user autopilot-item`}>
                <p>{window.W_L.report_name}:{autoPilot.report_name}</p>
                <p>{window.W_L.report_desc}:{autoPilot.report_desc}</p>
                {/* <LogWorkflow Cardloading={false} logData={[...(autoPilot.chat_log || [])]} /> */}
                <LogWorkflow Cardloading={false} logData={autoPilot.chat_log} />
              {
                autoPilot.html_code?
                <AutoPilot title={autoPilot.report_name} content={autoPilot.html_code}></AutoPilot>
                :
                <div style={{textAlign:"center",marginTop:"50px"}}>
                    <svg t="1701954689007" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="1988" width="200" height="200"><path d="M777 806.7c0 38.7-31.1 70.3-69.7 70.3H225.9c-38.7 0-69.9-31.6-69.9-70.3V174.4c0-38.7 31.2-70.4 69.9-70.4h481.4c38.7 0 69.7 31.7 69.7 70.4v632.3z" fill="#FFFFFF" p-id="1989"></path><path d="M707.3 886H225.9c-43.5 0-78.9-35.8-78.9-79.3V174.4c0-43.5 35.4-79.4 78.9-79.4h481.4c43.5 0 79.7 35.9 79.7 79.4v632.3c0 43.5-36.2 79.3-79.7 79.3zM225.9 113c-33.7 0-59.9 27.7-59.9 61.4v632.3c0 33.7 26.2 61.3 59.9 61.3h481.4c33.7 0 60.7-27.6 60.7-61.3V174.4c0-33.7-27-61.4-60.7-61.4H225.9z" fill="#282828" p-id="1990"></path><path d="M732 763.1c0 33.3-26.1 60.9-59.4 60.9H251.7c-33.3 0-59.7-27.6-59.7-60.9V226.9c0-33.3 26.4-60.9 59.7-60.9h420.9c33.3 0 59.4 27.5 59.4 60.9v536.2z" fill="#50BCFF" p-id="1991"></path><path d="M361.8 251.9H208.6c-4.9 0-8.9-4-8.9-8.9s4-8.9 8.9-8.9h153.1c4.9 0 8.9 4 8.9 8.9s-3.9 8.9-8.8 8.9zM549.2 313H208.6c-4.9 0-8.9-4-8.9-8.9s4-8.9 8.9-8.9h340.5c4.9 0 8.9 4 8.9 8.9s-3.9 8.9-8.8 8.9zM652 448.4H208.6c-4.9 0-8.9-4-8.9-8.9s4-8.9 8.9-8.9H652c4.9 0 8.9 4 8.9 8.9s-4 8.9-8.9 8.9zM588.4 515.8H208.6c-4.9 0-8.9-4-8.9-8.9s4-8.9 8.9-8.9h379.7c4.9 0 8.9 4 8.9 8.9 0.1 4.9-3.9 8.9-8.8 8.9z" fill="#282828" p-id="1992"></path><path d="M673.3 113.3c0 19.6-15.9 35.5-35.5 35.5h-346c-19.6 0-35.5-15.9-35.5-35.5v-6.7c0-19.6 15.9-35.5 35.5-35.5h346c19.6 0 35.5 15.9 35.5 35.5v6.7z" fill="#1A6DFF" p-id="1993"></path><path d="M637.8 157.6h-346c-24.5 0-44.4-19.9-44.4-44.4v-6.7c0-24.5 19.9-44.4 44.4-44.4h346c24.5 0 44.4 19.9 44.4 44.4v6.7c0 24.5-19.9 44.4-44.4 44.4zM291.9 80c-14.7 0-26.6 11.9-26.6 26.6v6.7c0 14.7 11.9 26.6 26.6 26.6h346c14.7 0 26.6-11.9 26.6-26.6v-6.7c0-14.7-11.9-26.6-26.6-26.6h-346z" fill="#282828" p-id="1994"></path><path d="M849.7 758.4c0 106.3-86.2 192.4-192.4 192.4-106.3 0-192.4-86.2-192.4-192.4C464.8 652.1 551 566 657.3 566c106.2 0 192.4 86.1 192.4 192.4z" fill="#FFFFFF" p-id="1995"></path><path d="M657.3 959.7c-111 0-201.3-90.3-201.3-201.3s90.3-201.3 201.3-201.3 201.3 90.3 201.3 201.3-90.3 201.3-201.3 201.3z m0-384.8c-101.2 0-183.5 82.3-183.5 183.5S556.1 942 657.3 942s183.6-82.3 183.6-183.6c-0.1-101.2-82.4-183.5-183.6-183.5z" fill="#282828" p-id="1996"></path><path d="M657.3 553.6v204.9h203.6c-0.2-112.7-91.2-204-203.6-204.9z" fill="#1A6DFF" p-id="1997"></path><path d="M860.9 767.3H657.3c-4.9 0-8.9-4-8.9-8.9V553.6c0-2.4 0.9-4.6 2.6-6.3 1.7-1.7 3.6-2.4 6.3-2.6 116.9 0.9 212.2 96.8 212.4 213.7 0 2.4-0.9 4.6-2.6 6.3-1.6 1.7-3.9 2.6-6.2 2.6z m-194.8-17.8h185.6C847 649.3 766.2 568 666.1 562.7v186.8z" fill="#282828" p-id="1998"></path><path d="M794.1 903.7c-2.3 0-4.6-0.9-6.3-2.6L652.1 764.7c-3.5-3.5-3.4-9.1 0-12.6 3.5-3.5 9.1-3.4 12.6 0l135.7 136.5c3.5 3.5 3.4 9.1 0 12.6-1.8 1.7-4 2.5-6.3 2.5z" fill="#282828" p-id="1999"></path></svg>
                    <p style={{marginTop:"20px"}}>{window.W_L.task_processing}</p>
                </div>
              } 
            
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
