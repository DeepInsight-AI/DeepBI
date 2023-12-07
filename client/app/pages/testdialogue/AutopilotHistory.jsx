import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import Table from "antd/lib/table";
import ItemsTable, { Columns } from "@/components/items-list/components/ItemsTable";
import Link from "@/components/Link";
import EmptyState from "@/components/items-list/components/EmptyState";
import Layout from "@/components/layouts/ContentWithSidebar";
import { axios } from "@/services/axios";
import "./index.less";

function AutopilotHistory() {
  const [AutoPilotList, setAutoPilotList] = useState([]);
const [isLoading, setIsLoading] = useState(true);
  const listColumns = [
    Columns.favorites({ className: "p-r-0" }),
    Columns.custom.sortable(
      (text, item) => (
        <React.Fragment>
          <Link className="table-main-title" href={"queries/" + item.id}>
            {item.report_name}
          </Link>
          {/* <QueryTagsControl className="d-block" tags={item.tags} isDraft={item.is_draft} isArchived={item.is_archived} /> */}
        </React.Fragment>
      ),
      {
        title: window.W_L.name,
        field: "report_name",
        width: null,
      }
    ),
    Columns.dateTime.sortable({ title: window.W_L.create_time, field: "created_at", width: "1%" }),
    Columns.dateTime.sortable({ title: window.W_L.task_status, field: "is_generate", width: "1%" }),
  ];
  const  getAutoPilotList = async () => {
    setIsLoading(true);
    const res = await axios.get("/api/auto_pilot");
    if(res.code === 200){
        setAutoPilotList(res.data);
    }
    setIsLoading(false);
    };
    useEffect(() => {
        getAutoPilotList();
    }, []);
  return (
    <div className="page-alerts-list">
        <Layout.Content style={{width:"95% !important"}}>
        <React.Fragment>
        <div className="bg-white tiled table-responsive">
            <Table
            className="table-data"
            rowKey="id"
            size="middle"
            columns={listColumns}
            dataSource={AutoPilotList}
            pagination={false}
            loading={isLoading}
            locale={{
            emptyText: (
                <EmptyState className="" />
            ),
            }}
        />
        </div>
        </React.Fragment>
        </Layout.Content>
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
