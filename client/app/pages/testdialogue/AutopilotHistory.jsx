import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import Table from "antd/lib/table";
import ItemsTable, { Columns } from "@/components/items-list/components/ItemsTable";
import Link from "@/components/Link";
import EmptyState from "@/components/items-list/components/EmptyState";
import Layout from "@/components/layouts/ContentWithSidebar";
import Tag from "antd/lib/tag";
import { axios } from "@/services/axios";
import { durationHumanize, formatDate, formatDateTime } from "@/lib/utils";
import "./index.less";

function AutopilotHistory() {
  const [AutoPilotList, setAutoPilotList] = useState([]);
const [isLoading, setIsLoading] = useState(true);
  // const listColumns = [
  //   Columns.favorites({ className: "p-r-0" }),
  //   Columns.custom.sortable(
  //     (text, item) => (
  //       <React.Fragment>
  //         <Link className="table-main-title" href={"queries/" + item.id}>
  //           {item.report_name}
  //         </Link>
  //         {/* <QueryTagsControl className="d-block" tags={item.tags} isDraft={item.is_draft} isArchived={item.is_archived} /> */}
  //       </React.Fragment>
  //     ),
  //     {
  //       title: window.W_L.name,
  //       field: "report_name",
  //       width: null,
  //       sorter: false,
  //     }
  //   ),
  //   Columns.dateTime.sortable({ title: window.W_L.create_time, field: "created_at", sorter: false}),
  //   Columns.custom.sortable(
  //   (text, item) => (
  //       <React.Fragment>
  //       <Tag color={item.is_generate ? "green" : "red"}>{item.is_generate ? window.W_L.success : window.W_L.fail}</Tag>
  //       </React.Fragment>
  //   ),
  //       { title: window.W_L.task_status, field: "is_generate", sorter: false}),


  
  // ];
  const listColumns = [
    {
      title: window.W_L.name,
      dataIndex: "report_name",
      width: null,
      render: (text, item) => (
        <React.Fragment>
          <Link className="table-main-title" href={"autopilot/" + item.id}>
            {item.report_name}
          </Link>
        </React.Fragment>
      )
    },
    {
      title: window.W_L.create_time,
      dataIndex: "created_at",
      render: (text, item) => (
        formatDateTime(text)
      )
    },
    {
      title: window.W_L.task_status,
      dataIndex: "is_generate",
      render: (text, item) => (
        <Tag color={item.is_generate ? "green" : "red"}>
          {item.is_generate ? window.W_L.success : window.W_L.fail}
        </Tag>
      )
    }
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
    <div className="auto-pilot-list">
         <Layout>
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
        </Layout>
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
