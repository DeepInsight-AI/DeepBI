import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import Table from "antd/lib/table";
import ItemsTable, { Columns } from "@/components/items-list/components/ItemsTable";
import Link from "@/components/Link";
import EmptyState from "@/components/items-list/components/EmptyState";
import Layout from "@/components/layouts/ContentWithSidebar";
import Tag from "antd/lib/tag";
import Modal from "antd/lib/modal";
import { axios } from "@/services/axios";
import PageHeader from "@/components/PageHeader";
import  DeleteOutlinedIcon  from "@ant-design/icons/DeleteOutlined";
import toast from 'react-hot-toast';
import "./index.less";

function AutopilotHistory() {
  const [AutoPilotList, setAutoPilotList] = useState([]);
const [isLoading, setIsLoading] = useState(true);
const listColumns = [
    {
      title: window.W_L.name,
      dataIndex: "report_name",
      width: null,
      align: "center",
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
      align: "center",
    },
    {
      title: window.W_L.task_status,
      dataIndex: "is_generate",
      align: "center",
      render: (text, item) => (
        <React.Fragment>
            {item.is_generate === 0 && <Tag color="gold">{window.W_L.waiting}</Tag>}
            {item.is_generate === 1 && <Tag color="blue">{window.W_L.generating}</Tag>}
            {item.is_generate === 2 && <Tag color="green">{window.W_L.success}</Tag>}
            {item.is_generate === -1 && <Tag color="red">{window.W_L.fail}</Tag>}
        </React.Fragment>
      )
    },
    {
      title: "",
      dataIndex: "actions",
      width: 50,
      align: "center",
      render: (text, item) => (
        <React.Fragment>
          <DeleteOutlinedIcon style={{ color: "#f08282", fontSize: "15px" }} onClick={() => handleDelete(item.id)} />
        </React.Fragment>
      )
    }
  ];
  const handleDelete = async (id) => {
    Modal.confirm({
      title: window.W_L.confirm_delete,
      content: window.W_L.confirm_delete_tip,
      onOk: async () => {
        try {
          const res = await axios.delete(`/api/auto_pilot/delete/${id}`);
          if(res.code === 200){
            toast.success(window.W_L.delete_success);
            getAutoPilotList();
          }else{
            toast.error(window.W_L.delete_fail);
          }
          // Handle the response as needed
          
        } catch (error) {
          // Handle any errors
        }
      },
      onCancel() {
        // Handle cancel action
      },
    });
  };
  const  getAutoPilotList = async () => {
    setIsLoading(true);
    const res = await axios.get("/api/auto_pilot");
    if(res.code === 200){
      res.data.sort(function(a,b){
        return Date.parse(b.created_at) - Date.parse(a.created_at);
      });
        setAutoPilotList(res.data);
    }
    setIsLoading(false);
    };
    useEffect(() => {
        getAutoPilotList();
    }, []);
  return (
    <div className="page-queries-list">
      <div className="container">
        <PageHeader
          title={window.W_L.all_autopilot}
          actions={
              <Link.Button block type="primary" href="autopilot">
                <i className="fa fa-plus m-r-5" aria-hidden="true" />
                {window.W_L.auto_pilot}
              </Link.Button>
          }
        />
         <Layout>
        <Layout.Content>
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
