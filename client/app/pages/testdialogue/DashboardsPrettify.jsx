import React, { useEffect, useState } from "react";
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
import DeleteOutlinedIcon from "@ant-design/icons/DeleteOutlined";
import toast, { Toaster } from 'react-hot-toast';
import "./index.less";

function DashboardsPrettify() {
    const [DashboardsPrettifyList, setDashboardsPrettifyList] = useState([]);
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
                    if (res.code === 200) {
                        toast.success(window.W_L.delete_success);
                        getDashboardsPrettifyList();
                    } else {
                        toast.error(window.W_L.delete_fail);
                    }
                } catch (error) {
                    // Handle any errors
                }
            },
            onCancel() {
                // Handle cancel action
            },
        });
    };
    const getDashboardsPrettifyList = async () => {
        setIsLoading(true);
        const res = await axios.get("/api/auto_pilot");
        if (res.code === 200) {
            setDashboardsPrettifyList(res.data);
        }
        setIsLoading(false);
    };
    useEffect(() => {
        getDashboardsPrettifyList();
    }, []);
    const pre_btn = () => {
        return (
            <button className="dashbord_button">
            <span></span>
            <span></span>
            <span></span>
            <span></span> Hover me
          </button>
        )
    }
        return (
            <div className="page-queries-list">
                <div className="container">
                    <Toaster />
                    <PageHeader
                        title={window.W_L.dashboards_prettify_all}
                        actions={
                            pre_btn()
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
                                        dataSource={DashboardsPrettifyList}
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

    function registerDashboardsPrettifyRoute() {
        routes.register(
            "Dialogue.List.dashboards_prettify",
            routeWithUserSession({
                path: "/dashboards_prettify",
                title: "dashboards_prettify",
                render: (pageProps) => <DashboardsPrettify {...pageProps} />,
            })
        );
    }

    registerDashboardsPrettifyRoute();
