import React, { useEffect, useState,useRef } from "react";
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
import StepModal from "./components/StepModal/StepModal";
import toast from 'react-hot-toast';
import "./index.less";

function DashboardsPrettify() {
    const stepModalRef = useRef();
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
                    <span className="table-main-title" style={{color:"#2196f3",cursor: "pointer"}} onClick={() => handleClickHtml(item)}>{item.report_name}</span>
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

    // open html
    const handleClickHtml = async (item) => {
        if(item.is_generate === 2 && item.html_name){
            const html_name = item.html_name.split(".")[0];
            const url = window.location.protocol + "//" + window.location.host + "/pretty_dashboard/" + html_name;
            window.open(url);
        }else{
            switch (item.is_generate) {
                case 0:
                    toast.error(window.W_L.waiting);
                    break;
                case 1:
                    toast.error(window.W_L.generating);
                    break;
                case -1:
                    toast.error(window.W_L.fail);
                    break;
                default:
                    break;
            }
        }
        // const res = await axios.get(`/api/dashboard/${id}`);
        // if (res.code === 200) {
        //     window.open(res.data);
        // }
    };

    const handleDelete = async (id) => {
        Modal.confirm({
            title: window.W_L.confirm_delete,
            content: window.W_L.confirm_delete_tip,
            onOk: async () => {
                try {
                    const res = await axios.delete(`/api/pretty_dashboard/delete/${id}`);
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
        const res = await axios.get("/api/pretty_dashboard");
        if (res.code === 200) {
            res.data.sort(function(a,b){
                return Date.parse(b.created_at) - Date.parse(a.created_at);
            });
            setDashboardsPrettifyList(res.data);
        }
        setIsLoading(false);
    };
    const handleClick = () => {
        stepModalRef.current.openModal();
    };
    useEffect(() => {
        getDashboardsPrettifyList();
    }, []);
    const pre_btn = () => {
        return (
            <button className="dashbord_button" onClick={handleClick}> <span class="text">
                

                <strong>大屏美化</strong>
            <svg t="1703404031302" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="6245" width="20" height="20"><path d="M954.368 999.296a69.376 69.376 0 0 1-47.04-18.304l-556.8-510.464a69.632 69.632 0 0 1-4.288-98.368 69.632 69.632 0 0 1 98.368-4.224l556.8 510.464a69.632 69.632 0 0 1-47.04 120.896z" fill="#333333" p-id="6246"></path><path d="M571.52 648.384a69.376 69.376 0 0 1-47.04-18.304L350.528 470.528a69.632 69.632 0 0 1-4.288-98.368 69.632 69.632 0 0 1 98.368-4.224l174.016 159.488a69.632 69.632 0 0 1-47.04 120.896z" fill="#FFCC00" p-id="6247"></path><path d="M1000.832 163.968h-185.6a23.232 23.232 0 0 1-23.232-23.168 23.232 23.232 0 0 1 23.232-23.232h185.6a23.232 23.232 0 0 1 23.232 23.232 23.232 23.232 0 0 1-23.232 23.168z" fill="#333333" p-id="6248"></path><path d="M722.368 256.768h-185.6a23.232 23.232 0 0 1-23.232-23.168 23.232 23.232 0 0 1 23.232-23.232h185.6a23.232 23.232 0 0 1 23.232 23.232 23.232 23.232 0 0 1-23.232 23.168z" fill="#333333" p-id="6249"></path><path d="M351.168 163.968h-185.6a23.232 23.232 0 0 1-23.232-23.168 23.232 23.232 0 0 1 23.232-23.232h185.6a23.232 23.232 0 0 1 23.232 23.232 23.232 23.232 0 0 1-23.232 23.168z" fill="#333333" p-id="6250"></path><path d="M211.904 522.176h-185.6a23.232 23.232 0 0 1-23.232-23.168 23.232 23.232 0 0 1 23.232-23.232h185.6a23.232 23.232 0 0 1 23.232 23.232 23.232 23.232 0 0 1-23.232 23.168z" fill="#333333" p-id="6251"></path><path d="M884.736 233.6V48a23.232 23.232 0 0 1 46.4 0v185.6a23.232 23.232 0 0 1-46.4 0z" fill="#333333" p-id="6252"></path><path d="M606.336 326.4V140.8a23.232 23.232 0 0 1 46.4 0v185.6a23.232 23.232 0 0 1-46.4 0z" fill="#333333" p-id="6253"></path><path d="M235.136 233.6V48a23.232 23.232 0 0 1 46.4 0v185.6a23.232 23.232 0 0 1-46.4 0z" fill="#333333" p-id="6254"></path><path d="M95.936 591.808v-185.6a23.232 23.232 0 0 1 46.4 0v185.6a23.232 23.232 0 0 1-46.4 0z" fill="#333333" p-id="6255"></path></svg>    
            
            </span>
            <span class="blob"></span>
            <span class="blob"></span>
            <span class="blob"></span>
            <span class="blob"></span>
          </button>
        )
    }
        return (
            <div className="page-queries-list">
                <StepModal ref={stepModalRef}></StepModal>
                <div className="container">
                    <PageHeader
                        title={window.W_L.dashboards_prettify_all}
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
