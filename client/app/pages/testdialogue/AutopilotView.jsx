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

function AutopilotView(props) {
    useEffect(() => {
      console.log(props.query,"props.query====")
    }, []);
  return (
    <div className="auto-pilot-list">
        123123123
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
