import React,{useEffect,useState} from "react";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import routes from "@/services/routes";
import ItemsTable, { Columns } from "@/components/items-list/components/ItemsTable";
import Paginator from "@/components/Paginator";
import { axios } from "@/services/axios";
import "./index.less";

function AutopilotHistory() {
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
       <div className="bg-white tiled table-responsive">
                  {/* <ItemsTable
                    items={controller.pageItems}
                    loading={!controller.isLoaded}
                    columns={tableColumns}
                    orderByField={controller.orderByField}
                    orderByReverse={controller.orderByReverse}
                    toggleSorting={controller.toggleSorting}
                  /> */}
                  {/* <Paginator
                    showPageSizeSelect
                    totalCount={controller.totalItemsCount}
                    pageSize={controller.itemsPerPage}
                    onPageSizeChange={itemsPerPage => controller.updatePagination({ itemsPerPage })}
                    page={controller.page}
                    onChange={page => controller.updatePagination({ page })}
                  /> */}
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
