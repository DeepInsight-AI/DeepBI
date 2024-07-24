import { useState, useCallback,useEffect,useRef } from 'react';
import { axios } from "@/services/axios";

export const useChartCode = (sendSocketMessage,saveDashboardId, props, successSetting,CharttableDate,new_sql,DashboardId,sendDashId) => {
  const [state, setState] = useState({ dashboard_id: null });
  const DashId = useRef(null);
  useEffect(() => {
    if(DashboardId){
      DashId.current = DashboardId;
    }
}, [DashboardId])

  const saveChart = useCallback(async (options, type = null,task_id) => {
    // console.log(options[0].globalSeriesType, 'globalSeriesType')
    // console.log(options[0].columnMapping, 'columnMapping')
    const sqlId = new_sql.current;
    // console.log("savasqlId",sqlId)
    const data = {
      description: "",
      name: window.W_L.CHART,
      query_id: sqlId,
      type: "CHART",
      options: {
        alignYAxesAtZero: false,
        globalSeriesType: options[0].globalSeriesType,
        coefficient: 1,
        columnMapping: options[0].columnMapping,
        dateTimeFormat: "DD/MM/YY HH:mm",
        direction: {
          type: "counterclockwise"
        },
        error_y: {
          type: "data",
          visible: true
        },
        legend: {
          enabled: true,
          placement: "auto",
          traceorder: "normal"
        },
        missingValuesAsZero: true,
        numberFormat: "0,0[.]00000",
        percentFormat: "0[.]00%",
        series: {
          stacking: null,
          error_y: {
            type: "data",
            visible: true
          },
        },
        seriesOptions: {},
        showDataLabels: true,
        sizemode: "diameter",
        sortX: false,
        textFormat: "",
        valuesOptions: {},
        xAxis: {
          type: "category",
          labels: {
            enabled: true,
          }
        },
        yAxis: [
          { type: "linear" },
          { type: "linear", opposite: true }
        ],
      }
    }
    try {
      const res = await axios.post(`/api/visualizations`, data);
      // console.log(res, 'save_success');
      if (type === "edit") {
        // console.log("second saveChart")
        await dashboardsId(res, type,task_id);
      } else {
        await publishQuery(null,task_id);
        // await newReport(type,task_id);
      }
  
    } catch (err) {
      // console.log(err, 'save_error');
      sendSocketMessage(500,'bi','chart_code',err,task_id)
    }
  }, [state, dashboardsId, publishQuery]);
  
  // publishQuery
const publishQuery = useCallback(async (type = null,task_id) => {
  // console.log(task_id,"publishQuery")
  const sqlId = new_sql.current;
    const CharttableDate = CharttableDate;
    const data = {
      id: sqlId,
      is_draft: true,
      version: 1
    }
    try {
      const res = await axios.post(`/api/queries/${sqlId}`, data);
      // console.log(res, 'publishQuery_success');
      if (CharttableDate && CharttableDate.dashboard_id) {
        await dashboardsId(res,type,task_id);
      } else {
        await newReport(type,task_id);
      }
    } catch (err) {
      // console.log(err, 'publishQuery_error');
      sendSocketMessage(500, 'bi', 'chart_code', err,task_id)
    }
}, [state, CharttableDate, dashboardsId, newReport, sendSocketMessage]);
// deleteReports
const deleteReports = (async (report_name) => {
  const res = await axios.get(`/api/dashboards?order&page=1&page_size=20&q=${report_name}`);
  if(res.results.length>0){
    res.results.forEach(async (item,index)=>{
      if(item.name===report_name){
        const resdel = await axios.delete(`/api/dashboards/${item.id}`);
      }
    })
  }
});
// newReport
const newReport = useCallback(async (type = null,task_id) => {
    const data = {
      name: window.W_L.temporary_dashboard
    }
    try {
      await deleteReports(window.W_L.temporary_dashboard);
      const res = await axios.post('/api/dashboards', data);
      // console.log(res, 'newReport_success');
      // setState(prevState => ({ ...prevState, dashboard_id: res.id }));
      saveDashboardId("dashboard_id", res.id);
      await dashboardsId(res, type,task_id);
    } catch (err) {
      // console.log(err, 'newReport_error');
    }
  }, [setState, saveDashboardId, dashboardsId]);
 
// DashboardsId
const dashboardsId = useCallback(async (response, type = null,task_id) => {
  let dashboard_id;
  // console.log("dashboardId.current:", DashId.current);
// console.log("response.id:", response.id);
    if (DashId.current) {
      dashboard_id = DashId.current;
    }else{
      dashboard_id = response.id;
    }
    // if (!CharttableDate.publicUrl && type === "ask_data") {
    //   sendSocketMessage(500, 'bi', 'ask_data', [],task_id)
    //   return
    // }
   
    try {
      const res = await axios.get(`/api/dashboards/${dashboard_id}`);
      // console.log(res, 'get_dashboard_id_success');
      if (type === "delete") {
        deleteReport(response, res,task_id);
        return
      }
      if (type === "ask_data") {
        widgetsJson(res, type,task_id);
        return
      }
      if (type === "getPath" || type === "delGetPath") {
        sendSocketMessage(200, 'bi', type==="getPath"?'chart_code':'delete_chart', res,task_id)
        sendDashId(dashboard_id)
        successSetting()
        return
      }
      await queriesId(res, type,task_id);
    } catch (err) {
      // console.log(err, 'get_dashboard_id_error');
    }
  }, [state, setState, sendSocketMessage, deleteReport, widgetsJson, props, successSetting, queriesId,saveDashboardId,sendDashId,DashId]);
  
  const queriesId = useCallback(async (response, type,task_id) => {
    const sqlId = new_sql.current;
    try {
      const res = await axios.get(`/api/queries/${sqlId}`);
      // console.log(res, 'get_sql_id_success');
      await editReport(res, response, type,task_id);
    } catch (error) {
      // console.log(error, 'get_sql_id_error');
    }
  }, [state, editReport]);
  
  // editReport
  const editReport = useCallback(async (response, response_dashboards, type,task_id) => {
    const dashboard_id = DashId.current;
    const data = {
      created_at: new Date(),
      dashboard_id,
      options: {
        isHidden: false,
        parameterMappings: {},
        position: {
          autoHeight: false,
          col: 0,
          maxSizeX: 6,
          maxSizeY: 1000,
          minSizeX: 1,
          minSizeY: 5,
          row: 0,
          sizeX: 6,
          sizeY: 12
        }
      },
      text: "",
      visualization_id: response.visualizations[response.visualizations.length - 1].id,
      width: 1
    }
    try {
      const res = await axios.post(`/api/widgets`, data);
      // console.log(res, 'add_success');
      await saveReport(res, response_dashboards, type,task_id);
    } catch (err) {
      // console.log(err, 'add_error')
    }
  }, [state, saveReport]);
  
  // saveReport
  const saveReport = useCallback(async (response, response_dashboards, type,task_id) => {
    let options=response.options
    options.position.row=response_dashboards.widgets.length*12
    let data = {
      dashboard_id: response.dashboard_id,
      id: response.id,
      options,
      text: "",
      visualization_id: response.visualization.id,
      width: 1
    }
    try {
      const res = await axios.post(`/api/widgets/${response.id}`, data);
      await widgetsJson(res, type,task_id);
    } catch (error) {
      console.log(error, 'saveReport_error')
    }
  }, [widgetsJson]);
  
  // widgetsJson
  const widgetsJson = useCallback(async (response, type,task_id) => {
    const dashboard_id = DashId.current;
    const sqlId = new_sql.current;
    try {
      if (type === "ask_data") {
        let content = []
        const arr = response.widgets.map(async (item, index) => {
          const res = await axios.get(`/api/queries/${item.visualization.query.id}/results/${item.visualization.query.latest_query_data_id}.json`)
          // console.log(res, 'jsonget_success')
          content.push({ data: res.query_result.data, table_name: item.visualization.query.name })
        })
        Promise.all(arr).then(res => {
          // console.log(content, '')
          sendSocketMessage(200, 'bi', 'ask_data', content,task_id)
        })
        return
      }
      const res = await axios.get(`/api/queries/${sqlId}/results/${response.visualization.query.latest_query_data_id}.json`)
      if (type !== "edit") {
        // await publishReport(task_id)
        sendDashId(dashboard_id)
      sendSocketMessage(200, 'bi', 'chart_code', res,task_id)
      successSetting()
      } else {
        dashboardsId(res, "getPath",task_id)
      }
    } catch (error) {
      // console.log(error, 'jsonget_error')
    }
  }, [state, sendSocketMessage,new_sql, dashboardsId]);
  


  // publishReport
  // const publishReport = useCallback(async (task_id) => {
  //    const dashboard_id = DashId.current;
  //   const data = {
  //     id: dashboard_id,
  //     is_draft: false,
  //   }
  
  //   try {
  //     const res = await axios.post(`/api/dashboards/${dashboard_id}`, data)
  //     // await shareQuery(task_id);
  //     sendDashId(dashboard_id)
  //     sendSocketMessage(200, 'bi', 'chart_code', res,task_id)
  //     successSetting()
  //   } catch (error) {
  //   }
  // }, [state,successSetting,sendSocketMessage]);
  
  // shareQuery
  // const shareQuery = useCallback(async (task_id) => {
  //    const dashboard_id = DashId.current;
  //   try {
  //     const res = await axios.post(`/api/dashboards/${dashboard_id}/share`)
  //     console.log(res.public_url, 'send_public_url')
  //     // props.sendDashId(res.public_url)
  //     successSetting()
  //     sendSocketMessage(200, 'bi', 'chart_code', res,task_id)
  //   } catch (error) {
  //     console.log(error, 'share_error')
  //   }
  // }, [state, props, successSetting, sendSocketMessage]);
  
  // deleteReport
  const deleteReport = useCallback(async (nameList, response,task_id) => {
    try {
      const arr = response.widgets.map(async (item, index) => {
        if (nameList.includes(item.visualization.query.name)) {
          await axios.delete(`/api/widgets/${item.id}`)
        }
      })
      Promise.all(arr).then(res => {
        dashboardsId(res, "delGetPath",task_id)
      })
  
    } catch (error) {
    }
  }, [dashboardsId]);

  return { DashId,saveChart,dashboardsId };
};