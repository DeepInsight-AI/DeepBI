import { useState, useCallback,useEffect,useRef } from 'react';
import { axios } from "@/services/axios";

export const useSql = (tab_id,sendSocketMessage,errorSetting) => {
  const [state, setState] = useState({ SelectValue: null, sqlId: null });
  const new_sql = useRef(null);
  const tabId =useRef(null);
  useEffect(() => {
    if(state.sqlId){
      new_sql.current = state.sqlId;
    }
}, [state.sqlId])
useEffect(() => {
  if(tab_id){
    tabId.current = tab_id;
  }
}, [tab_id])

const findQueryResult = useCallback(async (response,type,name,task_id) => {
  try {
    const res = await axios.get(`/api/query_results/${response.job.query_result_id}`);
    // console.log(res, 'gettable_data_success');
    if(res.query_result.data.rows.length===0){
      sendSocketMessage(500,'bi','mysql_code',"sql没有查询到数据",task_id)
      return
    }
    
    if(type==='test'){
      await saveSql(res.query_result.query,name,task_id)
    }else{
      sendSocketMessage(200,'bi','mysql_code',res.query_result.data,task_id)
    }

  } catch (err) {
    // console.log(err,'gettable_data_error');
  }
}, [sendSocketMessage,saveSql]);

const findJob = useCallback(async (response,type,name,task_id,max=50) => {
  let num = 0;
  let isCalled = false;
  const timer = setInterval(async () => {
    num++;
    if(num>max){
      clearInterval(timer);
      errorSetting()
      return
    }
    try {
      const res = await axios.get(`/api/jobs/${response.job.id}`);
      if (res.job.query_result_id&& !isCalled) {
        // console.log(res, 'getquery_result_id');
        isCalled = true;
        clearInterval(timer);
        await findQueryResult(res,type,name,task_id);
      } else {
        if(res.job.error){
          clearInterval(timer);
          sendSocketMessage(500,'bi','mysql_code',res.job.error,task_id)
          return
        }
        // console.log(res, 'query_result_id_undefined');
      }
    } catch (err) {
      // console.log(err, 'getquery_result_id_error');
    }
  }, 200);
}, [errorSetting, sendSocketMessage, findQueryResult]);

const executeSql = useCallback(async (response,task_id) => {
  // const sqlId = new_sql.current;
  // console.log("sqlId", sqlId);
  const data = {
    id: response.id,
    parameters: {},
    apply_auto_limit: true,
    max_age: 0
  };

  try {
    const res = await axios.post(`/api/queries/${response.id}/results`, data);
    // console.log(res, 'executeSql_success');
    await findJob(res,"execute",null,task_id);
  } catch (err) {
    // console.log(err, 'executeSql_error');
    sendSocketMessage(500,'bi','mysql_code',"",task_id)
  }
}, [findJob,sendSocketMessage]);

  const saveSql = useCallback(async (query,name,task_id) => {
    const data = {
      data_source_id: tabId.current,
      latest_query_data_id: null,
      name,
      options: {
        apply_auto_limit: true,
        parameters: []
      },
      query,
      schedule: null,
      tags: []
    };
    try {
      const res = await axios.post('/api/queries', data);
      // console.log(res, 'saveSql_success');
      setState(prevState => ({ ...prevState, sqlId: res.id }));
      await executeSql(res,task_id);
    } catch (err) {
      // console.log(err, 'saveSql_error');
    }
  }, [executeSql]);



  const testAndVerifySql = useCallback(async (query,name,task_id) => {
    // console.log("tabId",tabId)
    const data = {
      apply_auto_limit : true,
      data_source_id : tabId.current,
      max_age : 0,
      parameters : {},
      query,
    }
    try {
      const res = await axios.post(`/api/query_results`,data);
      await findJob(res,"test",name,task_id);
      // console.log(res, 'testAndVerifySql_success');
    } catch (err) {
      // console.log(err, 'testAndVerifySql_error');
    }
  }, [findJob]);

 



  return { state, saveSql, executeSql, testAndVerifySql, findJob, findQueryResult,new_sql };
};