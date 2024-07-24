import React, { useState, useEffect, useRef, useCallback } from "react";
import { axios } from "@/services/axios";
import {websocket,createWebSocket,lockReconnect,setLockReconnect,setStopGeneration,stopGeneration} from './websocket.js';
import { useSql } from './method/useSql.js';
import { useChartCode } from "./method/useChartCode.js";
import { dialogueStorage } from "./method/dialogueStorage.js";
import DialogueTop from "../DialogueTop";
// import OpenKey from "../OpenKey"
import DialogueContent from "../DialogueContent"
import toast from "react-hot-toast";
import MenuMask from "../MenuMask/index.jsx";
import "./index.less";
import moment from "moment";
import AutoPilot from "../AutoPilot/AutoPilot.jsx";

const Dialogue = (props) => {
  const {chat_type,sendUrl,uuid} =props
  const OpenKeyRef = useRef();
  const DialogueContentRef = useRef();
  const new_logData = useRef([]);
  const messagesRef = useRef();
  const Charttable_id = useRef(null);
  const Charttable_item = useRef({});
  const Dashboard_id = useRef(null);
  const CharttableD_date = useRef(null);
  const [dashboardId, setDashboardId] = useState(null);
  const [CharttableDate, setCharttableDate] = useState(null);
  const [LoadingState, setLoadingState] = useState(false);
  const [startUse ,setStartUse] = useState(false);
  const [SendTableDate, setSendTableDate] = useState(0);
  const [LoadingMask,setLoadingMask] =useState(false);
  const [selectTableName, setSelectTableName] = useState([]);
  const [selectTableDesc, setSelectTableDesc] = useState({});
  const selectTableDescRef = useRef();
  const selectTableNameRef = useRef();
  const [ConfirmLoading, setConfirmLoading] = useState(false);
  const sourceTypeRef = useRef("mysql");
  const [percent,setPercent] = useState(0);
  let timeoutId = null;
  const [state, setState] = useState({
    messages: [],
    inputMessage: "",
    lockReconnect :false,
    data_type:null,
    newInputMessage:"",
    logData:[],
  });
  useEffect(() => {
  }, [chat_type])
  useEffect(() => {
  if(state.logData){
    new_logData.current = state.logData;
  }
}, [state.logData])
  useEffect(() => {
    getDialogueDashboardStorage();
    openSocket();
  }, []);
  useEffect(() => {
    if(uuid){
      getDialogueDashboardStorage();
    }

  }, [uuid])
  useEffect(() => {
    selectTableDescRef.current = selectTableDesc;
  }, [selectTableDesc]);
  useEffect(() => {
    selectTableNameRef.current = selectTableName;
  }, [selectTableName]);
  useEffect(() => {
    CharttableD_date.current = CharttableDate;
  }, [CharttableDate]);


const sendSocketMessage = useCallback((state, sender, data_type, content,id=0) => {
    const messgaeInfo = {
      state,
      database:sourceTypeRef.current,
      sender,
      chat_type,
      data: {
        data_type,
        databases_id:Charttable_id.current || 0,
        language_mode:window.W_L.language_mode,
        content,
      },
      id
    }
    websocket.send(JSON.stringify(messgaeInfo));
}, [state]);


const getDialogueDashboardStorage = (type=null) => {
  // || chat_type=="autopilot"
if(chat_type==="chat" ||chat_type==="report" ||chat_type==="autopilot"){
  let res=[];
  switch (chat_type) {
    case "chat":
      res = getDialogueStorage();
      break;
    case "report":
      res = getDashboard();
      break;
    default:
      break;
  }
  if (res&&res.length>0) {
    setCharttableDate(res[0].table_name);
    saveDashboardId("", res[0].Charttable_id);
    sourceTypeRef.current = res[0].type;
    Charttable_item.current = {
      label: res[0].label,
      id: res[0].id,
      type: res[0].type,
    };
    if(res[0].dashboardId){
      sendUrl(res[0].dashboardId);
      saveDashboardId("dashboard_id", res[0].dashboardId);
    }else{
      if(chat_type==="report"){
        sendUrl("new_report");
      }
      saveDashboardId("dashboard_id", null);
    }
    
    if(res[0].messages&&res[0].messages.length>0){
      setState(prevState => ({
        ...prevState,
        messages:res[0].messages
      }));
      setLoadingMask(false);
      setStartUse(true);
      stopSend('edit');
      setSendTableDate(0);
    }else{
      onUse();
    }
  }else{
    if(chat_type==="report"){
      sendUrl("");
    }
    setCharttableDate(null);
    saveDashboardId(null, null);
    setState(prevState => ({
      ...prevState,
      messages:[]
    }));
    setLoadingMask(false);
    setSendTableDate(0);
    setStartUse(false);
    Charttable_item.current = {};
  }
}else if(chat_type==="viewConversation"){
    const res = getAllStorage();
    if (res&&res.length>0 && uuid) {
      const currentList = res.filter(item=>item.uuid===uuid);
      setCharttableDate(currentList[0].table_name);
      saveDashboardId("", currentList[0].Charttable_id);
      Charttable_item.current = {
        label: currentList[0].label,
        id: currentList[0].id,
        type: currentList[0].type,
      };
      if(currentList[0].messages&&currentList[0].messages.length>0){
        setState(prevState => ({
          ...prevState,
          messages:currentList[0].messages
        }));
        setLoadingMask(false);
        setSendTableDate(1);
        setStartUse(true);
      }
    }
}
};
// setStorage
const setDialogueDashboardStorage = () => {
  let existingDialogueStorage =[]
  let Chart_Dashboard ={
    table_name:selectTableNameRef.current,
    Charttable_id:Charttable_id.current,
    ...Charttable_item.current,
  }
  Chart_Dashboard.title=window.W_L.new_dialogue + String(getAllStorage().length + 1);
  Chart_Dashboard.uuid= Date.now();
  Chart_Dashboard.messages=[];
  existingDialogueStorage.push(Chart_Dashboard)
  if(chat_type==="report"){
  addDashboard(existingDialogueStorage);
  }else if(chat_type==="chat"){
    addDialogueStorage(existingDialogueStorage);
  }else if(chat_type==="autopilot"){
    addAutopilotStorage(existingDialogueStorage);
  }
}
// clearStorage
const closeDialogue = () => {
  closeSetMessage();
  if(chat_type==="report"){
    addDashboard([])
  }else if(chat_type==="chat"){
    addDialogueStorage([])
  }else if(chat_type==="autopilot"){
    addAutopilotStorage([])
  }
  DialogueContentRef.current.sourceEdit([]);
  getDialogueDashboardStorage("report")
};
const updateCharttableDate = () => {
  setCharttableDate(selectTableNameRef.current);

  // test
  setDialogueDashboardStorage()
};

const onSuccess = useCallback((code, value,source_item,result,firstTableData) => {
  if (!lockReconnect) {
    toast.error(window.W_L.connection_seems_lost);
    setConfirmLoading(false);
    openSocket();
    return
  }
  Charttable_id.current = source_item.id;
  Charttable_item.current = {
    label: source_item.label,
    id: source_item.id,
    type: source_item.type,
  };
  sourceTypeRef.current = source_item.type

  setConfirmLoading(true);
  setState(prevState => ({
    ...prevState,
    data_type: "mysql_comment",
  }));

  if(firstTableData){
    setSelectTableName(result)
  }
  
  setSelectTableDesc({table_desc:value})
  const allIsPass= value.map(item => {
    const newFieldDesc = item.field_desc.filter(field => field.in_use === 1);
    return {
      table_name: item.table_name,
      table_comment: item.table_comment,
      field_desc: newFieldDesc
    };
  });
  const content = {
    databases_desc: "",
    table_desc: allIsPass
  }
  // console.log("content",content)
  sendSocketMessage(code, 'bi', 'mysql_comment', content)
}, [setState, sendSocketMessage]);
const mergeObj= (obj1,obj2)=>{
  let obj3 = JSON.parse(JSON.stringify(obj1));
  obj2.table_desc.forEach((item,index)=>{
      let obj3Index = obj3.table_desc.findIndex((item2,index2)=>{
          return item.table_name === item2.table_name
      })
      if(obj3Index !== -1){
        obj3.table_desc[obj3Index].table_comment = item.table_comment
          item.field_desc.forEach((item3,index3)=>{
              let obj3Index2 = obj3.table_desc[obj3Index].field_desc.findIndex((item4,index4)=>{
                  return item3.name === item4.name
              })
              if(obj3Index2 !== -1){
                  obj3.table_desc[obj3Index].field_desc[obj3Index2].comment = item3.comment
                  obj3.table_desc[obj3Index].field_desc[obj3Index2].in_use = item3.in_use
                  obj3.table_desc[obj3Index].field_desc[obj3Index2].is_pass = item3.is_pass
              }else{
                  obj3.table_desc[obj3Index].field_desc.push(item3)
              }
          })
      }else{
          obj3.table_desc.push(item)
      }
  })
  return obj3
}
const handleSuccess =async (tableId,table,isSendTableDateType=null) => {
  // console.log("table",table);
  // console.log("selectTableDescRef.current",selectTableDescRef.current);
  try {
    const mergeTable = mergeObj(selectTableDescRef.current,table);
    // console.log(mergeTable,"mergeTable====")
    const promises = mergeTable.table_desc.map(async (item) => {
      const columns_obj = {
        table_name : item.table_name,
        table_inuse : true,
        table_desc:item.table_comment,
        table_columns_info : {
          field_desc:item.field_desc
        }
      }
      // console.log("columns_obj",columns_obj)
       await axios.post(`/api/data_table/columns/${tableId}/${item.table_name}`,columns_obj);
    });

    Promise.all(promises).then(() => {
      if(isSendTableDateType){
        onUse();
      }
    });

} catch (error) {
  setConfirmLoading(false);
}

};
const handleSocketMessage = useCallback(() => {
  if (!lockReconnect) {
    createWebSocket();
    return
  }
  websocket.onclose = (event) => {
    setState(prevState => ({
      ...prevState,
      messages: prevState.messages.map((message, i) =>
        i === prevState.messages.length - 1 && message.sender === "bot"&& message.Cardloading
          ? { ...message, content: window.W_L.connection_seems_lost, Cardloading: false }
          : message
      ),
      // messages: prevState.messages.filter((item,index)=>item.content!==window.W_L.stopping_generation),
    }));
    setLoadingMask(false);
    setSendTableDate(0);
    setLockReconnect(false);
    errorSetting();
  }

  websocket.onmessage = async (event) => {
    try {
      const data = JSON.parse(event.data);

      if (data.receiver === 'user') {
        setState(prevState => ({
          ...prevState,
          messages: prevState.messages.map((message, i) =>
            i === prevState.messages.length - 1 && message.sender === "bot"
              ? { ...message, content: data.data.content, Cardloading: false,time:moment().format('YYYY-MM-DD HH:mm') }
              : message
          ),
        }));
        setLoadingState(false);
        scrollToBottom();
      }

      if (data.state === 500) {
        if (data.receiver === 'bi') {
          if (data.data.data_type === 'mysql_comment_first') {
            // setState(prevState => ({ ...prevState, sendTableDate: 0 }));
            setSendTableDate(0)
            setLoadingMask(false);
            setLoadingState(false);
          }
        }

        setConfirmLoading(false);
        if (data.receiver === 'log') {
          toast.error(data.data.content);
          return
        }
        errorSetting();
        return;
      }

      if (data.receiver === 'bi') {
        if (data.data.data_type === 'mysql_code') {
          setData_type("mysql_code");
          testAndVerifySql(data.data.content, data.data.name,data.id);
        } else if (data.data.data_type === 'ask_data') {
          setData_type("ask_data");
          dashboardsId("", "ask_data",data.id);
        } else if (data.data.data_type === 'chart_code') {
          setData_type("chart_code")
        try {
          if(Dashboard_id.current){
            saveChart(JSON.parse(data.data.content),"edit",data.id)
          }else{
            saveChart(JSON.parse(data.data.content),null,data.id)
          }

        } catch (error) {
            sendSocketMessage(500,'bi','chart_code',error,data.id)
        }
        } else if (data.data.data_type === 'mysql_comment') {
          setData_type("mysql_comment");
          try {
            const table_desc_list = JSON.parse(JSON.stringify(data.data.content.table_desc));
            const table_desc = await filterTableDesc(table_desc_list);
            if (table_desc.length > 0) {
              toast(window.W_L.please_fill_in_the_description);
              setConfirmLoading(false);
              DialogueContentRef.current.sourceEdit(table_desc);
              handleSuccess(Charttable_id.current,data.data.content);
            } else {
              updateCharttableDate();

              setConfirmLoading(false);
              if(chat_type==="report"){
                sendUrl("new_report");
              }
              handleSuccess(Charttable_id.current,data.data.content,"success");
            }
            setPercent(0)
          } catch (error) {
            errorSetting();
          }
        } else if (data.data.data_type === 'mysql_comment_first') {
          if(chat_type==="autopilot"){
            setState({
              messages: [{ content: data.data.content, sender: "bot", Cardloading: false,time:moment().format('YYYY-MM-DD HH:mm') },{ content: "", sender: "user",time:moment().format('YYYY-MM-DD HH:mm') }],
            });
          }else{
            setState({
              messages: [{ content: data.data.content, sender: "bot", Cardloading: false,time:moment().format('YYYY-MM-DD HH:mm') }],
              // loadingMask: false,
              // sendTableDate: 1,
              data_type: "mysql_comment_first"
            });
          }
          
          setLoadingMask(false);
          setSendTableDate(1);
          setStartUse(true);
          setLoadingState(false);
          toast.success(window.W_L.configuration_completed + " " + chat_type==="autopilot"?"":window.W_L.start_the_dialogue,{
            icon: '👏',
          });
          
        } else if(data.data.data_type === 'mysql_comment_second'){
          setState(prevState => ({
            ...prevState,
            // loadingMask: false,
            // sendTableDate: 1,
          }));
          setLoadingMask(false);
          // setLoadingState(false);
          setSendTableDate(1);
          sendSocketMessage(200, 'user', 'question', state.newInputMessage);
        } else if (data.data.data_type === 'delete_chart') {
          setData_type("delete_chart");
          dashboardsId(data.data.content, "delete",data.id);
        } else if (data.data.data_type === 'table_code') {
          setData_type("table_code");
          if (Dashboard_id.current) {
            publishQuery("edit",data.id);
          } else {
            publishQuery(null,data.id);
          }
        }
      } else if (data.receiver === 'log') {
        if(!data.data.content) return
        if(data.data.data_type === 'data_check'){
          setPercent(data.data.content)
          return
        }
        setState(prevState => ({
          messages: prevState.messages.map((message, i) =>
            i === prevState.messages.length - 1 && message.sender === "bot"
            ? { ...message, logData: [...(message.logData || []), data.data.content] }
              : message
          )
        }));
      }else if(data.receiver === 'python'){
          if(data.data.data_type === 'echart_code'){
            setState(prevState => ({
              messages: prevState.messages.map((message, i) =>
                i === prevState.messages.length - 1 && message.sender === "bot"
                  ? { ...message, chart: data.data.content}
                  : message
              ),
            }));
            scrollToBottom();
          }
      }
      // else if(data.receiver === 'autopilot') {
      //   if(data.data.data_type === 'autopilot_code'){
      //     setState(prevState => ({
      //       messages: prevState.messages.map((message, i) =>
      //         i === prevState.messages.length - 1 && message.sender === "bot"&& message.Cardloading
      //           ? { ...message, autopilot: data.data.content,Cardloading: false,time:moment().format('YYYY-MM-DD HH:mm') }
      //           : message
      //       ),
      //     }));
      //     setLoadingState(false);
      //     scrollToBottom();
      //   }
      // }
    } catch (error) {
      console.log(error, 'socket_error');
    }
  }
}, [state, setState,props]);

// websocket
const openSocket = useCallback(() => {

    createWebSocket();

    handleSocketMessage()
  }, []);
  const closeSetMessage=()=>{
    if(chat_type==="chat" || chat_type==="report"){
      let allMessages = messagesRef.current;
      const lastMessage = messagesRef.current[messagesRef.current.length - 1];
      if (lastMessage && lastMessage.sender === "bot" && lastMessage.Cardloading) {
        allMessages.pop();
      }
      addChatList(allMessages,chat_type);
    }
  }
  useEffect(() => {
    messagesRef.current = state.messages;
  }, [state.messages]);

  useEffect(() => {
    // 将 closeSetMessage 函数封装以便在 beforeunload 事件中使用
    const handleBeforeUnload = () => {
      closeSetMessage();
    };
  
    // 添加 beforeunload 事件监听
    window.addEventListener('beforeunload', handleBeforeUnload);
  
    // 返回一个清理函数，在组件卸载时执行
    return () => {
      // 移除 beforeunload 事件监听
      window.removeEventListener('beforeunload', handleBeforeUnload);
      // 同时，当组件卸载时保存对话记录
      closeSetMessage();
    };
  }, []);

  const onChange = useCallback((type,value=0,item) => {
    openSocket();
    // if (!lockReconnect) {
    //   notification.warning(window.W_L.connection_failed, window.W_L.connection_failed_tip);
    //   openSocket();
    //   return
    // }
   
    // console.log(`selected ${value}`);
    // console.log(`selectedtype ${type}`);
    // Charttable_id.current = value;
    // Charttable_item.current = {
    //   label: item.label,
    //   id: item.id,
    //   type: item.type,
    // };
    // // setSourceType(type);
    // sourceTypeRef.current = type
    // schemaList(value,type);
  }, [setState, handleSocketMessage, openSocket]);

  const saveDashboardId = useCallback((key, value) => {
    if(key==='dashboard_id'){
      Dashboard_id.current = value;
      setDashboardId(value);
      return
    }
    Charttable_id.current = value;
  }, []);

  const isSendTableDate = useCallback((data_type) => {
    handleSocketMessage();
    if (!lockReconnect) {
      toast.error(window.W_L.connection_failed);
      // setState(prevState => ({ ...prevState, sendTableDate: 0 }));
      setSendTableDate(0);
      setLoadingState(false);
      openSocket();
      return;
    }
    if (CharttableD_date.current && CharttableD_date.current.tableName.length > 0) {
      if (SendTableDate === 0) {
        setState(prevState => ({ ...prevState,data_type }));
        setLoadingMask(true);
        setLoadingState(true);
        let promisesList = [];
        const promises = CharttableD_date.current.tableName.map(async (item) => {
          const res = await axios.get(`/api/data_table/columns/${Charttable_id.current}/${item.name}`);
          promisesList.push({
            table_name: res.table_name,
            table_comment: res.table_desc,
            field_desc:filterColumnsByInUse(res.table_columns_info)
          });
        });
        Promise.all(promises).then(() => {
          sendSocketMessage(200, 'bi', data_type, {
            databases_desc: "",
            table_desc: promisesList
          });
        }).catch((err) => {
          console.log(err, 'first_error');
          // setState(prevState => ({ ...prevState, loadingMask: false }));
          setLoadingMask(false);
          setLoadingState(false);
          setSendTableDate(0);
        });
      }
    }
  }, [state, setState, handleSocketMessage, openSocket, sendSocketMessage]);


  const handleSendMessage1 = useCallback(async () => {
    handleSocketMessage();

    const { inputMessage, messages } = state;
    if (inputMessage.trim() === "") {
      return;
    }
    setState(prevState => ({
      ...prevState,
      newInputMessage: inputMessage,
      messages: [...messages, { content: inputMessage, sender: "user",time:moment().format('YYYY-MM-DD HH:mm') }, { content: "", sender: "bot", Cardloading: true }],
      inputMessage: "",
      data_type: "question"
    }));
    setLoadingState(true);
    scrollToBottom();
    if(SendTableDate===0 && messages.length>=1){
      isSendTableDate("mysql_comment_second");
      return
    }
    sendSocketMessage(200, 'user', 'question', inputMessage);
  }, [state, setState, handleSocketMessage, scrollToBottom, sendSocketMessage,isSendTableDate]);

  const handleSendMessage = useCallback(() => {
    const { inputMessage, messages } = state;
     if(stopGeneration){
      return
    }
    if (LoadingState) {
      return
    }
    if (!lockReconnect) {
      toast.error(window.W_L.connection_failed);
      // setState(prevState => ({ ...prevState, sendTableDate: 0 }));
      setSendTableDate(0);
      setLoadingState(false);
      openSocket();
      return
    }
    if(!startUse){
      return
    }
    if (CharttableD_date.current && CharttableD_date.current.tableName.length > 0) {
      handleSendMessage1();
    }
  }, [state, setState, openSocket, handleSendMessage1]);

    const errorSetting = useCallback(() => {
    if(stopGeneration){
      return
    }
    toast.error(window.W_L.connection_failed);
    // setState(prevState => ({ ...prevState, Cardloading: false }));
    setLoadingState(false);
  }, [setState]);
  const onUse = useCallback(() => {
    isSendTableDate("mysql_comment_first");
  }, [isSendTableDate]);



  const setData_type = useCallback((value) => {
    setState(prevState => ({ ...prevState, data_type: value }));
  }, [setState]);



  const successSetting = useCallback(() => {
    if(stopGeneration){
      return
    }
    toast.success(window.W_L.report_generation_completed);
  }, []);

  function filterColumnsByInUse(columnsInfo) {
    return columnsInfo.field_desc.filter(column => column.in_use === 1);
  }

  const filterTableDesc = useCallback((tableDesc) => {
    return tableDesc.filter((item) => {
      return item.field_desc.some((field) => field.is_pass !== 1);
    });
  }, []);

  const scrollToBottom = useCallback(() => {
    if(chat_type==="viewConversation") return
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
    const messageContainer = document.querySelector('.dialogue-content-all');
    if (messageContainer) {
        const scrollHeight = messageContainer.scrollHeight;
      const clientHeight = messageContainer.clientHeight;
      messageContainer.scrollTo({
        top: scrollHeight - clientHeight,
        behavior: 'smooth',
      });
    }
  }, 50);
  }, []);



  const ChangeScrollTop = useCallback(() => {
    setTimeout(() => {
      scrollToBottom()
    }, 100);
  }, []);
  const stopSend = useCallback((type=null) => {
    websocket&&websocket.close();
    setSendTableDate(type==="edit"?1:0);
    setLoadingState(false);
    setStopGeneration(true);
    scrollToBottom();
    openSocket();
  }, [setState, openSocket]);
  const sendDashId = useCallback((id) => {
    sendUrl(id);
    setDialogueStorageDashboardId(id)
  }, []);
  const retry = useCallback((index) => {
  }, [setState, sendSocketMessage]);
  const onOpenKeyClick = useCallback(() => {
    // OpenKeyRef.current.showModal();
  }, [setState, OpenKeyRef]);
  const { new_sql,testAndVerifySql } = useSql(Charttable_id.current,sendSocketMessage,errorSetting);
  const { saveChart,dashboardsId,publishQuery }=useChartCode(sendSocketMessage,saveDashboardId, props, successSetting,CharttableD_date.current,new_sql,dashboardId,sendDashId);
  const { setDialogueStorageDashboardId, addDashboard, getDashboard,addDialogueStorage,getDialogueStorage,addChatList,getAllStorage,addAutopilotStorage}=dialogueStorage();
//   const Dialogue = () => {
    const { messages, inputMessage, newInputMessage } = state;

    return (
      <div className="dialogue-content">
        <DialogueTop loadingMask={LoadingMask} Charttable={CharttableDate} CharttableItem={Charttable_item.current} closeDialogue={closeDialogue} chat_type={chat_type}></DialogueTop>
        {/* <OpenKey ref={OpenKeyRef}></OpenKey> */}
       {LoadingState&& <MenuMask/>}
        <DialogueContent
        databases_type={sourceTypeRef}
        ref={DialogueContentRef}
        Charttable={CharttableDate}
        onUse={onUse}
        sendTableDate={SendTableDate}
        onChange={onChange}
        confirmLoading={ConfirmLoading}
        loadingMask={LoadingMask}
        messages={messages}
        ChangeScrollTop={ChangeScrollTop}
        loadingState={LoadingState}
        stopSend={stopSend}
        inputMessage={inputMessage}
        newInputMessage={newInputMessage}
        setState={setState}
        handleSendMessage={handleSendMessage}
        chat_type={chat_type}
        retry={retry}
        onOpenKeyClick={onOpenKeyClick}
        onSuccess={onSuccess}
        percent={percent}
        sourceTypeRef={sourceTypeRef}
        />
      </div>
    );
  }

  export default Dialogue;
