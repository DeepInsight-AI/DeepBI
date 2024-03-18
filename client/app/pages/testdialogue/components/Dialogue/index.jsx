import React, { useState, useEffect, useRef, useCallback } from "react";
import { axios } from "@/services/axios";
import { websocket, createWebSocket, lockReconnect, setLockReconnect, setStopGeneration, stopGeneration } from './websocket.js';
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
import { API_CHAT } from './const';
import { currentUser } from "@/services/auth";
import DialogueContext from '../../context/DialogueContext.js';
import { generateChart } from './generateChart';
const Dialogue = (props) => {

  const { chat_type, sendUrl, uuid } = props
  const OpenKeyRef = useRef(); // 打开key
  const DialogueContentRef = useRef(); // 对话内容
  const new_logData = useRef([]); // 新的logData
  const messagesRef = useRef(); // 对话记录
  const Charttable_id = useRef(null); // Charttable_id
  const Charttable_item = useRef({}); // Charttable_item
  const Dashboard_id = useRef(null); // Dashboard_id
  const CharttableD_date = useRef(null); // CharttableD_date
  const [dashboardId, setDashboardId] = useState(null); // dashboardId
  const [CharttableDate, setCharttableDate] = useState(null);
  const [LoadingState, setLoadingState] = useState(false);
  const [startUse, setStartUse] = useState(false); // 初始化状态
  const [SendTableDate, setSendTableDate] = useState(0); // 发送表格状态
  const [LoadingMask, setLoadingMask] = useState(false); // 加载状态
  const [selectTableName, setSelectTableName] = useState([]);
  const [selectTableDesc, setSelectTableDesc] = useState({}); // 
  const selectTableDescRef = useRef(); // 保存selectTableDesc
  const selectTableNameRef = useRef(); // 保存selectTableName
  const [ConfirmLoading, setConfirmLoading] = useState(false); // 确认加载
  const sourceTypeRef = useRef("mysql"); // 数据库类型
  const [percent, setPercent] = useState(0); // 进度条
  const [cachedTableDesc, setCachedTableDesc] = useState(null); // 添加一个状态来缓存数据
  const MAX_QUESTIONS = chat_type === "chat" ? 5 : 1; // 假设最大问题数为5
  const abortControllersRef = useRef([]); // 使用ref来跟踪所有的AbortController实例
  let timeoutId = null;
  const [inputMessage, setInputMessage] = useState("");// 输入的消息
  const wsRef = useRef(null);
  const [state, setState] = useState({
    messages: [], // 对话记录
    lockReconnect: false,
    data_type: null, // 数据类型
    logData: [], // logData
  });


  useEffect(() => {
    if (state.logData) {
      new_logData.current = state.logData;
    }
  }, [state.logData])

  // 取历史对话记录
  useEffect(() => {
    getDialogueDashboardStorage();
  }, []);
  useEffect(() => {
    if (uuid) {
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




  // 获取历史对话记录
  const getDialogueDashboardStorage = (type = null) => {
    // || chat_type=="autopilot"
    if (chat_type === "chat" || chat_type === "report" || chat_type === "autopilot") {
      let res = [];
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
      if (res && res.length > 0) {
        setCharttableDate(res[0].table_name);
        saveDashboardId("", res[0].Charttable_id);
        sourceTypeRef.current = res[0].type;
        Charttable_item.current = {
          label: res[0].label,
          id: res[0].id,
          type: res[0].type,
        };
        if (res[0].dashboardId) {
          sendUrl(res[0].dashboardId);
          saveDashboardId("dashboard_id", res[0].dashboardId);
        } else {
          if (chat_type === "report") {
            sendUrl("new_report");
          }
          saveDashboardId("dashboard_id", null);
        }

        if (res[0].messages && res[0].messages.length > 0) {
          setState(prevState => ({
            ...prevState,
            messages: res[0].messages
          }));
          setLoadingMask(false);
          setStartUse(true);
          stopSend('edit');
          setSendTableDate(0);
        } else {
          onUse();
        }
      } else {
        if (chat_type === "report") {
          sendUrl("");
        }
        setCharttableDate(null);
        saveDashboardId(null, null);
        setState(prevState => ({
          ...prevState,
          messages: []
        }));
        setLoadingMask(false);
        setSendTableDate(0);
        setStartUse(false);
        Charttable_item.current = {};
      }
    } else if (chat_type === "viewConversation") {
      const res = getAllStorage();
      if (res && res.length > 0 && uuid) {
        const currentList = res.filter(item => item.uuid === uuid);
        setCharttableDate(currentList[0].table_name);
        saveDashboardId("", currentList[0].Charttable_id);
        Charttable_item.current = {
          label: currentList[0].label,
          id: currentList[0].id,
          type: currentList[0].type,
        };
        if (currentList[0].messages && currentList[0].messages.length > 0) {
          setState(prevState => ({
            ...prevState,
            messages: currentList[0].messages
          }));
          setLoadingMask(false);
          setSendTableDate(1);
          setStartUse(true);
        }
      }
    }
    scrollToBottom();
  };
  // setStorage
  const setDialogueDashboardStorage = () => {
    let existingDialogueStorage = []
    let Chart_Dashboard = {
      table_name: selectTableNameRef.current,
      Charttable_id: Charttable_id.current,
      ...Charttable_item.current,
    }
    Chart_Dashboard.title = window.W_L.new_dialogue + String(getAllStorage().length + 1);
    Chart_Dashboard.uuid = Date.now();
    Chart_Dashboard.messages = [];
    existingDialogueStorage.push(Chart_Dashboard)
    if (chat_type === "report") {
      addDashboard(existingDialogueStorage);
    } else if (chat_type === "chat") {
      addDialogueStorage(existingDialogueStorage);
    } else if (chat_type === "autopilot") {
      addAutopilotStorage(existingDialogueStorage);
    }
  }

  // clearStorage
  const closeDialogue = () => {
    closeSetMessage();
    if (chat_type === "report") {
      addDashboard([])
    } else if (chat_type === "chat") {
      addDialogueStorage([])
    } else if (chat_type === "autopilot") {
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

  const onSuccess = useCallback(async (code, value, source_item, result, firstTableData) => {
    // if (!lockReconnect) {
    //   toast.error(window.W_L.connection_seems_lost);
    //   setConfirmLoading(false);
    //   openSocket();
    //   return
    // }
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

    if (firstTableData) {
      setSelectTableName(result)
    }

    setSelectTableDesc({ table_desc: value })
    const allIsPass = value.map(item => {
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
    await sendSocketMessage(code, 'bi', 'mysql_comment', content)
  }, [setState, sendSocketMessage]);

  // 表数据合并
  const mergeObj = (obj1, obj2) => {
    let obj3 = JSON.parse(JSON.stringify(obj1));
    obj2.table_desc.forEach((item, index) => {
      let obj3Index = obj3.table_desc.findIndex((item2, index2) => {
        return item.table_name === item2.table_name
      })
      if (obj3Index !== -1) {
        obj3.table_desc[obj3Index].table_comment = item.table_comment
        item.field_desc.forEach((item3, index3) => {
          let obj3Index2 = obj3.table_desc[obj3Index].field_desc.findIndex((item4, index4) => {
            return item3.name === item4.name
          })
          if (obj3Index2 !== -1) {
            obj3.table_desc[obj3Index].field_desc[obj3Index2].comment = item3.comment
            obj3.table_desc[obj3Index].field_desc[obj3Index2].in_use = item3.in_use
            obj3.table_desc[obj3Index].field_desc[obj3Index2].is_pass = item3.is_pass
          } else {
            obj3.table_desc[obj3Index].field_desc.push(item3)
          }
        })
      } else {
        obj3.table_desc.push(item)
      }
    })
    return obj3
  }

  // 提交成功处理
  const handleSuccess = async (tableId, table, isSendTableDateType = null) => {
    // console.log("table",table);
    // console.log("selectTableDescRef.current",selectTableDescRef.current);
    try {
      const mergeTable = mergeObj(selectTableDescRef.current, table);
      // console.log(mergeTable,"mergeTable====")
      const promises = mergeTable.table_desc.map(async (item) => {
        const columns_obj = {
          table_name: item.table_name,
          table_inuse: true,
          table_desc: item.table_comment,
          table_columns_info: {
            field_desc: item.field_desc
          }
        }
        // console.log("columns_obj",columns_obj)
        await axios.post(`/api/data_table/columns/${tableId}/${item.table_name}`, columns_obj);
      });

      Promise.all(promises).then(() => {
        if (isSendTableDateType) {
          onUse();
        }
      });

    } catch (error) {
      setConfirmLoading(false);
    }

  };

  // 返回结果处理
  const handleSocketMessage = useCallback(async (event) => {
    //   if(chat_type === "report"){
    //   websocket.onclose = (event) => {
    //     setState(prevState => ({
    //       ...prevState,
    //       messages: prevState.messages.map((message, i) =>
    //         i === prevState.messages.length - 1 && message.sender === "bot"&& message.Cardloading
    //           ? { ...message, content: window.W_L.connection_seems_lost, Cardloading: false }
    //           : message
    //       ),
    //       // messages: prevState.messages.filter((item,index)=>item.content!==window.W_L.stopping_generation),
    //     }));
    //     setLoadingMask(false);
    //     setSendTableDate(0);
    //     setLockReconnect(false);
    //     errorSetting();
    //   }

    //   try {
    //     websocket.onmessage = (event) => {
    //       const data = JSON.parse(event.data);
    //     // const data = event;

    //     // 调用输出消息
    //     outPutMessage(data);
    //   }


    //   } catch (error) {
    //     console.log(error, 'socket_error');
    //     setLoadingMask(false);
    //     setLoadingState(false);
    //   }

    // }
    try {
      const data = event;

      // 调用输出消息
      outPutMessage(data);


    } catch (error) {
      console.log(error, 'socket_error');
      setLoadingMask(false);
      setLoadingState(false);
    }

  }, [state, setState, props]);

  // 返回处理
  const outPutMessage = async (data) => {
    if (data.receiver === 'user') {
      setState(prevState => ({
        ...prevState,
        messages: prevState.messages.map((message, i) =>
          (chat_type === "chat" && message.sender === "bot" && message.chat_id === data.chat_id) ||
            (chat_type !== "chat" && i === prevState.messages.length - 1 && message.sender === "bot")
            ? { ...message, content: data.data.content, Cardloading: false, time: moment().format('YYYY-MM-DD HH:mm') }
            : message
        ),
      }));
      setLoadingState(false);
      scrollToBottom(true);
      closeWS();
    }

    if (data.state === 500) {
      closeWS();
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
      // errorSetting();

      return;
    }

    if (data.receiver === 'bi') {
      if (data.data.data_type === 'mysql_code') {
        setData_type("mysql_code");
        testAndVerifySql(data.data.content, data.data.name, data.id);
      } else if (data.data.data_type === 'ask_data') {
        setData_type("ask_data");
        dashboardsId("", "ask_data", data.id);
      } else if (data.data.data_type === 'chart_code') {
        setData_type("chart_code")
        try {
          if (Dashboard_id.current) {
            saveChart(JSON.parse(data.data.content), "edit", data.id)
          } else {
            saveChart(JSON.parse(data.data.content), null, data.id)
          }

        } catch (error) {
          await sendSocketMessage(500, 'bi', 'chart_code', error, data.id)
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
            handleSuccess(Charttable_id.current, data.data.content);
          } else {
            updateCharttableDate();

            setConfirmLoading(false);
            if (chat_type === "report") {
              sendUrl("new_report");
            }
            handleSuccess(Charttable_id.current, data.data.content, "success");
          }
          setPercent(0)
        } catch (error) {
          // errorSetting();
        }
      } else if (data.data.data_type === 'mysql_comment_first') {
        if (chat_type === "autopilot") {
          setState({
            messages: [{ content: data.data.content, sender: "bot", Cardloading: false, time: moment().format('YYYY-MM-DD HH:mm') }, { content: "", sender: "user", time: moment().format('YYYY-MM-DD HH:mm') }],
          });
        } else {
          setState({
            messages: [{ content: data.data.content, sender: "bot", Cardloading: false, chat_id: data.chat_id, time: moment().format('YYYY-MM-DD HH:mm') }],
            // loadingMask: false,
            // sendTableDate: 1,
            data_type: "mysql_comment_first"
          });
        }

        setLoadingMask(false);
        setSendTableDate(1);
        setStartUse(true);
        setLoadingState(false);
        toast.success(window.W_L.configuration_completed + " " + chat_type === "autopilot" ? "" : window.W_L.start_the_dialogue, {
          icon: '👏',
        });

      } else if (data.data.data_type === 'mysql_comment_second') {
        setState(prevState => ({
          ...prevState,
          loadingMask: false,
          sendTableDate: 1,
        }));
        setLoadingMask(false);
        setLoadingState(false);
        setSendTableDate(1);
        await sendSocketMessage(200, 'user', 'question', inputMessage);
      } else if (data.data.data_type === 'delete_chart') {
        setData_type("delete_chart");
        dashboardsId(data.data.content, "delete", data.id);
      } else if (data.data.data_type === 'table_code') {
        setData_type("table_code");
        if (Dashboard_id.current) {
          publishQuery("edit", data.id);
        } else {
          publishQuery(null, data.id);
        }
      }
    } else if (data.receiver === 'log') {
      if (!data.data.content) return
      if (data.data.data_type === 'data_check') {
        setPercent(data.data.content)
        return
      }
      setState(prevState => ({
        messages: prevState.messages.map((message, i) =>
          (chat_type === "chat" && message.sender === "bot" && message.chat_id === data.chat_id) ||
            (chat_type !== "chat" && i === prevState.messages.length - 1 && message.sender === "bot")
            ? { ...message, logData: [...(message.logData || []), data.data.content] }
            : message
        )
      }));
    } else if (data.receiver === 'python') {
      if (data.data.data_type === 'echart_code') {
        setState(prevState => ({
          messages: prevState.messages.map((message, i) =>
            (chat_type === "chat" && message.sender === "bot" && message.chat_id === data.chat_id) ||
              (chat_type !== "chat" && i === prevState.messages.length - 1 && message.sender === "bot")
              ? { ...message, chart: data.data.content }
              : message
          ),
        }));
        // scrollToBottom();
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
  };
  // 关闭对话框时保存对话记录
  const closeSetMessage = () => {
    if (chat_type === "chat" || chat_type === "report") {
      let allMessages = messagesRef.current;
      const lastMessage = messagesRef.current[messagesRef.current.length - 1];
      if (lastMessage && lastMessage.sender === "bot" && lastMessage.Cardloading) {
        allMessages.pop();
      }
      addChatList(allMessages, chat_type);
    }

    cancelRequestAll();
  }
  // 取消所有请求
  const cancelRequestAll = () => {
    closeWS();
    if (abortControllersRef.current.length === 0) {
      return;
    }
    try {
      abortControllersRef.current.forEach(abortController => {
        abortController.abort();
      }
      );
      setState(prevState => ({
        ...prevState,
        messages: prevState.messages.map(message =>
          message.sender === "bot" && message.Cardloading
            ? { ...message, Cardloading: false }
            : message
        )
      }));
    } catch (error) {
      console.log(error, 'cancelRequestAll_error');
    }
  };

  // 保存对话记录
  useEffect(() => {
    messagesRef.current = state.messages;
  }, [state.messages]);

  // 在组件卸载时保存对话记录
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


  const onChange = useCallback((type, value = 0, item) => {
    // openSocket();
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
  }, [setState, handleSocketMessage]);


  const saveDashboardId = useCallback((key, value) => {
    if (key === 'dashboard_id') {
      Dashboard_id.current = value;
      setDashboardId(value);
      return
    }
    Charttable_id.current = value;
  }, []);


  // 获取数据库字段信息
  const isSendTableDate = useCallback(async (data_type) => {
    let baseMessageContent = {}; // 初始化一个空对象来构建base_message内容

    if (data_type === "mysql_comment_first") {
      setState(prevState => ({ ...prevState, data_type }));
      setLoadingMask(true);
      setLoadingState(true);
    }
    // 检查是否已经缓存了数据，如果已缓存，则直接使用缓存的数据
    if (cachedTableDesc) {
      baseMessageContent = {
        databases_desc: "",
        table_desc: cachedTableDesc
      };
    } else if (CharttableD_date.current && CharttableD_date.current.tableName.length > 0 && SendTableDate === 0) {
      setState(prevState => ({ ...prevState, data_type }));
      const promises = CharttableD_date.current.tableName.map(async (item) => {
        const res = await axios.get(`/api/data_table/columns/${Charttable_id.current}/${item.name}`);
        return {
          table_name: res.table_name,
          table_comment: res.table_desc,
          field_desc: filterColumnsByInUse(res.table_columns_info)
        };
      });

      try {
        const results = await Promise.all(promises);
        baseMessageContent = {
          databases_desc: "",
          table_desc: results
        };
        setCachedTableDesc(results); // 缓存请求的结果
      } catch (err) {
        console.log(err, 'first_error');
        setLoadingMask(false);
        setLoadingState(false);
      }
    }

    return baseMessageContent;
  }, [state, setState, handleSocketMessage, sendSocketMessage]);

  // 发送对话消息1
  const handleSendMessage1 = useCallback(async () => {
    const { messages } = state;
    if (inputMessage.trim() === "") {
      return;
    }
    // 判断问题数是否超过最大问题数
    const questionCount = messages.filter(message => message.sender === "bot" && message.Cardloading).length;
    console.log("questionCount==", questionCount)
    if (questionCount >= MAX_QUESTIONS) {
      toast.error("问题数超过最大问题数");
      return;
    }

    const chat_id = moment().valueOf();
    console.log("当前对话标识==", chat_id)
    // 创建一个新的AbortController实例并保存其引用
    const abortController = new window.AbortController();
    abortControllersRef.current.push(abortController);
    setState(prevState => ({
      ...prevState,
      messages: [...messages, { content: inputMessage, sender: "user", chat_id, time: moment().format('YYYY-MM-DD HH:mm') }, { content: "", sender: "bot", Cardloading: true, chat_id, abortController }],
      data_type: "question"
    }));
    setInputMessage("");
    setLoadingState(true);
    scrollToBottom();

    const baseMessageContent = await isSendTableDate("mysql_comment_second");
    await sendSocketMessage(200, 'user', 'question', inputMessage, 0, baseMessageContent, chat_id, abortController.signal);
    abortControllersRef.current = abortControllersRef.current.filter(ac => ac !== abortController);
  }, [state, setState, inputMessage, setInputMessage, scrollToBottom, sendSocketMessage, isSendTableDate]);

  // 发送对话消息0
  const handleSendMessage = useCallback(() => {
    if (!startUse) {
      return
    }
    if (CharttableD_date.current && CharttableD_date.current.tableName.length > 0) {
      handleSendMessage1();
    }
  }, [state, setState, handleSendMessage1]);

  // error消息
  const errorSetting = useCallback(() => {
    toast.error(window.W_L.connection_failed);
    setLoadingState(false);
  }, [setState]);

  // 初始化对话 发送first
  const onUse = useCallback(async () => {
    const data_type = "mysql_comment_first"
    const baseMessageContent = await isSendTableDate(data_type);
    await sendSocketMessage(200, 'bi', data_type, baseMessageContent);
  }, [isSendTableDate]);

  // set data_type
  const setData_type = useCallback((value) => {
    setState(prevState => ({ ...prevState, data_type: value }));
  }, [setState]);


  // 成功消息
  const successSetting = useCallback(() => {
    // if(stopGeneration){
    //   return
    // }
    toast.success(window.W_L.report_generation_completed);
  }, []);

  // 过滤表中使用字段
  function filterColumnsByInUse(columnsInfo) {
    return columnsInfo.field_desc.filter(column => column.in_use === 1);
  }

  // 过滤表中使用字段
  const filterTableDesc = useCallback((tableDesc) => {
    return tableDesc.filter((item) => {
      return item.field_desc.some((field) => field.is_pass !== 1);
    });
  }, []);

  // 滚动到底部
  const scrollToBottom = useCallback((isHas = false) => {
    if (chat_type === "viewConversation" || isHas) return
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

  // 停止对话
  const stopSend = useCallback((type = null) => {
    // websocket&&websocket.close();
    setSendTableDate(type === "edit" ? 1 : 0);
    setLoadingState(false);
    // setStopGeneration(true);
    closeWS();
  }, [setState]);

  const closeWS = () => {
    if (wsRef.current && chat_type === "report") {
      wsRef.current.close();
      setState(prevState => ({
        ...prevState,
        messages: prevState.messages.map((message, i) =>
          i === prevState.messages.length - 1 && message.sender === "bot"
            ? { ...message, Cardloading: false }
            : message
        ),
      }));
    }
  }

  // 添加一个函数来取消特定的对话请求
  const cancelRequest = useCallback((message) => {
    console.log(message, "message====")
    if (chat_type === "report") {
      closeWS();
      return
    }
    // 找到message与chat_id关联的内容的bot
    if (message && message.abortController) {
      console.log(message.abortController, "message.abortController====")
      try {
        message.abortController.abort();
        // 从ref中移除已取消的AbortController实例
        abortControllersRef.current = abortControllersRef.current.filter(ac => ac !== message.abortController);
        setState(prevState => ({
          ...prevState,
          messages: prevState.messages.map((item, index) => {
            if (item.chat_id === message.chat_id) {
              return {
                ...item,
                Cardloading: false
              }
            } else {
              return item
            }
          })
        }));
      } catch (error) {
        console.error(error, 'cancelRequest_error');
      }
    }
  }, []);


  // 设置报表
  const sendDashId = useCallback((id) => {
    sendUrl(id);
    setDialogueStorageDashboardId(id)
  }, []);

  // 重试
  const retry = useCallback((index) => {
  }, [setState, sendSocketMessage]);

  // 打开key
  const onOpenKeyClick = useCallback(() => {
    // OpenKeyRef.current.showModal();
  }, [setState, OpenKeyRef]);


  // const sendSocketMessage = useCallback((state, sender, data_type, content,id=0) => {

  // }, [state]);

  // const sendSocketMessage =  useCallback( async (state, sender, data_type, content,id=0) => {
  //   // const messageData = {
  //   //   "message": "你好",
  //   //   "user_id": currentUser.id,
  //   //   "user_name": currentUser.name,
  //   //   "chat_id": new Date().getTime()
  //   // };
  //   const messageData = {
  //     user_id: currentUser.id,
  //     user_name: currentUser.name,
  //     message:{
  //       state,
  //       database:sourceTypeRef.current,
  //       sender,
  //       chat_type,
  //       data: {
  //         data_type,
  //         databases_id:Charttable_id.current || 0,
  //         language_mode:window.W_L.language_mode,
  //         content,
  //       },
  //       id
  //     }
  //   }
  //   try {
  //     const response = await fetch(API_CHAT, {
  //       method: 'POST',
  //       headers: {
  //         'Access-Control-Allow-Origin': '*',
  //         'Content-Type': 'application/json',
  //       },
  //       body: JSON.stringify(messageData),
  //     });

  //     // 检查浏览器是否支持ReadableStream
  //     if (response.body && response.body.getReader) {
  //       const reader = response.body.getReader();
  //       const decoder = new TextDecoder();

  //       // 读取数据
  //       reader.read().then(function processText({ done, value }) {
  //         if (done) {
  //           console.log("Stream complete");
  //           return;
  //         }

  //         // 解码并处理接收到的数据
  //         const chunk = decoder.decode(value);
  //         console.log('Received chunk:', chunk);
  //         setState(prevState => ({
  //           ...prevState,
  //           testFetchMessage: prevState.testFetchMessage + chunk,
  //         }));
  //         console.log('new testFetchMessage:', state.testFetchMessage);
  //         // 假设服务器发送的是JSON字符串，尝试解析并更新状态
  //         try {
  //           const data = JSON.parse(chunk);
  //           console.log('Parsed JSON:', data)
  //           handleSocketMessage(data);
  //           // 更新状态或UI
  //           // setState(prevState => ({
  //           //   ...prevState,
  //           //   messages: [...prevState.messages, { content: data.message, sender: "bot" }],
  //           // }));
  //         } catch (error) {
  //           console.error('Error parsing JSON:', error);
  //         }

  //         // 递归调用以读取下一个数据块
  //         reader.read().then(processText);
  //       });
  //     } else {
  //       console.log('Streaming not supported');
  //     }
  //   } catch (error) {
  //     console.error('Fetch error:', error);
  //   }
  // }, [state]);


  // fetch请求 
  const sendSocketMessage = useCallback(async (state, sender, data_type, content, id = 0, base_message = null, chat_id = 0, signal = null) => {

    if (chat_type === "report" && data_type !== "mysql_comment_first" && data_type !== "mysql_comment") {
      const messgaeInfo = {
        state,
        database: sourceTypeRef.current,
        sender,
        chat_type,
        base_message,
        data: {
          data_type,
          databases_id: Charttable_id.current || 0,
          language_mode: window.W_L.language_mode,
          content,
        },
        id
      }
      generateChart(
        wsRef,
        messgaeInfo,
        // on Change
        (res) => {
          handleSocketMessage(res);
        },
        // on Cancel
        () => {
          setState(prevState => ({
            ...prevState,
            messages: prevState.messages.map((message, i) =>
              i === prevState.messages.length - 1 && message.sender === "bot" && message.Cardloading
                ? { ...message, content: window.W_L.connection_seems_lost, Cardloading: false }
                : message
            ),
            // messages: prevState.messages.filter((item,index)=>item.content!==window.W_L.stopping_generation),
          }));
          setLoadingMask(false);
          setSendTableDate(0);
          setConfirmLoading(false);
          // errorSetting();
        }
      );

      return
    }

    // 
    const messageData = {
      user_id: currentUser.id,
      user_name: currentUser.name,
      chat_id,
      message: {
        state,
        database: sourceTypeRef.current,
        sender,
        chat_type,
        base_message,
        data: {
          data_type,
          databases_id: Charttable_id.current || 0,
          language_mode: window.W_L.language_mode,
          content,
        },
        id
      }
    };

    try {
      const response = await fetch(API_CHAT, {
        method: 'POST',
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(messageData),
        signal
      });

      if (response.body && response.body.getReader) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        // 递归函数处理流数据
        const processText = async ({ done, value }) => {
          if (done) {
            console.log("Stream complete");
            return;
          }

          // 累积数据块
          buffer += decoder.decode(value, { stream: true });
          // 分割完整消息
          let parts = buffer.split('---ENDOFMESSAGE---');
          console.log('Received buffer:', buffer);
          console.log('Received parts:', parts);
          buffer = parts.pop(); // 保留未完成的部分

          parts.forEach(part => {
            try {

              const data = JSON.parse(part);
              console.log('Parsed JSON:', data)
              handleSocketMessage(data); // 处理解析后的消息
            } catch (error) {
              console.error('Error parsing JSON:', error);
            }
          });

          // 继续读取下一个数据块
          reader.read().then(processText);
        };

        // 开始读取数据
        reader.read().then(processText);
      } else {
        console.log('Streaming not supported');
      }
    } catch (error) {
      console.error('Fetch error:', error);
      setLoadingMask(false);
      setLoadingState(false);
      setConfirmLoading(false);
      // toast.error(window.W_L.ERROR_MESSAGE);
    }
  }, [state, isSendTableDate]);

  const { new_sql, testAndVerifySql } = useSql(Charttable_id.current, sendSocketMessage, errorSetting);
  const { saveChart, dashboardsId, publishQuery } = useChartCode(sendSocketMessage, saveDashboardId, props, successSetting, CharttableD_date.current, new_sql, dashboardId, sendDashId);
  const { setDialogueStorageDashboardId, addDashboard, getDashboard, addDialogueStorage, getDialogueStorage, addChatList, getAllStorage, addAutopilotStorage } = dialogueStorage();
  //   const Dialogue = () => {
  const { messages } = state;

  return (
    <DialogueContext.Provider value={{ cancelRequest }}>
      <div className="dialogue-content">
        <DialogueTop loadingMask={LoadingMask} Charttable={CharttableDate} CharttableItem={Charttable_item.current} closeDialogue={closeDialogue} chat_type={chat_type}></DialogueTop>
        {/* <OpenKey ref={OpenKeyRef}></OpenKey> */}
        {/* {LoadingState && <MenuMask />} */}
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
          setInputMessage={setInputMessage}
          handleSendMessage={handleSendMessage}
          chat_type={chat_type}
          retry={retry}
          onOpenKeyClick={onOpenKeyClick}
          onSuccess={onSuccess}
          percent={percent}
          sourceTypeRef={sourceTypeRef}
        />
      </div>
    </DialogueContext.Provider>
  );
}

export default Dialogue;
