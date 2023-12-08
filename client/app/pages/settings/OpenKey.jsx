import React,{useState,useEffect,useCallback} from 'react';
import Button from "antd/lib/button";
import Form from "antd/lib/form";
import Input from "antd/lib/input";
import Radio from "antd/lib/radio";
// import * as XLSX from 'xlsx';
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
// import LoadingState from "@/components/items-list/components/LoadingState";
import wrapSettingsTab from "@/components/SettingsWrapper";
import notification from "@/services/notification";
import routes from "@/services/routes";
import { axios } from "@/services/axios";
import { websocket,createWebSocket } from '../testdialogue/components/Dialogue/websocket';
const SettingsOpenKey = () => {
    const [form] = Form.useForm();
  const [disabled, setDisabled] = useState(false);
  const [aiOption, setAiOption] = useState('Holmes'); // 默认选项

  const getOpenKey = useCallback(async () => {
    setDisabled(true);
    const {data} = await axios.get(`/api/ai_token`);
    form.setFieldsValue(data);
    createWebSocket()
    setDisabled(false)
  }, [form]);

  useEffect(() => {
    getOpenKey();
  }, [getOpenKey]);
  const handOpenKey = (info)=>{
    console.log(info,"info===")
    return
    axios.post("/api/ai_token",info).then((res) => {
        setDisabled(false)
        notification.success(window.W_L.save_success)
        getOpenKey();
    }).catch((err) => {
        notification.error(window.W_L.save_failed)
      setDisabled(false)
    })

  }
  const onFinish = (values) => {
    setDisabled(true)
    if (values.HttpProxyPort === undefined) {
      values.HttpProxyPort = '';
    }
    if (values.HttpProxyHost === undefined) {
      values.HttpProxyHost = '';
    }
    handOpenKey(values)
  };
  const handleMessage=()=>{
    try {
      websocket.onmessage = async (event) => {
        let data = JSON.parse(event.data)
        if (data.receiver === 'api') {
          notification.info(data.data.content)
          setDisabled(false)
        }
      }
    } catch (error) {
      setDisabled(false)
    }
  }
  const connectTest=()=>{
    
    if(!websocket){
      createWebSocket()
      return
    }
    handleMessage();
    
    form.validateFields().then((values) => {
      setDisabled(true)
      let sendInfo={
        state:200,
        receiver:"sender",
        chat_type:"test",
        data:{
          data_type:"apikey",
          content:values,
          language_mode:window.W_L.language_mode,
        }
    }
    console.log(sendInfo,"sendInfo")
    return
    websocket.send(JSON.stringify(sendInfo))
  })
    
  }
  const handleRadioChange = e => {
    setAiOption(e.target.value);
  };
  return (
   
     <React.Fragment>
     <div className="row1" style={{width:"50%",margin:"auto"}}>
       {/* {! && <LoadingState className="" />} */}
     
       <Form
      form={form}
      layout="vertical"
      disabled={disabled}
      onFinish={onFinish}
    >
      <Form.Item>
      <h3 style={{marginRight:"30px"}}>AI:</h3>
            <Radio.Group onChange={handleRadioChange} value={aiOption}  style={{ display: 'flex', flexDirection: 'row' }}>
           
              <Radio value="Holmes">Holmes Key</Radio>
              <Radio value="Openai">OpenAI Key</Radio>
            </Radio.Group>
          </Form.Item>
          {aiOption === 'Holmes' && (
            <Form.Item name="ApiKey" label="ApiKey" rules={[{ required: true, message: 'Please enter API key' }]}>
              <Input placeholder="ApiKey" />
            </Form.Item>
          )}

{aiOption === 'openaikey' && (
            <>
             <Form.Item name="OpenaiApiKey" label="OpenaiApiKey"  rules={[{ required: true, message: window.W_L.please_enter_api_key }]}>
        <Input placeholder="OpenaiApiKey" />
      </Form.Item>
      <Form.Item
      name="HttpProxyHost"
        label="HttpProxyHost">
        <Input placeholder="HttpProxyHost" />
      </Form.Item>
      <Form.Item
      name="HttpProxyPort"
        label="HttpProxyPort">
        <Input placeholder="HttpProxyPort" />
      </Form.Item>
            </>
          )}
     
      <Form.Item style={{textAlign: "right"}}>
      <Button disabled={disabled} style={{marginRight:"10px"}}
      onClick={() => connectTest()}>{window.W_L.connect_test}</Button>
      <Button disabled={disabled} htmlType="submit" type="primary">{window.W_L.submit}</Button>
      </Form.Item>
    </Form>
     </div>
   </React.Fragment>
  );
};

export default SettingsOpenKey;

const SettingsOpenKeyPage = wrapSettingsTab(
  "Settings.OpenKey",
  {
    title: "API Key",
    path: "settings/OpenKey",
    order: 9,
  },
  SettingsOpenKey
);

routes.register(
  "Settings.OpenKey",
  routeWithUserSession({
    path: "/settings/OpenKey",
    title: "API Key",
    render: pageProps => <SettingsOpenKeyPage {...pageProps} />,
  })
);
