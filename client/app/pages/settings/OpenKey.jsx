import React,{useState,useEffect,useCallback} from 'react';
import Button from "antd/lib/button";
import Form from "antd/lib/form";
import Input from "antd/lib/input";
import Radio from "antd/lib/radio";
// import * as XLSX from 'xlsx';
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
// import LoadingState from "@/components/items-list/components/LoadingState";
import wrapSettingsTab from "@/components/SettingsWrapper";
import routes from "@/services/routes";
import { axios } from "@/services/axios";
import Link from "@/components/Link";
import QuestionCircleOutlinedIcon from "@ant-design/icons/QuestionCircleOutlined";
import { websocket,createWebSocket,closeWebSocket } from '../testdialogue/components/Dialogue/websocket';
import toast, { Toaster } from 'react-hot-toast';

const SettingsOpenKey = () => {
    const [form] = Form.useForm();
  const [disabled, setDisabled] = useState(false);
  const [aiOption, setAiOption] = useState('DeepInsight'); // 默认选项

  const getOpenKey = useCallback(async () => {
    setDisabled(true);
    const {data} = await axios.get(`/api/ai_token`);
      if(!data.in_use){
        form.setFieldsValue(data);
      }else{
        setAiOption(data.in_use);
        const {OpenAI, DeepInsight} = data;
        form.setFieldsValue({
        ApiKey: DeepInsight.ApiKey|| "",
        OpenaiApiKey: OpenAI.OpenaiApiKey|| "",
        HttpProxyHost: OpenAI.HttpProxyHost|| "",
        HttpProxyPort: OpenAI.HttpProxyPort|| "",
        ApiHost: OpenAI.ApiHost|| "",
    });
      }
    createWebSocket()
    setDisabled(false)
  }, [form]);

  useEffect(() => {
    getOpenKey();
  }, [getOpenKey]);
  const handOpenKey = ()=>{
    const data ={
      in_use: aiOption,
      OpenAI: {
        OpenaiApiKey: form.getFieldValue("OpenaiApiKey") || "",
        HttpProxyHost:  form.getFieldValue("HttpProxyHost") || "",
        HttpProxyPort: form.getFieldValue("HttpProxyPort") || "",
        ApiHost: form.getFieldValue("ApiHost") || "",
      },
      DeepInsight: {
        ApiKey: form.getFieldValue("ApiKey") || "",
      }
      
    }
    axios.post("/api/ai_token",data).then((res) => {
      if(res.code===200){
        toast.success(window.W_L.save_success)
        closeWebSocket()
        getOpenKey();
      }else{
        toast.error(window.W_L.save_failed)
        
      }
      setDisabled(false)
    }).catch((err) => {
      toast.error(window.W_L.save_failed)
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
    if(values.ApiHost === undefined){
      values.ApiHost = '';
    }
    handOpenKey()
  };
  const handleMessage=()=>{
    try {
      websocket.onmessage = async (event) => {
        let data = JSON.parse(event.data)
        if (data.receiver === 'api') {
          toast.success(data.data.content)
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
       <Toaster />
       <Form
      form={form}
      layout="vertical"
      disabled={disabled}
      onFinish={onFinish}
    >
      <Form.Item>
      <div style={{display:"flex",alignItems:"center"}}>
      <h4 style={{marginRight:"30px"}}>AI:</h4>
            <Radio.Group onChange={handleRadioChange} value={aiOption}>
           
              <Radio value="DeepInsight">DeepInsight</Radio>
              <Radio value="OpenAI">OpenAI</Radio>
            </Radio.Group>
      </div>
          </Form.Item>
          {aiOption === 'DeepInsight' && (
            <Form.Item name="ApiKey" label="ApiKey" rules={[{ required: true, message: 'Please enter API key' }]}>
              <Input placeholder="ApiKey" />
            </Form.Item>
          )}

{aiOption === 'OpenAI' && (
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
      <Form.Item
      name="ApiHost"
        label="ApiHost">
        <Input placeholder="ApiHost" />
      </Form.Item>
            </>
          )}
     
      <Form.Item style={{textAlign: "right"}}>
     <div style={{width:"100%",display:"flex",alignItems: "center",justifyContent: "space-between"}}>
     <div style={{display:"flex",alignItems: "center"}}>
     <QuestionCircleOutlinedIcon style={{marginRight:"3px",color: "#2196f3"}} />
     <Link
             href="https://holmes.bukeshiguang.com/"
             rel="noopener noreferrer"
             target="_blank">
          {window.W_L.click_here_to_get_apikey}
      </Link>
     </div>
    <div>
    <Button disabled={disabled} style={{marginRight:"10px"}}
      onClick={() => connectTest()}>{window.W_L.connect_test}</Button>
      <Button disabled={disabled} htmlType="submit" type="primary">{window.W_L.apply}</Button>
    </div>
     </div>
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
