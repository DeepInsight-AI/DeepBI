import React, { useState, useEffect, useCallback } from "react";
import Button from "antd/lib/button";
import Form from "antd/lib/form";
import Input from "antd/lib/input";
import Radio from "antd/lib/radio";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import wrapSettingsTab from "@/components/SettingsWrapper";
import routes from "@/services/routes";
import { axios } from "@/services/axios";
import Link from "@/components/Link";
import QuestionCircleOutlinedIcon from "@ant-design/icons/QuestionCircleOutlined";
import { websocket, createWebSocket, closeWebSocket } from "../testdialogue/components/Dialogue/websocket";
import toast from "react-hot-toast";
<<<<<<< HEAD
const SettingsOpenKey = () => {
  const [form] = Form.useForm();
  const [disabled, setDisabled] = useState(false);
  const [aiOption, setAiOption] = useState("DeepInsight");
  const [aiOptions, setAiOptions] = useState({});
  const [requiredFields, setRequiredFields] = useState({});

  const getOpenKey = useCallback(async () => {
    setDisabled(true);
    try {
      const { data } = await axios.get(`/api/ai_token`);

      const response = await fetch("/static/llm.json");
      if (!response.ok) {
        throw new Error("Failed to load local llm.json API key data");
      }
      const JSON_ROOT = await response.json();
      // 合并接口数据和本地数据
      const mergedData = { ...JSON_ROOT };
      const inUse = data && data.in_use ? data.in_use : mergedData.in_use;
      // console.log("JSON_ROOT", JSON_ROOT);
      // console.log("mergedData", mergedData);
      Object.keys(data).forEach(key => {
        if (data[key]) {
          mergedData[key] = { ...mergedData[key], ...data[key] };
        }
      });
      // console.log("mergedData-----", mergedData);
      delete mergedData.in_use;
      setAiOptions(mergedData);
      setRequiredFields(mergedData[inUse].required || []);
      setAiOption(inUse);
      form.setFieldsValue(mergedData[inUse]);
    } catch (error) {
      // console.log("error", error)
      toast.error(error.message);
=======

const SettingsOpenKey = () => {
  const [form] = Form.useForm();
  const [disabled, setDisabled] = useState(false);
  const [aiOption, setAiOption] = useState("DeepInsight"); // 默认选项

  const getOpenKey = useCallback(async () => {
    setDisabled(true);
    const { data } = await axios.get(`/api/ai_token`);
    if (!data.in_use) {
      form.setFieldsValue(data);
    } else {
      setAiOption(data.in_use);
      const { OpenAI = {}, DeepInsight = {}, Azure = {} } = data;
      form.setFieldsValue({
        ApiKey: DeepInsight.ApiKey || "",
        OpenaiApiKey: OpenAI.OpenaiApiKey || "",
        HttpProxyHost: OpenAI.HttpProxyHost || "",
        HttpProxyPort: OpenAI.HttpProxyPort || "",
        ApiHost: OpenAI.ApiHost || "",
        AzureApiKey: Azure.AzureApiKey || "",
        AzureHost: Azure.AzureHost || "",
      });
>>>>>>> main
    }
    createWebSocket();
    setDisabled(false);
  }, [form]);

  useEffect(() => {
    getOpenKey();
  }, [getOpenKey]);
<<<<<<< HEAD

  const handleOpenKey = useCallback(
    async (values,callback) => {
      setDisabled(true);
      try {
        const updatedAiOptions = {
          ...aiOptions,
          [aiOption]: {
            ...aiOptions[aiOption],
            ...values,
          },
        };
        const optionsWithoutRequired = Object.entries(updatedAiOptions).reduce((acc, [key, value]) => {
            const { required, ...rest } = value; // 解构出'required'字段和剩余的字段
            acc[key] = rest; // 只保留剩余的字段
            return acc;
        }, {});
        const response = await axios.post("/api/ai_token", {
          in_use: aiOption,
          ...optionsWithoutRequired,
        });
        if (response.code === 200) {
=======
  const handOpenKey = (values, callback) => {
    const data = {
      in_use: aiOption,
      OpenAI: {
        OpenaiApiKey: form.getFieldValue("OpenaiApiKey") || "",
        HttpProxyHost: form.getFieldValue("HttpProxyHost") || "",
        HttpProxyPort: form.getFieldValue("HttpProxyPort") || "",
        ApiHost: form.getFieldValue("ApiHost") || "",
      },
      DeepInsight: {
        ApiKey: form.getFieldValue("ApiKey") || "",
      },
      Azure: {
        AzureApiKey: form.getFieldValue("AzureApiKey") || "",
        AzureHost: form.getFieldValue("AzureHost") || "",
      },
    };
    axios
      .post("/api/ai_token", data)
      .then(res => {
        if (res.code === 200) {
>>>>>>> main
          if (callback) {
            callback(values);
            return;
          }
          toast.success(window.W_L.save_success);
<<<<<<< HEAD
          getOpenKey();
        } else {
          toast.error(window.W_L.save_failed);
        }
      } catch (error) {
        console.log("error22", error)
        toast.error(window.W_L.save_failed);
      }
      closeWebSocket();
      setDisabled(false);
    },
    [aiOption, aiOptions, getOpenKey]
  );

  const onFinish = values => {
    handleOpenKey(values);
  };

  const handleRadioChange = e => {
    const newAiOption = e.target.value;
    const currentOptionValues = form.getFieldsValue();
    
    // 更新当前AI选项的值
    setAiOptions(prevOptions => ({
      ...prevOptions,
      [aiOption]: {
        ...prevOptions[aiOption],
        ...currentOptionValues,
      },
    }));
  
    // 切换到新的AI选项
    setAiOption(newAiOption);
    setRequiredFields(aiOptions[newAiOption].required || []);
    form.setFieldsValue(aiOptions[newAiOption]);
  };

  const handleMessage = () => {
    try {
      websocket.onmessage = async event => {
        let data = JSON.parse(event.data);
        if (data.receiver === "api") {
          toast.success(data.data.content);
          setDisabled(false);
        }
=======
          closeWebSocket();
          getOpenKey();
        } else {
          toast.error(window.W_L.save_failed);
        }
        setDisabled(false);
      })
      .catch(err => {
        toast.error(window.W_L.save_failed);
        setDisabled(false);
      });
  };
  const onFinish = values => {
    setDisabled(true);
    if (values.HttpProxyPort === undefined) {
      values.HttpProxyPort = "";
    }
    if (values.HttpProxyHost === undefined) {
      values.HttpProxyHost = "";
    }
    if (values.ApiHost === undefined) {
      values.ApiHost = "";
    }
    handOpenKey(values);
  };
  const handleMessage = () => {
    try {
      websocket.onmessage = async event => {
        let data = JSON.parse(event.data);
        if (data.receiver === "api") {
          toast.success(data.data.content);
          setDisabled(false);
        }
>>>>>>> main
      };
    } catch (error) {
      setDisabled(false);
    }
  };
  const connectTest = values => {
    if (!websocket) {
      createWebSocket();
      return;
    }
    handleMessage();
<<<<<<< HEAD

    setDisabled(true);
    let sendInfo = {
      state: 200,
      receiver: "sender",
      chat_type: "test",
      data: {
        data_type: "apikey",
        content: values,
        language_mode: window.W_L.language_mode,
      },
    };
    websocket.send(JSON.stringify(sendInfo));
  };

  const handleConnectTestClick = () => {
    form.validateFields().then(values => {
      handleOpenKey(values, connectTest);
    });
  };

  const renderFormItems = () => {
    const currentOption = aiOptions[aiOption] || {};
    const requiredKeys = currentOption.required || [];
    return Object.keys(currentOption)
      .filter(key => key !== "required")
      .map(key => {
        return (
          <Form.Item
            key={key}
            name={key}
            label={key}
            rules={[{ required: requiredKeys.includes(key), message: `${window.W_L.please_enter}${key}` }]}>
            <Input placeholder={key} />
          </Form.Item>
        );
      });
  };

  return (
    <React.Fragment>
      <div className="row1" style={{ width: "50%", margin: "auto" }}>
        <Form form={form} layout="vertical" disabled={disabled} onFinish={onFinish}>
          <Form.Item>
            <div style={{ display: "flex", alignItems: "center" }}>
              <h4 style={{ marginRight: "30px" }}>AI:</h4>
              <Radio.Group onChange={handleRadioChange} value={aiOption}>
                {Object.keys(aiOptions).filter(key => key !== "in_use").map(option => (
                  <Radio key={option} value={option}>
                    {option}
                  </Radio>
                ))}
              </Radio.Group>
            </div>
          </Form.Item>
          {renderFormItems()}
          <Form.Item style={{ textAlign: "right" }}>
            <div style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div style={{ display: "flex", alignItems: "center" }}>
                <QuestionCircleOutlinedIcon style={{ marginRight: "3px", color: "#2196f3" }} />
                <Link href="https://holmes.bukeshiguang.com/" rel="noopener noreferrer" target="_blank">
                  {window.W_L.click_here_to_get_apikey}
                </Link>
              </div>
=======

    setDisabled(true);
    let sendInfo = {
      state: 200,
      receiver: "sender",
      chat_type: "test",
      data: {
        data_type: "apikey",
        content: values,
        language_mode: window.W_L.language_mode,
      },
    };
    websocket.send(JSON.stringify(sendInfo));
  };
  const handleConnectTestClick = () => {
    form.validateFields().then(values => {
      handOpenKey(values, connectTest);
    });
  };
  const handleRadioChange = e => {
    setAiOption(e.target.value);
  };
  return (
    <React.Fragment>
      <div className="row1" style={{ width: "50%", margin: "auto" }}>
        {/* {! && <LoadingState className="" />} */}
        <Form form={form} layout="vertical" disabled={disabled} onFinish={onFinish}>
          <Form.Item>
            <div style={{ display: "flex", alignItems: "center" }}>
              <h4 style={{ marginRight: "30px" }}>AI:</h4>
              <Radio.Group onChange={handleRadioChange} value={aiOption}>
                <Radio value="DeepInsight">DeepInsight</Radio>
                <Radio value="OpenAI">OpenAI</Radio>
                <Radio value="Azure">Azure</Radio>
              </Radio.Group>
            </div>
          </Form.Item>
          {aiOption === "DeepInsight" && (
            <Form.Item
              name="ApiKey"
              label="ApiKey"
              rules={[{ required: true, message: window.W_L.please_enter_api_key }]}>
              <Input placeholder="ApiKey" />
            </Form.Item>
          )}

          {aiOption === "OpenAI" && (
            <>
              <Form.Item
                name="OpenaiApiKey"
                label="OpenaiApiKey"
                rules={[{ required: true, message: window.W_L.please_enter_api_key }]}>
                <Input placeholder="OpenaiApiKey" />
              </Form.Item>
              <Form.Item name="HttpProxyHost" label="HttpProxyHost">
                <Input placeholder="HttpProxyHost" />
              </Form.Item>
              <Form.Item name="HttpProxyPort" label="HttpProxyPort">
                <Input placeholder="HttpProxyPort" />
              </Form.Item>
              <Form.Item name="ApiHost" label="ApiHost">
                <Input placeholder="ApiHost" />
              </Form.Item>
            </>
          )}

          {aiOption === "Azure" && (
            <>
              <Form.Item
                name="AzureApiKey"
                label="AzureApiKey"
                rules={[{ required: true, message: window.W_L.please_enter_api_key }]}>
                <Input placeholder="AzureApiKey" />
              </Form.Item>
              <Form.Item
                name="AzureHost"
                label="AzureHost"
                rules={[{ required: true, message: window.W_L.please_enter_api_host }]}>
                <Input placeholder="AzureHost" />
              </Form.Item>
            </>
          )}
          <Form.Item style={{ textAlign: "right" }}>
            <div style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              {aiOption === "OpenAI" ? (
                <></>
              ) : (
                <div style={{ display: "flex", alignItems: "center" }}>
                  <QuestionCircleOutlinedIcon style={{ marginRight: "3px", color: "#2196f3" }} />
                  <Link href="https://holmes.bukeshiguang.com/" rel="noopener noreferrer" target="_blank">
                    {window.W_L.click_here_to_get_apikey}
                  </Link>
                </div>
              )}
>>>>>>> main
              <div>
                <Button loading={disabled} style={{ marginRight: "10px" }} onClick={() => handleConnectTestClick()}>
                  {window.W_L.connect_test}
                </Button>
                <Button loading={disabled} htmlType="submit" type="primary">
                  {window.W_L.apply}
                </Button>
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
