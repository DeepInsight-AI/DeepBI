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
import { createWebSocket, closeWebSocket } from "../testdialogue/components/Dialogue/websocket";
import toast from "react-hot-toast";
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

      const response = await fetch("/static/apiKey/apikey.json");
      if (!response.ok) {
        throw new Error("Failed to load local API key data");
      }
      const JSON_ROOT = await response.json();
      // 合并接口数据和本地数据
      const mergedData = { ...JSON_ROOT };
      // del in_use
      delete mergedData.in_use;
      console.log("JSON_ROOT", JSON_ROOT);
      console.log("mergedData", mergedData);
      Object.keys(data).forEach(key => {
        if (data[key]) {
          mergedData[key] = { ...mergedData[key], ...data[key] };
        }
      });
      console.log("mergedData-----", mergedData);
      setAiOptions(mergedData);
      setRequiredFields(mergedData[data.in_use].required || []);
      setAiOption(data.in_use);
      form.setFieldsValue(mergedData[data.in_use]);
    } catch (error) {
      console.log("error", error)
      toast.error(window.W_L.fail);
    }
    createWebSocket();
    setDisabled(false);
  }, [form]);

  useEffect(() => {
    getOpenKey();
  }, [getOpenKey]);

  const handleOpenKey = useCallback(
    async values => {
      setDisabled(true);
      try {
        const updatedAiOptions = {
          ...aiOptions,
          [aiOption]: {
            ...aiOptions[aiOption],
            ...values,
          },
        };
        const response = await axios.post("/api/ai_token", {
          in_use: aiOption,
          ...updatedAiOptions,
        });
        if (response.code === 200) {
          toast.success(window.W_L.save_success);
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
              <div>
                <Button loading={disabled} style={{ marginRight: "10px" }} onClick={() => form.submit()}>
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
