import React, { useState, useEffect } from "react";
import routes from "@/services/routes";
import Router from "./Router";
import handleNavigationIntent from "./handleNavigationIntent";
import ErrorMessage from "./ErrorMessage";
import ConfigProvider from 'antd/lib/config-provider';
import zh_CN from 'antd/lib/locale-provider/zh_CN';
import moment from 'moment';
import 'moment/locale/zh-cn';
import { Toaster } from 'react-hot-toast';
export default function ApplicationArea() {
  const [currentRoute, setCurrentRoute] = useState(null);
  const [unhandledError, setUnhandledError] = useState(null);

  useEffect(() => {
    if (currentRoute && currentRoute.title) {
      document.title = currentRoute.title;
    }
  }, [currentRoute]);

  useEffect(() => {
    function globalErrorHandler(event) {
      event.preventDefault();
      setUnhandledError(event.error);
    }

    document.body.addEventListener("click", handleNavigationIntent, false);
    window.addEventListener("error", globalErrorHandler, false);

    return () => {
      document.body.removeEventListener("click", handleNavigationIntent, false);
      window.removeEventListener("error", globalErrorHandler, false);
    };
  }, []);

  if (unhandledError) {
    return <ErrorMessage error={unhandledError} />;
  }

  // moment.locale('zh-cn')
  window.W_L.language_mode==="CN"?moment.locale('zh-cn'):moment.locale('en')
  return (
    <ConfigProvider locale={window.W_L.language_mode==="CN"?zh_CN:""}>
       <Toaster />
      <Router routes={routes.items} onRouteChange={setCurrentRoute} />
    </ConfigProvider>
  );
}
