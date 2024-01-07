import React, { useState,useEffect } from "react";
import Modal from "antd/lib/modal";
import Steps from "antd/lib/steps";
import Select from "antd/lib/select";
import Checkbox from "antd/lib/checkbox";
import Button from "antd/lib/button";
import Form from "antd/lib/form";
import { axios } from "@/services/axios";
import toast, { Toaster } from 'react-hot-toast';
import dashboards_prettify_1 from "../../../../assets/images/dashboard-example/dashboards_prettify_1.jpg";
import "./StepModal.css";
import { get } from "lodash";
const { Step } = Steps;
const { Option } = Select;
const StepModal =React.forwardRef((props, ref)  => {
  const [visible, setVisible] = useState(false);
  const [current, setCurrent] = useState(0);
  const [selectedContent, setSelectedContent] = useState(null);
  const [selectedTemplate, setSelectedTemplate] = useState(1);
  const [selectedDashboard, setSelectedDashboard] = useState(null);
  const templates = [{ id: 1, image: dashboards_prettify_1 }];
  const [loading, setLoading] = useState(false);
  const openModal = () => {
    setVisible(true);
  };

  React.useImperativeHandle(ref, () => ({
    openModal
  }));
  const getDashboardList = (async () => {
    const res = await axios.get(`/api/dashboards?order&page=1&page_size=50`);
    if(res.results.length>0){
      setSelectedContent(res.results[0].id);
      setSelectedDashboard(res.results);
    }else{
      setSelectedDashboard([]);
    }
  });
  const seriesTypeMapping = {
    'CHART': (widget) => widget.visualization.options.globalSeriesType === 'column' ? 'bar' : widget.visualization.options.globalSeriesType,
    'TABLE': () => 'table'
  };
  
  const globalSeriesType = (widget) => seriesTypeMapping[widget.visualization.type](widget);
  
  const getQueryResult = async (widgets) => {
    const promises = widgets.map(async (widget) => {
      const res = await axios.get(`/api/queries/${widget.visualization.query.id}/results/${widget.visualization.query.latest_query_data_id}.json`);
      return {
        id: widget.id,
        chart_name: widget.visualization.query.name,
        chart_type: globalSeriesType(widget),
        data: res.query_result.data,
        columnMapping: widget.visualization.options.columnMapping
      };
    });
    return Promise.all(promises);
  }
  
  const getDashboardDetail = () => {
    return axios.get(`/api/dashboards/${selectedContent}`)
      .then(async res => {
        if(res.widgets && res.widgets.length > 0){
          const queryResult = await getQueryResult(res.widgets);
          return {
            dashboard_name: res.name,
            query_result: queryResult
          };
        } else {
          throw new Error(window.W_L.no_dashboard_data);
        }
      })
      .catch(error => {
        console.error(error);
        throw error;
      });
  }


  const postDashboardDetail = (detail) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        console.log('Posting detail:', detail);
        resolve('Post success');
      }, 2000);
    });
  };


 const handleOk = () => {
  setLoading(true);
  toast.promise(getDashboardDetail(), {
    loading: 'Loading...',
    success: (detail) => {
      postDashboardDetail(detail)
        .then(() => {
          setLoading(false);
          return window.W_L.submit_success;
        })
        .catch((err) => {
          setLoading(false);
          console.error(err);
          return window.W_L.submit_fail;
        });
    },
    error: (err) => {
      setLoading(false);
      console.error(err);
      return window.W_L.submit_fail;
    },
  });
};

  const handleCancel = () => {
    setVisible(false);
  };

  const next = () => {
    setCurrent(current + 1);
  };

  const prev = () => {
    setCurrent(current - 1);
  };

  const handleTemplateSelect = id => {
    setSelectedTemplate(id);
  };

  const steps = [
    {
      title: "仪表盘选择",
      content: (
        <div className="content-container">
          <Form.Item label="仪表盘" className="content-select">
            <Select
              value={selectedContent}
              onChange={value => setSelectedContent(value)}>
              {selectedDashboard && selectedDashboard.map(dashboard => (
                <Option key={dashboard.id} value={dashboard.id}>
                  {dashboard.name}
                </Option>
              ))}
            </Select>
          </Form.Item>
        </div>
      ),
    },
    {
      title: "选择模板",
      content: (
        <div className="template-container">
          {templates.map(template => (
            <div key={template.id} className="template-item" onClick={() => handleTemplateSelect(template.id)}>
              <img alt="example" src={template.image} className="template-img" />
              <Checkbox checked={selectedTemplate === template.id} className="template-checkbox" />
            </div>
          ))}
        </div>
      ),
    },
  ];
  useEffect(() => {
    getDashboardList();
  }, []);
  return (
    <>
     <Toaster />
      <Modal
        title="仪表盘美化"
        visible={visible}
        onCancel={handleCancel}
        confirmLoading={loading}
        closable={!loading}
        footer={
          current === 0 ? (
            <Button type="primary" onClick={next} disabled={!selectedContent}>
              下一步
            </Button>
          ) : (
            <>
              <Button style={{ marginRight: 8 }} onClick={prev}>
                上一步
              </Button>
              <Button type="primary" onClick={handleOk}>
                完成
              </Button>
            </>
          )
        }>
        <Steps current={current}>
          {steps.map(item => (
            <Step key={item.title} title={item.title} />
          ))}
        </Steps>
        <div className="steps-content">{steps[current].content}</div>
      </Modal>
    </>
  );
});

export default StepModal;
