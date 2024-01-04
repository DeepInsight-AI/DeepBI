import React, { useState, useEffect } from "react";
import Drawer from "antd/lib/drawer";
import Checkbox from "antd/lib/checkbox";
import Modal from "antd/lib/modal";
import Button from "antd/lib/button";
import { axios } from "@/services/axios";
import toast from "react-hot-toast";
import dashboards_prettify_1 from "../../../../assets/images/dashboard-example/dashboards_prettify_1.jpg";
import dashboards_prettify_2 from "../../../../assets/images/dashboard-example/dashboards_prettify_2.jpg";
import dashboards_prettify_3 from "../../../../assets/images/dashboard-example/dashboards_prettify_3.jpg";
import "./StepModal.css";
const StepModal = React.forwardRef((props, ref) => {
  const {dashboardId} = props;
  const [visible, setVisible] = useState(false);
  const [selectedTemplate, setSelectedTemplate] = useState(1);
  const templates = [
    { id: 1, title: window.W_L.example1, image: dashboards_prettify_1 },
    { id: 2, title: window.W_L.example2, image: dashboards_prettify_2 },
    { id: 3, title: window.W_L.example3, image: dashboards_prettify_3 },
    { id: 999, title: window.W_L.more, image: dashboards_prettify_1 },
  ];
  const [loading, setLoading] = useState(false);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [previewImage, setPreviewImage] = useState(null);
  const openModal = () => {
    setVisible(true);
  };
  const getDashboardsPrettifyList = async () => {
    await axios.get("/api/pretty_dashboard");
};
  React.useImperativeHandle(ref, () => ({
    openModal,
  }));
  const globalSeriesType = widget => {
    if (widget.visualization.type === "TABLE") {
      return "table";
    }
    const type = widget.visualization.options.globalSeriesType;
    if (["column", "line", "pie", "area"].includes(type)) {
      return type === "column" ? "bar" : type;
    }
    return false
  };
  const getQueryResult = async widgets => {
    const promises = widgets.map(async widget => {
      const chartType = globalSeriesType(widget);
      if (chartType) {
        const res = await axios.get(
          `/api/queries/${widget.visualization.query.id}/results/${widget.visualization.query.latest_query_data_id}.json`
        );
        return {
          id: widget.visualization.query.id,
          latest_query_data_id: widget.visualization.query.latest_query_data_id,
          chart_name: widget.visualization.query.name,
          chart_type: chartType,
          data: res.query_result.data,
          columnMapping: widget.visualization.options.columnMapping,
        };
      }
    });
    // return Promise.all(promises.filter(Boolean));
    const results = await Promise.all(promises);
    return results.filter(Boolean);
  };

  const getDashboardDetail = () => {
    return axios
      .get(`/api/dashboards/${dashboardId}`)
      .then(async res => {
        if (res.widgets && res.widgets.length > 0) {
          const queryResult = await getQueryResult(res.widgets);
          return {
            dashboard_name: res.name,
            dashboard_id : res.id,
            query_result: queryResult,
          };
        } else {
          throw new Error(window.W_L.no_dashboard_data);
        }
      })
      .catch(error => {
        console.error(error);
        throw error;
      });
  };

  const handlePreview = template => {
    if(template.id === 999) return;
    setPreviewImage(template.image);
    setIsModalVisible(true);
  };

  const postDashboardDetail =async detail => {
    const data={
      ...detail,
      template_id: selectedTemplate,
    }
    const res = await axios.post("/api/pretty_dashboard", data);
    return res;
  };

  const handleOk = async () => {
    setLoading(true);
    try {
      const detail = await getDashboardDetail();
      const res = await postDashboardDetail(detail);
      if(res && res.code === 200){
        toast.success(window.W_L.submit_success);
        getDashboardsPrettifyList();
      }else{
        toast.error(res.data || window.W_L.submit_fail);
      }
      setLoading(false);
      
    } catch (err) {
      setLoading(false);
      console.error(err);
      toast.error(window.W_L.submit_fail);
    }
  };
  const handleCancel = () => {
    if (loading) {
      return;
    }
    setVisible(false);
  };

  const handleTemplateSelect = id => {
    setSelectedTemplate(id);
  };
  return (
    <>
      <Drawer
        title={window.W_L.prettify_dashboard}
        visible={visible}
        onClose={handleCancel}
        closable={false}
        mask={false}
        footer={
          <div className="drawer-footer">
            <Button loading={loading} className="finish-button" type="primary" onClick={handleOk}>
            {window.W_L.apply}
            </Button>
            <Button className="cancel-button" onClick={handleCancel}>
            {window.W_L.close}
            </Button>
          </div>
        }>
        <div className="template-container">
          {templates.map(template => (
            <div key={template.id} className={`template-item`} onClick={() => handleTemplateSelect(template.id)}>
              <p>{template.title}</p>
              <div className="template-div">
              <img
                alt="example"
                src={template.image}
                className={`template-img ${selectedTemplate === template.id ? "template-item-selected" : ""}`}>
              </img>
              <div className={`template-overlay ${template.id === 999 ? "always-show" : ""}`}>
                  <button
                    className={`preview-button ${template.id === 999 ? "coming-soon-button" : ""}`}
                    onClick={e => {
                      e.stopPropagation();
                      handlePreview(template);
                    }}>
                    {template.id === 999 ? window.W_L.coming_soon : window.W_L.preview}
                  </button>
                </div>
                </div>
              {template.id !== 999 && (
                <Checkbox checked={selectedTemplate === template.id} className="template-checkbox" />
              )}
            </div>
          ))}
        </div>
      </Drawer>
      <Modal
        className="preview-modal"
        visible={isModalVisible}
        onCancel={() => setIsModalVisible(false)}
        footer={null}
        closable={false}
        width="80%">
        <img alt="example" style={{ width: "100%" }} src={previewImage} />
      </Modal>
    </>
  );
});

export default StepModal;
