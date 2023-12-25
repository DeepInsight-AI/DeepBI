import React, { useState, useEffect } from "react";
import Drawer from "antd/lib/drawer";
import Checkbox from "antd/lib/checkbox";
import Modal from "antd/lib/modal";
import Button from "antd/lib/button";
import { axios } from "@/services/axios";
import toast, { Toaster } from "react-hot-toast";
import dashboards_prettify_1 from "../../../../assets/images/dashboard-example/dashboards_prettify_1.jpg";
import "./StepModal.css";
const StepModal = React.forwardRef((props, ref) => {
  const {dashboardId} = props;
  const [visible, setVisible] = useState(false);
  const [selectedTemplate, setSelectedTemplate] = useState(1);
  const templates = [
    { id: 1, title: window.W_L.example1, image: dashboards_prettify_1 },
    { id: 2, title: window.W_L.example2, image: dashboards_prettify_1 },
    { id: 999, title: window.W_L.more, image: dashboards_prettify_1 },
  ];
  const [loading, setLoading] = useState(false);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [previewImage, setPreviewImage] = useState(null);
  const openModal = () => {
    setVisible(true);
  };

  React.useImperativeHandle(ref, () => ({
    openModal,
  }));
  const seriesTypeMapping = {
    CHART: widget =>
      widget.visualization.options.globalSeriesType === "column"
        ? "bar"
        : widget.visualization.options.globalSeriesType,
    TABLE: () => "table",
  };

  const globalSeriesType = widget => seriesTypeMapping[widget.visualization.type](widget);

  const getQueryResult = async widgets => {
    const promises = widgets.map(async widget => {
      const res = await axios.get(
        `/api/queries/${widget.visualization.query.id}/results/${widget.visualization.query.latest_query_data_id}.json`
      );
      return {
        id: widget.id,
        chart_name: widget.visualization.query.name,
        chart_type: globalSeriesType(widget),
        data: res.query_result.data,
        columnMapping: widget.visualization.options.columnMapping,
      };
    });
    return Promise.all(promises);
  };

  const getDashboardDetail = () => {
    return axios
      .get(`/api/dashboards/${dashboardId}`)
      .then(async res => {
        if (res.widgets && res.widgets.length > 0) {
          const queryResult = await getQueryResult(res.widgets);
          return {
            dashboard_name: res.name,
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

  const handlePreview = image => {
    setPreviewImage(image);
    setIsModalVisible(true);
  };

  const postDashboardDetail = detail => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        console.log("Posting detail:", detail);
        resolve("Post success");
      }, 2000);
    });
  };

  const handleOk = () => {
    setLoading(true);
    toast.promise(getDashboardDetail(), {
      loading: "Loading...",
      success: detail => {
        return postDashboardDetail(detail)
          .then(() => {
            setLoading(false);
            return window.W_L.submit_success;
          })
          .catch(err => {
            setLoading(false);
            console.error(err);
            return window.W_L.submit_fail;
          });
      },
      error: err => {
        setLoading(false);
        console.error(err);
        return window.W_L.submit_fail;
      },
    });
  };

  const handleCancel = () => {
    setVisible(false);
  };

  const handleTemplateSelect = id => {
    setSelectedTemplate(id);
  };
  return (
    <>
      <Toaster />
      <Drawer
        title={window.W_L.prettify_dashboard}
        visible={visible}
        onClose={handleCancel}
        closable={false}
        footer={
          <div className="drawer-footer">
            <Button loading={loading} className="finish-button" type="primary" onClick={handleOk}>
            {window.W_L.apply}
            </Button>
            <Button loading={loading} className="cancel-button" onClick={handleCancel}>
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
                      handlePreview(template.image);
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
