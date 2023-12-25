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
  const [visible, setVisible] = useState(false);
  const [selectedContent, setSelectedContent] = useState(null);
  const [selectedTemplate, setSelectedTemplate] = useState(1);
  const templates = [{ id: 1, image: dashboards_prettify_1 }];
  const [loading, setLoading] = useState(false);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [previewImage, setPreviewImage] = useState(null);
  const openModal = () => {
    setVisible(true);
  };

  React.useImperativeHandle(ref, () => ({
    openModal,
  }));
  const getDashboardList = async () => {
    const res = await axios.get(`/api/dashboards?order&page=1&page_size=50`);
    if (res.results.length > 0) {
      setSelectedContent(res.results[0].id);
    }
  };
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
      .get(`/api/dashboards/${selectedContent}`)
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

  const handlePreview = (image) => {
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
        postDashboardDetail(detail)
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
  useEffect(() => {
    getDashboardList();
  }, []);
  return (
    <>
      <Toaster />
      <Drawer
        title="仪表盘美化"
        visible={visible}
        onClose={handleCancel}
        closable={!loading}
        footer={
          <div className="drawer-footer">
            <Button className="finish-button" type="primary" onClick={handleOk}>
              完成
            </Button>
          </div>
        }>
        <div className="template-container">
          {templates.map(template => (
            <div key={template.id} className={`template-item`} onClick={() => handleTemplateSelect(template.id)}>
              <img alt="example" src={template.image} className={`template-img ${selectedTemplate === template.id ? 'template-item-selected' : ''}`} />
              <div className="template-overlay">
                <button className="preview-button" onClick={(e) => { e.stopPropagation(); handlePreview(template.image); }}>预览</button>
              </div>
              <Checkbox checked={selectedTemplate === template.id} className="template-checkbox" />
            </div>
          ))}
        </div>
      </Drawer>
      <Modal className="preview-modal" visible={isModalVisible} onCancel={() => setIsModalVisible(false)} footer={null} width='80%'>
        <img alt="example" style={{ width: '100%' }} src={previewImage} />
      </Modal>
    </>
  );
});

export default StepModal;
