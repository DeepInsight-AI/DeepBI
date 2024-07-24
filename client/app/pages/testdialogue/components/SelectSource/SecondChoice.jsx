import React, { useEffect, useState, forwardRef } from "react";
import Input from "antd/lib/input";
import Table from "antd/lib/table";
import ArrowLeftOutlinedIcon from "@ant-design/icons/ArrowLeftOutlined";
import ExclamationCircleOutlinedIcon from "@ant-design/icons/ExclamationCircleOutlined";
import Tooltip from "@/components/Tooltip";
import './index.less';

const SecondChoice = forwardRef(({ SelectLoading, editData, closeEditData, submit, source_item }, ref) => {
  const [SchemaList, setSchemaList] = useState([]);
  const [SchemaListDataItem, setSchemaListDataItem] = useState({});
  const [SecondChoiceLoading, setSecondChoiceLoading] = useState(false);

  const columns = [
    {
      title: window.W_L.field_name,
      dataIndex: 'name',
      key: 'name',
      render: (text, record) => (
        <Tooltip title={text}>
          <span style={{ display: "inline-block", maxWidth: "100px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{text}</span>
        </Tooltip>
      ),
    },
    {
      title: window.W_L.description,
      dataIndex: 'comment',
      key: 'comment',
      render: (text, record) => (
        <Input
          type="text"
          placeholder={window.W_L.field_description}
          value={text}
          style={{ border: "none !important" }}
          onChange={(e) => handleFieldDescriptionChange(e, record)}
        />
      ),
    },
    {
      title: window.W_L.detection,
      dataIndex: 'is_pass',
      key: 'is_pass',
      render: (text, record) => (
        <div>
          {text === 1 ? (
            <span style={{ color: "#52c41a" }}>{window.W_L.pass}</span>
          ) : (
            <span style={{ color: "#f5222d" }}>{window.W_L.fail}</span>
          )}
        </div>
      ),
    }
  ];

  useEffect(() => {
    setSecondChoiceLoading(SelectLoading);
  }, [SelectLoading]);

  useEffect(() => {
    if (editData && editData.length > 0) {
      setSchemaList(editData);
      clickSchemaItem(editData[0]);
    }
  }, [editData]);

  const handleFieldDescriptionChange = (e, record) => {
    const value = e.target.value;
    const newSchemaListDataItem = { ...SchemaListDataItem };
    const newFieldDesc = newSchemaListDataItem.field_desc.map((item) => {
      if (item.name === record.name) {
        item.comment = value;
      }
      return item;
    });
    const newSchemaList = SchemaList.map((item) => {
      if (item.table_name === SchemaListDataItem.table_name) {
        item.field_desc = newFieldDesc;
      }
      return item;
    });
    setSchemaList(newSchemaList);
  };

  const handleTableDescriptionChange = (e) => {
    const value = e.target.value;
    const newSchemaList = SchemaList.map((item) => {
      if (item.table_name === SchemaListDataItem.table_name) {
        item.table_comment = value;
      }
      return item;
    });
    setSchemaList(newSchemaList);
  };

  const clickSchemaItem = (item) => {
    let newFieldDesc = item;
    newFieldDesc.field_desc = item.field_desc.filter((item) => item.is_pass === 0);
    setSchemaListDataItem(newFieldDesc);
  };

  const editSubmit = () => {
    submit(SchemaList);
  };

  const goBack = () => {
    closeEditData();
  };

  const tableIsShow = SchemaListDataItem.field_desc && SchemaListDataItem.field_desc.length > 0;
  const SchemaListIsShow = SchemaList && SchemaList.length > 0;

  return (
    <div>
      <p className="edit-back" onClick={goBack}>
        <ArrowLeftOutlinedIcon style={{ fontSize: "18px" }} />
      </p>
      <div className="select-content-item">
        <div style={{ position: "absolute", bottom: "-30px", fontSize: "12px" }}>
          <Tooltip title={window.W_L.source_tooltip}>
            <ExclamationCircleOutlinedIcon style={{ marginRight: "5px" }} />
          </Tooltip>
          {window.W_L.source_second_tooltip}
        </div>
        <div style={{ display: "flex", flexDirection: "column", alignItems: "baseline" }}>
          <div className="select-main" style={{ margin: "0" }}>
            <ul className={!SchemaListIsShow ? "flex-center" : ""} style={{ height: "288px" }}>
              {SchemaListIsShow && SchemaList.map((item, index) => (
                <li key={index}>
                  <span onClick={() => clickSchemaItem(item)} style={{ color: item.table_name === SchemaListDataItem.table_name ? "#2196F3" : "#333", textAlign: "center", display: "block" }}>{source_item.type === "csv" ? item.table_comment : item.table_name}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
        <div className="table-columns">
          {tableIsShow && (
            <div className="table-columns-item">
              <span>{source_item.type === "csv" ? SchemaListDataItem.table_comment : SchemaListDataItem.table_name}</span>
              <Input
                placeholder={window.W_L.sheet_description}
                style={{ padding: "2px", maxWidth: "30%" }}
                type="text"
                disabled={source_item.type === "csv" ? true : false}
                value={source_item.type === "csv" ? SchemaListDataItem.table_name : SchemaListDataItem.table_comment}
                onChange={handleTableDescriptionChange}
              />
            </div>
          )}
          <div className={!tableIsShow ? "flex-center" : ""} style={{ width: "400px", overflowY: "scroll", height: tableIsShow ? "235px" : "276px" }}>
            {tableIsShow && (
              <Table
                dataSource={SchemaListDataItem.field_desc}
                columns={columns}
                size="small"
                rowKey={(record) => record.name}
                pagination={false}>
              </Table>
            )}
          </div>
        </div>
        <button onClick={editSubmit} disabled={SecondChoiceLoading} className="submit-btn">{window.W_L.submit}</button>
      </div>
    </div>
  );
});

export default SecondChoice;
