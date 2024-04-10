import React, { useEffect, useState, forwardRef, useImperativeHandle } from "react";
import Select from "antd/lib/select";
import Input from "antd/lib/input";
import Table from "antd/lib/table";
import { axios } from "@/services/axios";
import Spin from "antd/lib/spin";
import Checkbox from "antd/lib/checkbox";
import Space from "antd/lib/space";
import Progress from "antd/lib/progress";
// import pg from "@/assets/images/db-logos/pg.png";
// import mysql from "@/assets/images/db-logos/mysql.png";
// import excel from "@/assets/images/db-logos/excel.png";
// import starrocks from "@/assets/images/db-logos/starrocks.png";
import InboxOutlinedIcon from "@ant-design/icons/InboxOutlined";
import QuestionCircleOutlinedIcon from "@ant-design/icons/QuestionCircleOutlined";
import InfoCircleOutlinedIcon from "@ant-design/icons/InfoCircleOutlined";
import Link from "@/components/Link";
import Tooltip from "@/components/Tooltip";
import SecondChoice from "./SecondChoice";
import { IMG_ROOT } from "@/services/data-source";
import "./index.less";
import { T } from "antd/lib/upload/utils";
const SelectSource = forwardRef(({ confirmLoading, Charttable, chat_type, onChange, onSuccess, percent }, ref) => {
  const [options, setOptions] = useState([]);
  const [source_item, setSourceItem] = useState({ type: "mysql" });
  const [source_id, setSource_id] = useState();
  const [SchemaList, setSchemaList] = useState([]);
  const [SchemaListData, setSchemaListData] = useState([]);
  const [SchemaListDataItem, setSchemaListDataItem] = useState({});
  const [selectSchema, setSelectSchema] = useState([]);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [tableSelectedRowKeys, setTableSelectedRowKeys] = useState([]);
  const [loadingTableColumns, setLoadingTableColumns] = useState(false);
  const [indeterminate, setIndeterminate] = useState(false); // all checkbox
  const [checkAll, setCheckAll] = useState(false); // all checkbox
  const [SelectLoading, setSelectLoading] = useState(true);
  const [editData, setEditData] = useState([]); //editData
  const [submitting, setSubmitting] = useState(false);
  const columns = [
    {
      title: window.W_L.field_name,
      dataIndex: "name",
      key: "name",
      render: (text, record) => (
        <Tooltip title={record.name}>
          <span className="field-name">{record.name}</span>
        </Tooltip>
      ),
    },
    {
      title: window.W_L.description,
      dataIndex: "comment",
      key: "comment",
      render: (text, record) => (
        <Input
          type="text"
          value={text}
          style={{ border: "none !important" }}
          placeholder={window.W_L.field_description}
          onChange={e => handleFieldDescriptionChange(e, record)}
        />
      ),
    },
  ];

  useEffect(() => {
    const getMySql = async () => {
      try {
        const res = await axios.get("/api/data_sources");
        const options = res.map(d => ({
          ...d,
          value: d.id,
          label: d.name,
          type: d.type,
        }));
        if (chat_type !== "report") {
          options.unshift({ label: window.W_L.excel_upload, value: 0, type: "csv", id: 0 });
        }

        setOptions(options);
      } catch (err) {
        console.error("error", err);
      }
    };
    getMySql();
  }, [chat_type]);
  useEffect(() => {
    setSelectLoading(confirmLoading);
    if (!confirmLoading) {
      setSubmitting(false);
    }
  }, [confirmLoading]);
  const filterOption = (input, option) => (option?.label ?? "").toLowerCase().includes(input.toLowerCase());

  const editTableData = data => {
    setEditData(data);
  };

  const handleChange = value => {
    resetStates();
    const selectedItem = options.find(item => item.value === value);
    const type = selectedItem ? selectedItem.type : null;
    onChange(type, value, selectedItem);
    setSourceItem(selectedItem);
    setSource_id(value);
    schemaList(value, type);
  };

  const resetStates = () => {
    setSourceItem({ type: "" });
    setSource_id("");
    setSchemaList([]);
    setSchemaListData([]);
    setSchemaListDataItem({});
    setSelectSchema([]);
    setSelectedRowKeys([]);
    setTableSelectedRowKeys([]);
    setIndeterminate(false);
    setCheckAll(false);
  };
  const getSchemaList = (val, max = 50) => {
    return new Promise((resolve, reject) => {
      let num = 0;
      const timer = setInterval(async () => {
        num++;
        if (num > max) {
          clearInterval(timer);
          reject([]);
        }
        try {
          const res = await axios.get(`/api/data_sources/${val}/schema`);
          if (res.schema) {
            clearInterval(timer);
            resolve(res.schema);
          }
        } catch (err) {
          clearInterval(timer);
          reject(err);
        }
      }, 200);
    });
  };
  const schemaList = async (val, type) => {
    try {
      let optionsList;
      const getOptionsList = (data, type) => {
        return data.map((item, index) => ({
          ...item,
          label: type === "csv" ? item.source_name : item.name,
          value: index,
          name: type === "csv" ? item.file_name : item.name,
          checked: false,
        }));
      };

      if (type === "csv") {
        const { data } = await axios.get(`api/upload`);
        optionsList = getOptionsList(data, type);
      } else {
        const schemaList = await getSchemaList(val);
        optionsList = getOptionsList(schemaList, type);
      }

      setSchemaList(optionsList);
    } catch (error) {
      console.error("error", error);
    }
  };

  const getTableColumns = async (item, type = null) => {
    setLoadingTableColumns(true);
    const table_desc_obj = {
      table_name: "",
      table_comment: "",
      field_desc: [],
    };
    const res = await axios.get(
      `/api/data_table/columns/${source_id}/${source_item.type === "csv" ? item.file_name : item.name}`
    );
    if (Object.keys(res).length !== 0) {
      table_desc_obj.table_name = res.table_name;
      table_desc_obj.table_comment = res.table_desc || "";
      table_desc_obj.field_desc = res.table_columns_info.field_desc;
    } else {
      table_desc_obj.table_name = source_item.type === "csv" ? item.file_name : item.name;
      table_desc_obj.table_comment = source_item.type === "csv" ? item.source_name : "";
      if (source_item.type !== "csv") {
        item.columns.forEach((i, index) => {
          const field_desc_obj = {
            name: source_item.type === "pg" ? i.name : i,
            comment: item.comment[index] || "",
            in_use: 1,
          };
          table_desc_obj.field_desc.push(field_desc_obj);
        });
      }
    }

    const existingTable = SchemaListData.find(table => table.table_name === table_desc_obj.table_name);
    if (!existingTable) {
      setSchemaListData(prevData => [...prevData, table_desc_obj]);
    }
    if (!type) {
      setSchemaListDataItem(existingTable || table_desc_obj);
    }

    // const existingTableSelectedRowKeys = tableSelectedRowKeys.find(
    //   table => table.table_name === table_desc_obj.table_name
    // );
    // if (existingTableSelectedRowKeys) {
    //   console.log("existingTableSelectedRowKeys===111", existingTableSelectedRowKeys);
    //   setSelectedRowKeys(existingTableSelectedRowKeys.selectedRowKeys);
    // } else {
    //   const newSelectedRowKeys = table_desc_obj.field_desc.map(field => field.name);
    //   if (!type) {
    //     setSelectedRowKeys(table_desc_obj.field_desc.filter(field => field.in_use === 1).map(field => field.name));
    //   }
    //   setTableSelectedRowKeys(prevData => [
    //     ...prevData,
    //     { table_name: table_desc_obj.table_name, selectedRowKeys: newSelectedRowKeys },
    //   ]);
    // }

    const newSelectedRowKeys = table_desc_obj.field_desc
                                .filter(field => field.in_use === 1)
                                .map(field => field.name);

    if (!type) {
      setSelectedRowKeys(newSelectedRowKeys);
    }
    setTableSelectedRowKeys(prevData => [
      ...prevData,
      { table_name: table_desc_obj.table_name, selectedRowKeys: newSelectedRowKeys },
    ]);


    setLoadingTableColumns(false);
  };

  const handleFieldDescriptionChange = (e, record) => {
    const value = e.target.value;
    const newSchemaListData = { ...SchemaListDataItem };
    newSchemaListData.field_desc = newSchemaListData.field_desc.map(item => {
      if (item.name === record.name) {
        item.comment = value;
      }
      return item;
    });
    setSchemaListDataItem(newSchemaListData);
    const newSchemaListDataList = SchemaListData.map(item => {
      if (item.table_name === newSchemaListData.table_name) {
        item.field_desc = newSchemaListData.field_desc;
      }
      return item;
    });
    setSchemaListData(newSchemaListDataList);
  };
  const handleTableDescriptionChange = e => {
    const value = e.target.value;
    setSchemaListDataItem(prevData => ({ ...prevData, table_comment: value }));
    const newSchemaListData = { ...SchemaListDataItem };
    newSchemaListData.table_comment = value;
    const newSchemaListDataList = SchemaListData.map(item => {
      if (item.table_name === newSchemaListData.table_name) {
        item.table_comment = newSchemaListData.table_comment;
      }
      return item;
    });
    setSchemaListData(newSchemaListDataList);
  };
  const changeSource = (e, item) => {
    setSchemaList([...SchemaList.map(i => (i.name === item.name ? { ...i, checked: e.target.checked } : i))]);
    if (e.target.checked) {
      setSelectSchema([...selectSchema, { ...item, checked: true }]);
      getTableColumns(item);
    } else {
      setSelectSchema(selectSchema.filter(i => i.name !== item.name));
      setSchemaListData(SchemaListData.filter(i => i.table_name !== item.name));
      setTableSelectedRowKeys(tableSelectedRowKeys.filter(i => i.table_name !== item.name));
      if (selectSchema.length < 1) {
        setSchemaListDataItem({});
      }
    }
    setSelectSchema(newSelectSchema => {
      if (newSelectSchema.length === SchemaList.length) {
        setIndeterminate(false);
        setCheckAll(true);
      } else if (newSelectSchema.length > 0) {
        setIndeterminate(true);
        setCheckAll(false);
      } else {
        setIndeterminate(false);
        setCheckAll(false);
      }
      return newSelectSchema;
    });
  };
  const changeSourceAll = (e, type = null) => {
    if (!type) return;
    setSelectLoading(true);
    setIndeterminate(false);
    setCheckAll(e.target.checked);
    const newSchemaList = [];
    const newSchemaListData = [];
    SchemaList.forEach(item => {
      newSchemaList.push({ ...item, checked: e.target.checked });
      // Only get table columns for items that were not already checked
      if (!item.checked && e.target.checked) {
        newSchemaListData.push(getTableColumns(item, "all"));
      }
    });
    setSchemaList(newSchemaList);
    setSelectSchema(e.target.checked ? newSchemaList : []);
    if (e.target.checked) {
      Promise.all(newSchemaListData).then(res => {
        setSelectLoading(false);
      });
    } else {
      setSelectSchema([]);
      setSchemaListDataItem({});
      setSchemaListData([]);
      setSelectedRowKeys([]);
      setTableSelectedRowKeys([]);
      setSelectLoading(false);
    }
  };

  const clickSchemaItem = item => {
    if (loadingTableColumns) {
      return;
    }
    const existingTable = SchemaListData.find(table => table.table_name === item.name);
    if (existingTable) {
      setSchemaListDataItem(existingTable);
      setSelectedRowKeys(existingTable.field_desc.filter(field => field.in_use === 1).map(field => field.name));
    } else {
      getTableColumns(item);
    }
  };

  const rowSelection = {
    selectedRowKeys,
    onChange: (selectedRowKeys, selectedRows) => {
      setSelectedRowKeys(selectedRowKeys);
      const newSchemaListData = { ...SchemaListDataItem };
      newSchemaListData.field_desc = newSchemaListData.field_desc.map(item => {
        item.in_use = selectedRowKeys.includes(item.name) ? 1 : 0;
        return item;
      });
      setSchemaListDataItem(newSchemaListData);
      const newSchemaListDataList = SchemaListData.map(item => {
        if (item.table_name === newSchemaListData.table_name) {
          item.field_desc = newSchemaListData.field_desc;
        }
        return item;
      });
      setSchemaListData(newSchemaListDataList);
      const existingTableSelectedRowKeys = tableSelectedRowKeys.find(
        table => table.table_name === newSchemaListData.table_name
      );
      if (existingTableSelectedRowKeys) {
        setTableSelectedRowKeys(
          tableSelectedRowKeys.map(table => {
            if (table.table_name === newSchemaListData.table_name) {
              table.selectedRowKeys = selectedRowKeys;
            }
            return table;
          })
        );
      } else {
        setTableSelectedRowKeys(prevData => [
          ...prevData,
          { table_name: newSchemaListData.table_name, selectedRowKeys: selectedRowKeys },
        ]);
      }

    },
  };

  const submit = async (data = []) => {
    setSubmitting(true);
    let tableNameList = [];
    let sourceData = [];
    let result = {};
    if (data.length > 0) {
      tableNameList = data.map(item => ({ name: item.table_name }));

      const promises = tableNameList.map(async item => {
        let res = await axios.get(`/api/data_table/columns/${source_id}/${item.name}`);
        if (res) {
          let obj = {
            table_name: res.table_name,
            table_comment: res.table_desc || "",
            field_desc: res.table_columns_info.field_desc,
          };
          data.forEach(item => {
            if (item.table_name === res.table_name) {
              obj.table_comment = item.table_comment;
              obj.field_desc.forEach((field, index) => {
                item.field_desc.forEach(i => {
                  if (field.name === i.name) {
                    obj.field_desc[index].comment = i.comment;
                  }
                });
              });
            }
          });
          sourceData.push(obj);
        }
      });

      await Promise.all(promises);
      result = { tableName: tableNameList };
      onSuccess(200, sourceData, source_item, result, "");
    } else {
      const checkedSchema = selectSchema.filter(item => item.checked);
      tableNameList = checkedSchema.map(item => ({ name: item.name }));
      result = { tableName: tableNameList };

      sourceData = SchemaListData.filter(item => checkedSchema.some(schema => schema.name === item.table_name));

      onSuccess(200, sourceData, source_item, result, "firstTableData");
    }
  };
  const dataSourceOptions = options.map(option => {
    const { type, value, label } = option;
    const imageFolder = IMG_ROOT;
    return (
      <Select.Option key={value} value={value}>
        <Space>
          <div aria-label={label} style={{ display: "flex", alignItems: "center" }}>
            <span role="img" style={{ marginRight: "5px" }}>
              <img src={`${imageFolder}/${type}.png`} alt={type} style={{ width: "30px", height: "30px" }} />
            </span>
            {label}
          </div>
        </Space>
      </Select.Option>
    );
  });
  const NoDataHtml = () => {
    return (
      <div style={{ textAlign: "center", display: "flex", flexDirection: "column", color: "#8f8f8f" }}>
        <InboxOutlinedIcon style={{ fontSize: "30px" }} />
        {window.W_L.no_data}
      </div>
    );
  };
  const closeEditData = () => {
    setEditData([]);
  };
  useImperativeHandle(ref, () => ({
    editTableData,
  }));
  const SchemaListIsShow = SchemaList && SchemaList.length > 0;
  const tableIsShow = SchemaListDataItem && SchemaListDataItem.table_name && SchemaListDataItem.field_desc.length > 0;

  return (
    !Charttable && (
      <div className="flex-column">
        <div className="dislogue-caption">
          <h1>Hi !</h1>
          <span>
            {chat_type === "chat"
              ? window.W_L.chat_start
              : chat_type === "autopilot"
              ? window.W_L.AutoPilot_start
              : window.W_L.report_start}
            <Link href="/data_sources">{window.W_L.add_datasource}</Link>
          </span>
        </div>
        <div className="select-content">
          <Spin spinning={SelectLoading} className={!submitting ? "" : "dislogue-spin"}>
            {editData && editData.length > 0 ? (
              <SecondChoice
                SelectLoading={SelectLoading}
                editData={editData}
                closeEditData={closeEditData}
                submit={submit}
                source_item={source_item}></SecondChoice>
            ) : (
              <div className="select-content-item">
                <div style={{ position: "absolute", bottom: "-30px", fontSize: "12px" }}>
                  <Tooltip title={window.W_L.source_tooltip}>
                    <InfoCircleOutlinedIcon style={{ marginRight: "3px" }} />
                  </Tooltip>
                  {window.W_L.source_first_tooltip}
                </div>
                <div style={{ display: "flex", alignItems: "baseline" }}>
                  <span className="select-content-item-span">
                    {window.W_L.data_source}
                    <Tooltip title={window.W_L.source_tooltip} placement="bottom">
                      <QuestionCircleOutlinedIcon style={{ marginLeft: "5px" }} />
                    </Tooltip>
                  </span>
                  <div>
                    <Select
                      style={{ width: 240 }}
                      showSearch
                      defaultValue={source_id}
                      className="dialogue-content-select"
                      placeholder={window.W_L.select_data_source}
                      optionFilterProp="children"
                      onChange={handleChange}
                      optionSelectedBg="#eff0f4"
                      disabled={SelectLoading}
                      filterOption={filterOption}>
                      {dataSourceOptions}
                    </Select>
                    <div className="select-main">
                      <ul className={!SchemaListIsShow ? "flex-center" : ""}>
                        {SchemaListIsShow && (
                          <div className="flex-end">
                            <span>{window.W_L.select_all}</span>
                            <Checkbox
                              indeterminate={indeterminate}
                              checked={checkAll}
                              onChange={e => changeSourceAll(e, "all")}
                            />
                          </div>
                        )}
                        {SchemaListIsShow ? (
                          SchemaList.map((item, index) => (
                            <li key={index}>
                              <Tooltip title={item.label}>
                                <span
                                  onClick={() => clickSchemaItem(item)}
                                  style={{ color: item.label === SchemaListDataItem.table_name ? "#2196F3" : "#333" }}>
                                  {item.label}
                                </span>
                              </Tooltip>
                              <Checkbox checked={item.checked} onChange={e => changeSource(e, item)} />
                            </li>
                          ))
                        ) : (
                          <NoDataHtml />
                        )}
                      </ul>
                    </div>
                  </div>
                </div>
                <div className="table-columns">
                  {tableIsShow && (
                    <div className="table-columns-item">
                      <span>
                        {source_item.type === "csv" ? SchemaListDataItem.table_comment : SchemaListDataItem.table_name}
                      </span>
                      <Input
                        placeholder={window.W_L.sheet_description}
                        style={{ padding: "2px", maxWidth: "60%" }}
                        type="text"
                        disabled={source_item.type === "csv" ? true : false}
                        value={
                          source_item.type === "csv" ? SchemaListDataItem.table_name : SchemaListDataItem.table_comment
                        }
                        onChange={handleTableDescriptionChange}
                      />
                    </div>
                  )}
                  <div
                    className={!tableIsShow ? "flex-center" : ""}
                    style={{ width: "400px", overflowY: "scroll", height: tableIsShow ? "235px" : "276px" }}>
                    {tableIsShow ? (
                      <Table
                        rowSelection={{
                          type: "checkbox",
                          ...rowSelection,
                        }}
                        dataSource={SchemaListDataItem.field_desc}
                        columns={columns}
                        size="small"
                        rowKey={record => record.name}
                        pagination={false}></Table>
                    ) : (
                      <NoDataHtml />
                    )}
                  </div>
                </div>
                {selectSchema && selectSchema.length > 0 && (
                  <button onClick={submit} disabled={SelectLoading} className="submit-btn">
                    {window.W_L.submit} {selectSchema.length}
                  </button>
                )}
              </div>
            )}
          </Spin>
          {SelectLoading && submitting && (
            <div className="dislogue-progress">
              <Progress width={80} type="circle" percent={percent} />
              <p>{window.W_L.ai_understanding_data}</p>
            </div>
          )}
        </div>
      </div>
    )
  );
});

export default SelectSource;
