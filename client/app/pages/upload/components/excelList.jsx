import React from 'react';
import PlainButton from "@/components/PlainButton";
import csv from "@/assets/images/db-logos/csv.png";
import CloseCircleOutlinedIcon from "@ant-design/icons/CloseCircleOutlined";

const ExcelList = (props) => {
    const {excelList,delList} = props;
    const handleDeleteItem = (item) => {
        delList(item);
    }
    function ListItem({ item, keySuffix }) {
        const commonProps = {
          key: `card${keySuffix}`,
          className: "visual-card",
        //   onClick: item.onClick,
          children: (
            <div style={{display: "flex",alignItems: "center",position: "relative",height: "100%",width: "100%"}}>
                <div style={{position: "absolute",right: "0",top: "-10px",fontSize: "16px",color: "#999898"}} onClick={() => handleDeleteItem(item)}> <CloseCircleOutlinedIcon/></div>
              <img alt={item.source_name} src={csv} />
              <h3>{item.source_name}</h3>
            </div>
          ),
        };
      
        return <PlainButton type="link" {...commonProps} />;
      }
  return (
   
    <div className="row">
    <div className="col-lg-12 d-inline-flex flex-wrap visual-card-list">
      {excelList.map((item, index) => (
        <ListItem key={index} item={item} keySuffix={index.toString()} />
      ))}
    </div>
  </div>
  );
};

export default ExcelList;