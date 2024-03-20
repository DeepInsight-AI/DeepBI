import React from 'react';
import Select from 'antd/lib/select';
import shopsData from "@/assets/CommenExpressions/CommenExpressions.json";
import "./index.less";
const { Option } = Select;

class ChatShopSelect extends React.Component {
  static defaultShop = [
    { "id": "0", "name": "默认" }
  ];

  state = {
    selectedShop: "0",
    shops: shopsData && shopsData.length > 0 ? shopsData : ChatShopSelect.defaultShop,
  };

  handleChange = (value) => {
    try {
      const selectedShop = this.state.shops.find(shop => shop.id === value);
      this.setState({ selectedShop: value });
      if (selectedShop && selectedShop.name!== "默认") {
        localStorage.setItem("CommenExpressions", selectedShop.name);
      }
    } catch (error) {
      console.log(error);
    }
  };

  render() {
    const { shops, selectedShop } = this.state;
    return (
      <Select
        showSearch
        style={{ width: 150, position: "absolute", left: "0", top: "-35px" }}
        placeholder="选择常用语"
        optionFilterProp="children"
        onChange={this.handleChange}
        className="custom-select"
        value={selectedShop}
        planment="topLeft"
        filterOption={(input, option) =>
          option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
        }
      >
        {shops.map((shop) => (
          <Option key={shop.id} value={shop.id}>
            {shop.name}
          </Option>
        ))}
      </Select>
    );
  }
}

export default ChatShopSelect;