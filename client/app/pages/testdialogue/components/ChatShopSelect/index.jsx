import React from 'react';
import Select from 'antd/lib/select';
import shopsData from './shop.json'; // 确保路径正确
import "./index.less";
const { Option } = Select;

class ChatShopSelect extends React.Component {
  static defaultShop = [
    { "id": "0", "name": "所有" }
  ];

  state = {
    selectedShop: "0",
    shops: shopsData && shopsData.length > 0 ? shopsData : ChatShopSelect.defaultShop,
  };

  handleChange = (value) => {
    this.setState({ selectedShop: value });
    localStorage.setItem("selectedShop", value);
    // 这里可以添加更多处理，例如调用父组件的方法
  };

  render() {
    const { shops, selectedShop } = this.state;
    return (
      <Select
        showSearch
        style={{ width: 150 ,position:"absolute",left:"0",top:"-35px"}}
        placeholder="选择店铺"
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