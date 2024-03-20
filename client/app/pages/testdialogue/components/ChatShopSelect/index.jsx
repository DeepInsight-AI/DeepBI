import React from 'react';
import Select from 'antd/lib/select';
import shopsData from './shop.json'; // 确保路径正确
import "./index.less";
const { Option } = Select;

class ChatShopSelect extends React.Component {
  state = {
    selectedShop: "0", // 当前选中的商店
    shops: shopsData, // 从JSON文件中读取商店列表
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
        style={{ width: 200 ,position:"absolute",left:"0",top:"-30px"}}
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