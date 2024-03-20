import React from 'react';
import Select from 'antd/lib/select';
import shopsData from "@/assets/CommenExpressions/CommenExpressions.json";
import "./index.less";
const { Option } = Select;

class ChatShopSelect extends React.Component {
  static defaultShop = [
    { "id": "0", "label": "默认" }
  ];
  constructor(props) {
    super(props);
    const processedShopsData = shopsData && shopsData.length > 0
      ? shopsData.map((shop, index) => ({ ...shop, id: String(index + 1) }))
      : [];
    this.state = {
      chat_type: props.chat_type || "chat",
      selectedShop: "0", // 默认选中
      shops: [ChatShopSelect.defaultShop[0], ...processedShopsData],
    };
  }

  componentDidMount() {
    const selectedShopData = sessionStorage.getItem(`${this.state.chat_type}CommonExpressions`);
    if (selectedShopData) {
      const selectedShop = JSON.parse(selectedShopData);
      const shopExists = this.state.shops.some(shop => shop.id === selectedShop.id);
      if (selectedShop && selectedShop.id && shopExists) {
        this.setState({ selectedShop: selectedShop.id });
      } else {
        // 如果不存在，重置为默认商店
        this.setState({ selectedShop: ChatShopSelect.defaultShop[0].id });
      }
    }
  }

  handleChange = (value) => {
    try {
      const selectedShop = this.state.shops.find(shop => shop.id === value);
      this.setState({ selectedShop: value });
      if (selectedShop) {
        sessionStorage.setItem(`${this.state.chat_type}CommonExpressions`, JSON.stringify(selectedShop));
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
        placement="topLeft"
        filterOption={(input, option) =>
          option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
        }
      >
        {shops.map((shop) => (
          <Option key={shop.id} value={shop.id}>
            {shop.label}
          </Option>
        ))}
      </Select>
    );
  }
}

export default ChatShopSelect;
