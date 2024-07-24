import React from 'react';
import Button from "antd/lib/button";
import './index.css';

const Overlay = (props) => {
  const {loadingMask } = props;
  const onUse = () => {
    props.onUse();
  };
  return (
    <div className="overlay">
     <Button className="overlay-btn" onClick={onUse} loading={loadingMask}>{loadingMask?window.W_L.init_config:window.W_L.start_use}</Button>
    </div>
  );
};

export default Overlay;
