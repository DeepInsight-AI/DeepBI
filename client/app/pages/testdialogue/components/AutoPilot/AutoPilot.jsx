import React, { memo, useEffect, useRef, useState } from 'react';
import * as echarts from 'echarts';
const AutoPilot = memo(({ content }) => {
  const autopilotRef = useRef(null);
  // const [AutoPilotJson, setAutoPilotJson] = useState({});

  useEffect(() => {
    if(!content) return;
    const data = content;
    let div = document.createElement("div")
    div.innerHTML = data
    autopilotRef.current.append(div)
    // eval解析
  let scripts = div.querySelectorAll("script")
  scripts.forEach(item => {
    window.eval(item.innerText);
  });
  }, [content]);

  return <div ref={autopilotRef}/>;
});

export default AutoPilot;