import React, { memo, useEffect, useRef, useState } from 'react';
const AutoPilot = memo(({ content }) => {
  const autopilotRef = useRef(null);
  const [AutoPilotJson, setAutoPilotJson] = useState({});

  useEffect(() => {
    if(!content) return;
    const data = JSON.parse(content);
    let div = document.createElement("div")
    div.innerHTML = Html
    chartRef.current.append(div)
    // eval解析
  let scripts = div.querySelectorAll("script")
  scripts.forEach(item => {
    window.eval(item.innerText);
  });
  }, [content]);

  return <div ref={chartRef}/>;
});

export default AutoPilot;