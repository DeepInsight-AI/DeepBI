ECHARTS_BAR_PROMPT = """
option = {
/*  grid: {
    top: "20%",
    bottom: "12%"
  },*/
  tooltip: {
    trigger: "axis",
    axisPointer: {
      type: "shadow",
      label: {
        show: true
      }
    }
  },
  legend: {
    data: [],
    top: "2%",
    right:'10',
    textStyle: {
      color: "rgba(250,250,250,0.6)",
      fontSize: 16
    }
  },
  xAxis: {
    data: [],
    axisLine: {
      show: true, //隐藏X轴轴线
      lineStyle: {
        color: '#26D9FF',
        width:2
      }
    },
    axisTick: {
      show: true //隐藏X轴刻度
    },
    axisLabel: {
      show: true,
      textStyle: {
        color: "rgba(250,250,250,0.6)", //X轴文字颜色
        fontSize: 16
      }
    },
    splitArea: {
      show: true,
      areaStyle: {
        color: ["rgba(250,250,250,0.1)", "rgba(250,250,250,0)"]
      }
    }
  },
  yAxis: [{
    type: "value",
    nameTextStyle: {
      color: "#ebf8ac",
      fontSize: 16
    },
    splitLine: {
      show: false
    },
    axisTick: {
      show: true
    },
    axisLine: {
      show: true,
      lineStyle: {
        color: '#26D9FF',
        width:2
      }
    },
    axisLabel: {
      show: true,
      textStyle: {
        color: "rgba(250,250,250,0.6)",
        fontSize: 16
      }
    },

  },
    {
      type: "value",
      nameTextStyle: {
        color: "#ebf8ac",
        fontSize: 16
      },
      position: "right",
      splitLine: {
        show: false
      },
      axisTick: {
        show: false
      },
      axisLine: {
        show: false
      },
      axisLabel: {
        show: true,
        formatter: "{value} %", //右侧Y轴文字显示
        textStyle: {
          color: "rgba(250,250,250,0.6)",
          fontSize: 16
        }
      }
    }
  ],
  series: [{
    name: "销售量",
    type: "line",
    yAxisIndex: 1, //使用的 y 轴的 index，在单个图表实例中存在多个 y轴的时候有用
    smooth: true, //平滑曲线显示
    showAllSymbol: true, //显示所有图形。
    symbol: "circle", //标记的图形为实心圆
    symbolSize: 8, //标记的大小
    itemStyle: {
      //折线拐点标志的样式
      color: "#1045A0",
      borderColor: "#3D7EEB",
      width: 2,
      shadowColor: "#3D7EEB",
      shadowBlur: 4
    },
    lineStyle: {
      color: "#3D7EEB",
      width:2,
      shadowColor: "#3D7EEB",
      shadowBlur: 4
    },
    areaStyle: {
      color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{
        offset: 0,
        color: "rgba(61,126,235, 0.5)"
      },
        {
          offset: 1,
          color: "rgba(61,126,235, 0)"
        }
      ])
    },
    data: [4.2, 3.5, 2.9, 7.8, 5, 3]
  },
    {
      name: "业务1",
      type: "bar",
      barWidth: 15,
      itemStyle: {
        normal: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{
            offset: 0,
            color: "rgba(61,126,235, 1)"
          },
            {
              offset: 1,
              color: "rgba(61,126,235, 0)"
            }
          ]),
          borderColor:new echarts.graphic.LinearGradient(0, 0, 0, 1, [{
            offset: 0,
            color: "rgba(160,196,225, 1)"
          },
            {
              offset: 1,
              color: "rgba(61,126,235, 1)"
            }
          ]),
          borderWidth: 2
        }
      },
      data: [4, 3, 2, 8, 3, 5]
    },
    {
      name: "业务2",
      type: "bar",
      barWidth: 15,
      itemStyle: {
        normal: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{offset: 0, color: 'rgba(15,197,243,1)'}, {offset: 1, color: 'rgba(15,197,243,0)'}]),
          borderColor:new echarts.graphic.LinearGradient(0, 0, 0, 1, [{offset: 0, color: 'rgba(180,240,255,1)'}, {offset: 1, color: 'rgba(15,197,243,1)'}]),
          borderWidth: 2
        }
      },
      data: [3, 2, 3, 5, 4, 3]
    }
  ]
};
This is an ECharts bar chart example, you need to generate the option for the bar chart based on the data, and then return it.
- Don't use the data from the examples, just refer to the styles.
- Pay attention to the data field in the option, you need to generate the corresponding array based on this data, not generate data on your own.
- In the data field, fill in the complete data, such as ["0.09", "0.10", "0.11", ..., "0.92"], instead of using ... to omit.
- My page background color is #00206d, change the color, shape (such as rounded corners, right angles, etc.), stacking method, data label style, etc. of the bar chart to make the chart more beautiful and eye-catching.
- Set the background of echarts to transparent, that is, backgroundColor: 'rgba(0,0,0,0)', do not set it to white.
- You must use the x-axis and y-axis fields defined in columnMapping. If there are multiple y-axis fields, you need to display multiple y-axis data. The data for the x-axis and y-axis should be obtained from rows.
- Set animation effects to make the chart more lively.
- Draw multiple series of bar charts for comparing different data.
- Set stacked bar charts to show the proportion of data.
- If there are multiple y-axis data, combine with line charts to make the chart richer.
- Use gradient colors and shadow effects to enhance the visual effect of the chart.
- Add multi-dimensional labels and legends to provide more information and interactivity.
- Do not use iterative cases, for example. map(),.sort(), for loop, etc.
- Do not include the title part.
- Only need the option part, do not include "var option = {...}".
- Do not add comments in the code, such as "<!-- Add other navigation links as needed -->" and "<!-- ...other news items... -->", but write out the complete code.
- Do not include markdown "```" or "```html" at the beginning or end.
- Please only return code, do not include any explanatory text or comments.
"""

ECHARTS_PIE_PROMPT = """
option = {
   color: ['#36F097', '#3DFFDC', '#5A3FFF', '#268AFF', '#1ED6FF', '#ADE1FF'],
   tooltip: {
      trigger: 'item'
   },
   //   legend: {
   //     orient: 'horizontal',
   //     bottom:20,
   //     right:20,
   //     textStyle:{
   //        color:'#fff'
   //     }
   //   },
   grid: {
      left: '3%',
      right: '4%',
      bottom: '10%',
      top: '10%',
   },
   series: [
      {
         name: 'Access From',
         type: 'pie',
         roseType: "radius",
         radius: '70%',
         center: ["50%", "48%"],
         itemStyle: {
            // 需要间距时打开
            // borderColor: '#000',
            // borderWidth: 3,
            shadowColor: 'rgba(0, 0, 0, 0.3)',
            shadowBlur: 20
         },
         data: [
            { value: 648, name: 'Search Engine' },
            { value: 535, name: 'Direct' },
            { value: 500, name: 'Email' },
            { value: 484, name: 'Union Ads' },
            { value: 400, name: 'Video Ads' },
            { value: 366, name: 'Audio Ads' }

         ],
         labelLine: {
            show: true,
            lineStyle: {
               color: '#fff',
            },
         },
      }
   ]
};
This is an ECharts pie chart example, you need to generate the option for the pie chart based on the data, and then return it.
- Don't use the data from the examples, just refer to the styles.
- Pay attention to the data field in the option, you need to generate the corresponding array based on this data, not generate data on your own.
- In the data field, fill in the complete data, such as ["0.09", "0.10", "0.11", ..., "0.92"], instead of using ... to omit.
- My page background color is #00206d, modify the color, transparency, rounded corners, etc., to make the chart more beautiful and eye-catching.
- Set the background of echarts to transparent, that is, backgroundColor: 'rgba(0,0,0,0)', do not set it to white.
- You must use the x-axis and y-axis fields defined in columnMapping. If there are multiple y-axis fields, you need to display multiple y-axis data. The data for the x-axis and y-axis should be obtained from rows.
- Ensure the uniqueness of the color for each data, there should not be a situation where the colors between the data are the same.
- Set animation effects to make the chart more lively.
- Use gradient colors and shadow effects to enhance the visual effect of the chart.
- Add multi-dimensional labels and legends to provide more information and interactivity.
- Do not use iterative cases, for example. map(),.sort(), for loop, etc.
- Do not include the title part.
- Only need the option part, do not include "var option = {...}".
- Do not add comments in the code, such as "<!-- Add other navigation links as needed -->" and "<!-- ...other news items... -->", but write out the complete code.
- Do not include markdown "```" or "```html" at the beginning or end.
- Please only return code, do not include any explanatory text or comments.
"""

ECHARTS_LINE_PROMPT = """
 option = {
   //你的代码

    color: ['#165DFF'],
    tooltip: {
      show:false,
      trigger: 'axis',
      axisPointer: {
        // 坐标轴指示器，坐标轴触发有效
        type: 'shadow' // 默认为直线，可选为：'line' | 'shadow'
      }
    },
    grid: {
      left: '0%',
      right: '5%',
      bottom: '0%',
      containLabel: true
    },
    xAxis: [
      {
        type: 'category',
        data: ['2023/01','2023/02','2023/03','2023/04','2023/05','2023/06','2023/07','2023/08','2023/09','2023/10','2023/11','2023/12'],
        boundaryGap: false,
        axisTick:{
          show:false // 不显示坐标轴刻度线
        },
        splitLine: {
          show: false,
        },
        axisLine:{
          show: false,
        },
        axisLabel: {
          interval:0,
          color: "#86909C",
          fontSize:14
        }
      }
    ],
    yAxis: [
      {
        type: 'value',
        show: false,
        //y右侧文字
        axisLabel: {
          color: 'rgba(230, 247, 255, 0.50)',
          fontSize:16
        },
        // y轴的分割线
        splitLine: {
          show: true,
          lineStyle: {
            type:'dashed',
            color:'rgba(230, 247, 255, 0.20)',

          }
        }
      }
    ],
    series: [
      {
        name: '',
        type: 'line',
        smooth: true,
        symbol: 'none', // 不显示连接点
        tooltip: {
          trigger: 'axis'
        },
        lineStyle: {
          width: 2,
          shadowColor: "#165DFF",
          shadowBlur: 20
        },
		areaStyle: {
		    opacity: 0.8,
		    color: {
		        x: 0,
		        y: 0,
		        x2: 0,
		        y2: 1,
		        colorStops: [{
                  offset: 0,
                  color: 'rgba(22, 93, 255, 0.5)'
                },
                {
                  offset: 0.3,
                  color: 'rgba(22, 93, 255, 0.2)'
                },
                {
                  offset: 1,
                  color: 'rgba(22, 93, 255, 0)'
                }],

		    }
		},
        data: [140, 232, 101, 264, 90, 340, 250, 232, 101, 264, 90, 340]
      }
    ]

};

This is an ECharts line chart example, you need to generate the option for the line chart based on the data, and then return it.
- Don't use the data from the examples, just refer to the styles.
- Pay attention to the data field in the option, you need to generate the corresponding array based on this data, not generate data on your own.
- In the data field, fill in the complete data, such as ["0.09", "0.10", "0.11", ..., "0.92"], instead of using ... to omit.
- My page background color is #00206d, change the color, width, shadow color, and shadow blur of the line. In the area style, change the color and transparency to make the chart more beautiful and eye-catching.
- Set the background of echarts to transparent, that is, backgroundColor: 'rgba(0,0,0,0)', do not set it to white.
- Change the shape of the line, such as solid, dashed, dotted, etc.
- Change the style of the line, such as smooth, polyline, etc.
- Change the style of the data points, such as round, square, star, etc.
- Change the border style of the chart.
- You must use the x-axis and y-axis fields defined in columnMapping. If there are multiple y-axis fields, you need to display multiple y-axis data. The data for the x-axis and y-axis should be obtained from rows.
- Set animation effects to make the chart more lively.
- Do not use iterative cases, for example. map(),.sort(), for loop, etc.
- Do not include the title part.
- Only need the option part, do not include "var option = {...}".
- Do not add comments in the code, such as "<!-- Add other navigation links as needed -->" and "<!-- ...other news items... -->", but write out the complete code.
- Do not include markdown "```" or "```html" at the beginning or end.
- Please only return code, do not include any explanatory text or comments.
"""


def assemble_prompt(
        chart_type, data
):
    system_content = ECHARTS_BAR_PROMPT
    if chart_type == "bar":
        system_content = ECHARTS_BAR_PROMPT
    elif chart_type == "pie":
        system_content = ECHARTS_PIE_PROMPT
    elif chart_type == "line":
        system_content = ECHARTS_LINE_PROMPT
    return [
        {
            "role": "system",
            "content": system_content,
        },
        {
            "role": "user",
            "content": data,
        },
    ]
