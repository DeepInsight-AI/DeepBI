export const chart = (sqlId,options) => {
    return {
            description: "",
            name: window.W_L.CHART,
            query_id: sqlId,
            // type: "CHART",
            type: options[0].type,
            options: {
              alignYAxesAtZero: false,
              globalSeriesType: options[0].globalSeriesType,
              coefficient: 1,
              columnMapping: options[0].columnMapping,
              dateTimeFormat: "DD/MM/YY HH:mm",
              direction: {
                type: "counterclockwise"
              },
              error_y: {
                type: "data",
                visible: true
              },
              legend: {
                enabled: true,
                placement: "auto",
                traceorder: "normal"
              },
              missingValuesAsZero: true,
              numberFormat: "0,0[.]00000",
              percentFormat: "0[.]00%",
              series: {
                stacking: null,
                error_y: {
                  type: "data",
                  visible: true
                },
              },
              seriesOptions: {},
              showDataLabels: true,
              sizemode: "diameter",
              sortX: false,
              textFormat: "",
              valuesOptions: {},
              xAxis: {
                type: "category",
                labels: {
                  enabled: true,
                }
              },
              yAxis: [
                { type: "linear" },
                { type: "linear", opposite: true }
              ],
            }
    }
}

// CHOROPLETH
// {"type":"CHOROPLETH","name":"地理分布图(Choropleth Map)","description":"","options":{"mapType":"china_420000","keyColumn":"地级市名","targetField":"name","valueColumn":"2019年GDP","clusteringMode":"e","steps":11,"valueFormat":"0,0.00","noValuePlaceholder":"N/A","colors":{"min":"#799CFF","max":"#002FB4","background":"#ffffff","borders":"#000000","noValue":"#dddddd"},"legend":{"visible":true,"position":"bottom-left","alignText":"right"},"tooltip":{"enabled":true,"template":"<b>{{ @@name }}</b>: {{ @@value }}"},"popup":{"enabled":false,"template":"地区: <b>{{ @@name_long }} ({{ @@iso_a2 }})</b>\n<br>\n值: <b>{{ @@value }}</b>"}},"query_id":796}
export const choropleth = (sqlId,options) => {
    return {
            description: "",
            name: window.W_L.CHOROPLETH,
            query_id: sqlId,
            type: options[0].type,
            options: {
              mapType: "china_420000",
              keyColumn: options[0].keyColumn,
              targetField: options[0].targetField,
              valueColumn: options[0].valueColumn,
              clusteringMode: "e",
              steps: 11,
              valueFormat: "0,0.00",
              noValuePlaceholder: "N/A",
              colors: {
                min: "#799CFF",
                max: "#002FB4",
                background: "#ffffff",
                borders: "#000000",
                noValue: "#dddddd"
              },
              legend: {
                visible: true,
                position: "bottom-left",
                alignText: "right"
              },
              tooltip: {
                enabled: true,
                template: "<b>{{ @@name }}</b>: {{ @@value }}"
              },
              popup: {
                enabled: false,
                template: "地区: <b>{{ @@name_long }} ({{ @@iso_a2 }})</b>\n<br>\n值: <b>{{ @@value }}</b>"
              }
            }
    }
}

