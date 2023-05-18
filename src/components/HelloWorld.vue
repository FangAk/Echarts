<template>
  <div class="block">
    <div ref="main" style="width: 800px; height: 400px"></div>
    <div>
      <el-dropdown
        placement=" bottom-start"
        trigger="click"
        :hide-on-click="false"
      >
        <span class="el-dropdown-link">
          图例控制<i class="el-icon-arrow-down el-icon--right"></i>
        </span>

        <el-dropdown-menu slot="dropdown">
          <el-dropdown-item v-for="item in options" :key="item.name"
            ><el-checkbox v-model="item.ischeck" @change="test(item)">{{
              item.label
            }}</el-checkbox></el-dropdown-item
          >
        </el-dropdown-menu>
      </el-dropdown>
    </div>
  </div>
</template>
<script>
const cityOptions = ["上海", "北京", "广州", "深圳"];
import * as echarts from "echarts";
export default {
  name: "time-demo",
  data() {
    return {
      checkAll: false,
      checkedCities: ["上海", "北京"],
      cities: cityOptions,
      isIndeterminate: true,
      myChartInstance: {},
      myChartInstancetwo: {},
      patrolProblem: "总",
      options: [
        {
          ischeck: true,
          label: "所有",
        },
        {
          ischeck: true,
          label: "2015",
        },
        {
          ischeck: true,
          label: "2016",
        },
        {
          ischeck: true,
          label: "2017",
        },
        {
          ischeck: true,
          label: "2018",
        },
      ],
      date: [
        10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.1, 10.11,
        10.12, 10.13, 10.14, 10.15, 10.16, 10.17, 10.18, 10.19, 10.2, 10.21,
        10.22, 10.23, 10.24, 10.25, 10.26, 10.27, 10.28, 10.29, 10.3,
      ],
      dateval: ["直接登陆平台", "扫码登陆平台", "总"],
    };
  },
  mounted() {
    this.buildCharts();
    this.line();
  },
  methods: {
    test1(v) {
      //通过select控制echarts legend的显示隐藏
      // console.log(v);
      // this.myChartInstancetwo.dispatchAction({
      //   type: "legendToggleSelect",
      //   name: v,
      // });
    },
    test(d) {
      var testd = this.myChartInstance;
      if (d.label === "所有" && d.ischeck) {
        this.options.forEach(function (item) {
          item.ischeck = true;
        });
        testd.dispatchAction({
          type: "legendAllSelect",
        });
      } else if (d.label === "所有" && !d.ischeck) {
        testd.dispatchAction({
          type: "legendInverseSelect",
        });
        this.options.forEach(function (item) {
          item.ischeck = false;
        });
      } else {
        testd.dispatchAction({
          type: "legendToggleSelect",
          name: d.label,
        });
        //https://blog.csdn.net/BloodyMandoo/article/details/72902830?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522166685891416782417034501%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=166685891416782417034501&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~baidu_landing_v2~default-1-72902830-null-null.142^v62^control_1,201^v3^control_1,213^v1^t3_control1&utm_term=echarts%20legendToggleSelect&spm=1018.2226.3001.4187

        // var array = this.options;
        // var count1 = 0;
        // var count2 = 0;
        // for (let index = 1; index < array.length; index++) {
        //   const element = array[index];
        //   if (element.ischeck) {
        //     count1++;
        //   } else {
        //     count2++;
        //   }
        // }
        // if (count1 == array.length - 1) {
        //   this.options[0].ischeck = true;
        // }
        // if (count2 == array.length - 1) {
        //   this.options[0].ischeck = false;
        // }
      }
    },
    buildCharts() {
      console.log(this.$refs.main);
      var myChart = echarts.init(this.$refs.main);
      this.myChartInstance = myChart;
      // 绘制图表
      myChart.setOption({
        legend: {},
        tooltip: {},
        dataset: {
          source: [
            ["product", "2015", "2016", "2017", "2018"],
            ["Matcha Latte", 43.3, 85.8, 93.7],
            ["Milk Tea", 83.1, 73.4, 55.1],
            ["Cheese Cocoa", 86.4, 65.2, 82.5],
            ["Walnut Brownie", 72.4, 53.9, 39.1],
            ["f Brownie", 72, 53, 39],
          ],
        },
        xAxis: { type: "category" },
        yAxis: {},

        series: [
          { type: "line" },
          { type: "line" },
          { type: "line" },
          { type: "line" },
        ],
      });
    },
    line() {
      var myChart = echarts.init(this.$refs.line);
      this.myChartInstancetwo = myChart;
      const colorList = ["#1E90FF", "#FFA500", "#FF4500"];
      // 日期
      var date = this.date;

      var data1 = [7, 10, 0, 4, 6, 10, 25, 28, 24, 200, 25, 22, 20]; // 直接登录平台
      var data2 = [3, 4, 0, 2, 0, 2, 5, 3, 4, 5, 2, 0, 0]; // 扫码登录平台
      var data3 = [4, 14, 0, 6, 6, 12, 30, 31, 28, 34, 27, 22, 20]; // 总

      var option = {
        backgroundColor: "#FFFFFF",
        title: {
          text: "2021年10月每日新增用户",
          textStyle: {
            align: "center",
            fontSize: 20,
          },
          top: "5%",
          left: "center",
          show: true,
        },
        legend: {
          x: "center",
          y: "top",
          show: true,
          left: "5%",
          top: "5%",
          itemWidth: 6,
          itemGap: 20,
          textStyle: {
            color: "#556677",
          },
          data: ["直接登录平台", "扫码登录平台", "总"],
        },
        tooltip: {
          trigger: "axis",
          axisPointer: {
            label: {
              show: true,
              backgroundColor: "#fff",
              color: "#556677",
              borderColor: "rgba(0,0,0,0)",
              shadowColor: "rgba(0,0,0,0)",
              shadowOffsetY: 0,
            },
            lineStyle: {
              width: 0,
            },
          },
          backgroundColor: "#fff",
          textStyle: {
            color: "#5c6c7c",
          },
          padding: [10, 10],
          extraCssText: "box-shadow: 1px 0 2px 0 rgba(163,163,163,0.5)",
        },
        grid: {
          top: "15%",
          y2: 88,
        },
        // 滑动轴显示比例
        dataZoom: [
          {
            type: "inside",
            start: 0,
            end: 40,
          },
          {
            start: 0,
            end: 40,
          },
        ],
        xAxis: [
          {
            type: "category",
            name: "日期",
            data: date,
            axisLine: {
              show: true,
              lineStyle: {
                color: "#000000",
              },
            },
            axisTick: {
              show: true,
            },
            axisLabel: {
              interval: 0,
              textStyle: {
                color: "#556677",
              },
              // 默认x轴字体大小
              fontSize: 12,
              // margin:文字到x轴的距离
              margin: 15,
            },
            axisPointer: {
              label: {
                // padding: [11, 5, 7],
                padding: [0, 0, 10, 0],
                /*
                    除了padding[0]建议必须是0之外，其他三项可随意设置

                    和CSSpadding相同，[上，右，下，左]

                    如果需要下边线超出文字，设左右padding即可，注：左右padding最好相同

                    padding[2]的10:

                    10 = 文字距下边线的距离 + 下边线的宽度

                    如：UI图中文字距下边线距离为7 下边线宽度为2

                    则padding: [0, 0, 9, 0]

                                */
                // 这里的margin和axisLabel的margin要一致!
                margin: 15,
                // 移入时的字体大小
                fontSize: 12,
                backgroundColor: {
                  type: "linear",
                  x: 0,
                  y: 0,
                  x2: 0,
                  y2: 1,
                  colorStops: [
                    {
                      offset: 0,
                      color: "#fff", // 0% 处的颜色
                    },
                    {
                      // offset: 0.9,
                      offset: 0.86,
                      /*
                0.86 = （文字 + 文字距下边线的距离）/（文字 + 文字距下边线的距离 + 下边线的宽度）

                                        */
                      color: "#fff", // 0% 处的颜色
                    },
                    {
                      offset: 0.86,
                      color: "#33c0cd", // 0% 处的颜色
                    },
                    {
                      offset: 1,
                      color: "#33c0cd", // 100% 处的颜色
                    },
                  ],
                  global: false, // 缺省为 false
                },
              },
            },
            splitLine: {
              show: true,
              lineStyle: {
                type: "dashed",
              },
            },
            boundaryGap: false,
          },
        ],
        yAxis: [
          {
            type: "value",
            name: "单位：人数",
            nameTextStyle: {
              color: "#000000",
            },
            axisTick: {
              show: false,
            },
            axisLine: {
              show: true,
              lineStyle: {
                color: "#DCE2E8",
              },
            },
            axisLabel: {
              textStyle: {
                color: "#556677",
              },
            },
            splitLine: {
              show: true,
              lineStyle: {
                type: "dashed",
              },
            },
          },
        ],
        series: [
          {
            name: "直接登录平台",
            type: "line",
            data: data1,
            symbolSize: 1,
            symbol: "circle",
            smooth: true,
            yAxisIndex: 0,
            showSymbol: false,
            emphasis: {
              focus: "series",
            },
            lineStyle: {
              width: 5,
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                {
                  offset: 0,
                  color: "#87CEFA",
                },
                {
                  offset: 1,
                  color: "#1E90FF",
                },
              ]),
              shadowColor: "rgba(135,206,250, 0.3)",
              shadowBlur: 10,
              shadowOffsetY: 20,
            },
            itemStyle: {
              normal: {
                color: colorList[0],
                borderColor: colorList[0],
              },
            },
            markPoint: {
              symbol: "pin", //标记(气泡)的图形
              symbolSize: 50, //标记(气泡)的大小
              itemStyle: {
                // color: '#4587E7', //图形的颜色。
                borderColor: "#000", //图形的描边颜色。支持的颜色格式同 color，不支持回调函数。
                borderWidth: 0, //描边线宽。为 0 时无描边。
                borderType: "solid", //柱条的描边类型，默认为实线，支持 ‘solid’, ‘dashed’, ‘dotted’。
              },
              data: [
                { type: "max", name: "最大值" },
                { type: "min", name: "最小值" },
              ],
            },
            markLine: {
              data: [{ type: "average", name: "平均值" }],
            },
          },
          {
            name: "扫码登录平台",
            type: "line",
            data: data2,
            symbolSize: 1,
            symbol: "circle",
            smooth: true,
            yAxisIndex: 0,
            showSymbol: false,
            emphasis: {
              focus: "series",
            },
            lineStyle: {
              width: 5,
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                {
                  offset: 0,
                  color: "#FFE4B5",
                },
                {
                  offset: 1,
                  color: "#FFA500",
                },
              ]),
              shadowColor: "rgba(255,140,0, 0.3)",
              shadowBlur: 10,
              shadowOffsetY: 20,
            },
            itemStyle: {
              normal: {
                color: colorList[1],
                borderColor: colorList[1],
              },
            },
            markPoint: {
              symbol: "pin", //标记(气泡)的图形
              symbolSize: 50, //标记(气泡)的大小
              itemStyle: {
                color: "#4587E7", //图形的颜色。
                borderColor: "#000", //图形的描边颜色。支持的颜色格式同 color，不支持回调函数。
                borderWidth: 0, //描边线宽。为 0 时无描边。
                borderType: "solid", //柱条的描边类型，默认为实线，支持 ‘solid’, ‘dashed’, ‘dotted’。
              },
              data: [
                { type: "max", name: "最大值" },
                { type: "min", name: "最小值" },
              ],
            },
            markLine: {
              data: [{ type: "average", name: "平均值" }],
            },
          },
          {
            name: "总",
            type: "line",
            data: data3,
            symbolSize: 1,
            yAxisIndex: 0,
            symbol: "circle",
            smooth: true,
            showSymbol: false,
            emphasis: {
              focus: "series",
            },
            lineStyle: {
              width: 5,
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                {
                  offset: 0,
                  color: "#FA8072", // 线条开始颜色
                },
                {
                  offset: 1,
                  color: "#FF4500", // 线条结束颜色
                },
              ]),
              shadowColor: "rgba(255,69,0, 0.3)",
              shadowBlur: 10,
              shadowOffsetY: 20,
            },
            itemStyle: {
              normal: {
                color: colorList[2],
                borderColor: colorList[2],
              },
            },
            markPoint: {
              symbol: "pin", //标记(气泡)的图形
              symbolSize: 50, //标记(气泡)的大小
              itemStyle: {
                // color: '#4587E7', //图形的颜色。
                borderColor: "#FF4500", //图形的描边颜色。支持的颜色格式同 color，不支持回调函数。
                borderWidth: 0, //描边线宽。为 0 时无描边。
                borderType: "solid", //柱条的描边类型，默认为实线，支持 ‘solid’, ‘dashed’, ‘dotted’。
              },
              data: [
                { type: "max", name: "最大值" },
                { type: "min", name: "最小值" },
              ],
            },
            markLine: {
              data: [{ type: "average", name: "平均值" }],
            },
          },
        ],
      };
      myChart.setOption(option);
      var selectArr = myChart.getOption().legend[0].data;
      console.log(selectArr);
      let el = document.querySelector(".ffa");
      console.log(el);
      myChart.click(function () {
        console.log(111);
      });
      // el.addEventListener("change", function () {
      //   console.log(that.patrolProblem);
      // });

      // el.onchange(function () {
      //   var obj = {};
      //   for (var i = 0; i < that.myChartInstancetwo.length; i++) {
      //     if (that.myChartInstancetwo) {
      //       obj[selectArr[i]] = true;
      //     } else {
      //       obj[selectArr[i]] = false;
      //     }
      //   }
      //   option.legend.selected = obj;
      //   myChart.setOption(option);
      // });
    },
  },
};
</script>
<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
.add {
  position: absolute;
  right: 0%;
  top: 10%;
}
</style>