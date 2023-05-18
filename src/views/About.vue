
<template>
  <div style="width: 100vw; height: 100vh">
    <div ref="about" class="about" style="width: 500px; height: 600px"></div>

    <el-button type="primary" @click="add"> dada</el-button>
  </div>
</template>

<script>
import * as echarts from "echarts";
export default {
  data() {
    return {
      myChart: null,
    };
  },
  mounted() {
    this.myChart2();
  },
  methods: {
    // 初始化图表
    myChart2() {
      this.myChart = echarts.init(this.$refs.about);
      this.myChart.setOption({
        title: {
          text: "ECharts 入门示例",
        },
        tooltip: {},
        legend: {
          data: ["销量"],
        },
        xAxis: {
          data: ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"],
        },
        yAxis: {},
        series: [
          {
            name: "销量",
            type: "bar",
            data: [5, 20, 36, 10, 10, 20],
          },
        ],
      });
    },
    add() {
      //控制 销量显示隐藏
      
    },
  },
};
</script>
