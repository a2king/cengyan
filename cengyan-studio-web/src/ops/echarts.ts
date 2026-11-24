// 只注册运维页用到的部分，整个模块在打开运维页时才按需加载。
import * as echarts from 'echarts/core';
import { LineChart } from 'echarts/charts';
import { AxisPointerComponent, GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

echarts.use([LineChart, GridComponent, TooltipComponent, LegendComponent, AxisPointerComponent, CanvasRenderer]);

export { echarts };
