import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Button, Card, Empty, Segmented, Select, Spin, Typography } from 'antd';
import { ArrowLeft } from 'lucide-react';
import type { ECharts, EChartsCoreOption } from 'echarts/core';
import type { CengyanApi } from '../api';
import type { HostMetric, OpsHostSeries, OpsRange, OpsSeries } from '../types';
import { bytesText, escapeHtml, percentText, rateText } from './format';

const { Text } = Typography;

/** 分类色，按固定顺序分给服务器（对比图）或指标（单机图），不循环生成新色。 */
const SERIES_COLORS = ['#0072B2', '#E69F00', '#009E73', '#CC79A7', '#56B4E9', '#D55E00', '#6B7280', '#8C6D1F'];
const INK = '#24304a';
const MUTED = '#7b849d';
const LINE = '#e7eaf3';
const GRID = '#eef0f5';
const GROUP = 'cengyan-ops-trend';

type Kind = 'ratio' | 'rate';
const METRICS: Array<{ key: HostMetric; title: string; kind: Kind }> = [
  { key: 'cpu', title: 'CPU 使用率', kind: 'ratio' },
  { key: 'memory', title: '内存占用', kind: 'ratio' },
  { key: 'diskUsed', title: '磁盘占用', kind: 'ratio' },
  { key: 'diskBusy', title: '磁盘繁忙', kind: 'ratio' },
  { key: 'diskRead', title: '磁盘读取', kind: 'rate' },
  { key: 'diskWrite', title: '磁盘写入', kind: 'rate' },
  { key: 'netIn', title: '网络流入', kind: 'rate' },
  { key: 'netOut', title: '网络流出', kind: 'rate' },
];

const RANGES: Array<{ label: string; value: OpsRange }> = [
  { label: '1 小时', value: '1h' },
  { label: '1 天', value: '1d' },
  { label: '7 天', value: '7d' },
];

const RANGE_HINT: Record<OpsRange, string> = {
  '1h': '15 秒一个点',
  '1d': '5 分钟平均值，悬停可看段内峰值',
  '7d': '30 分钟平均值，悬停可看段内峰值',
};

type Point = [number, number | null];

function valueText(kind: Kind, value: number | null | undefined) {
  if (value === null || value === undefined) return '—';
  return kind === 'ratio' ? percentText(value) : rateText(value);
}

function axisText(kind: Kind, value: number) {
  return kind === 'ratio' ? `${Math.round(value * 100)}%` : `${bytesText(value)}/s`;
}

/** 两个点隔得太久（Studio 停过、节点没采到）就断开，不画一条假的直线。 */
function points(host: OpsHostSeries, values: Array<number | null> | undefined, stepMs: number): Point[] {
  const out: Point[] = [];
  host.ts.forEach((ts, index) => {
    if (index > 0 && ts - host.ts[index - 1] > stepMs * 2.5) out.push([host.ts[index - 1] + stepMs, null]);
    out.push([ts, values?.[index] ?? null]);
  });
  return out;
}

function tooltipTime(ts: number, range: OpsRange) {
  const date = new Date(ts);
  return range === '1h' ? date.toLocaleTimeString() : date.toLocaleString();
}

type AxisParam = { seriesId?: string; seriesName?: string; marker?: string; value?: unknown; dataIndex: number; axisValue?: number };

/** 悬停行：色块标身份，文字一律用正文色。汇总档带上段内峰值。 */
function tooltip(range: OpsRange, rows: (param: AxisParam) => { kind: Kind; peak?: number | null } | undefined) {
  return (raw: unknown) => {
    const params = (Array.isArray(raw) ? raw : [raw]) as AxisParam[];
    if (!params.length) return '';
    const head = `<div style="font-weight:600;margin-bottom:6px;color:${INK}">${escapeHtml(tooltipTime(Number(params[0].axisValue ?? (params[0].value as Point)[0]), range))}</div>`;
    const body = params.map((param) => {
      const meta = rows(param);
      if (!meta) return '';
      const value = (param.value as Point | undefined)?.[1];
      const peak = meta.peak !== undefined && meta.peak !== null ? `<span style="color:${MUTED};margin-left:8px">峰值 ${escapeHtml(valueText(meta.kind, meta.peak))}</span>` : '';
      return `<div style="display:flex;align-items:center;gap:12px;justify-content:space-between;color:${INK}">`
        + `<span>${param.marker || ''}${escapeHtml(param.seriesName || '')}</span>`
        + `<span style="font-variant-numeric:tabular-nums"><b>${escapeHtml(valueText(meta.kind, value))}</b>${peak}</span></div>`;
    }).join('');
    return head + body;
  };
}

const baseTooltip = {
  trigger: 'axis',
  confine: true,
  axisPointer: { type: 'line', lineStyle: { color: MUTED, width: 1 } },
  backgroundColor: '#fff',
  borderColor: LINE,
  borderWidth: 1,
  padding: [8, 12],
  textStyle: { color: INK, fontSize: 12 },
  extraCssText: 'box-shadow:0 8px 24px rgba(35,48,86,.12);border-radius:8px;',
};

function timeAxis(series: OpsSeries) {
  return {
    type: 'time',
    min: series.from,
    max: series.to,
    axisLine: { lineStyle: { color: LINE } },
    axisTick: { show: false },
    axisLabel: { color: MUTED, fontSize: 11, hideOverlap: true },
    splitLine: { show: false },
  };
}

function valueAxis(kind: Kind, extra: Record<string, unknown> = {}) {
  return {
    type: 'value',
    min: 0,
    ...(kind === 'ratio' ? { max: 1 } : {}),
    splitNumber: 3,
    axisLabel: { color: MUTED, fontSize: 11, formatter: (value: number) => axisText(kind, value) },
    splitLine: { lineStyle: { color: GRID } },
    ...extra,
  };
}

function lineSeries(id: string, name: string, color: string, data: Point[], extra: Record<string, unknown> = {}) {
  return {
    id,
    name,
    type: 'line',
    data,
    color,
    showSymbol: false,
    symbolSize: 8,
    connectNulls: false,
    triggerEvent: 'line',
    lineStyle: { width: 2 },
    emphasis: { focus: 'series', lineStyle: { width: 2 } },
    ...extra,
  };
}

/** 一张对比小图：一项指标，每台服务器一条线。 */
function compareOption(series: OpsSeries, metric: (typeof METRICS)[number]): EChartsCoreOption {
  const peaks = new Map<string, Point[]>();
  series.hosts.forEach((host) => { if (host.max) peaks.set(host.id, points(host, host.max[metric.key], series.stepMs)); });
  return {
    animation: false,
    grid: { left: 4, right: 14, top: 10, bottom: 4, outerBoundsMode: 'same', outerBoundsContain: 'axisLabel' },
    tooltip: {
      ...baseTooltip,
      formatter: tooltip(series.range, (param) => ({ kind: metric.kind, peak: peaks.get(param.seriesId || '')?.[param.dataIndex]?.[1] })),
    },
    xAxis: timeAxis(series),
    yAxis: valueAxis(metric.kind),
    series: series.hosts.map((host, index) => lineSeries(host.id, host.name, SERIES_COLORS[index % SERIES_COLORS.length],
      points(host, host.avg[metric.key], series.stepMs))),
  };
}

/** 单台服务器：左轴是比例，右轴是速率。实线读左轴，虚线读右轴。 */
function hostOption(series: OpsSeries, host: OpsHostSeries): EChartsCoreOption {
  return {
    animation: false,
    legend: { top: 0, left: 0, itemWidth: 18, itemHeight: 8, itemGap: 16, textStyle: { color: INK, fontSize: 12 } },
    grid: { left: 8, right: 8, top: 44, bottom: 4, outerBoundsMode: 'same', outerBoundsContain: 'axisLabel' },
    tooltip: {
      ...baseTooltip,
      formatter: tooltip(series.range, (param) => {
        const metric = METRICS.find((item) => item.key === param.seriesId);
        if (!metric) return undefined;
        return { kind: metric.kind, peak: host.max ? points(host, host.max[metric.key], series.stepMs)[param.dataIndex]?.[1] : undefined };
      }),
    },
    xAxis: timeAxis(series),
    yAxis: [
      valueAxis('ratio', { name: '占比', nameTextStyle: { color: MUTED, fontSize: 11 }, position: 'left' }),
      valueAxis('rate', { name: '速率', nameTextStyle: { color: MUTED, fontSize: 11 }, position: 'right', splitLine: { show: false } }),
    ],
    series: METRICS.map((metric, index) => lineSeries(metric.key, metric.title, SERIES_COLORS[index],
      points(host, host.avg[metric.key], series.stepMs), {
        yAxisIndex: metric.kind === 'ratio' ? 0 : 1,
        lineStyle: { width: 2, type: metric.kind === 'ratio' ? 'solid' : 'dashed' },
      })),
  };
}

type EchartsModule = typeof import('./echarts');

/** ECharts 按需加载；容器尺寸变化时跟着重排。 */
function EChart({ option, height, group, onPick }: { option: EChartsCoreOption; height: number; group?: string; onPick?: (seriesId: string) => void }) {
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<ECharts | null>(null);
  const pick = useRef(onPick);
  pick.current = onPick;
  const [lib, setLib] = useState<EchartsModule | null>(null);
  useEffect(() => {
    let live = true;
    void import('./echarts').then((module) => { if (live) setLib(module); });
    return () => { live = false; };
  }, []);
  useEffect(() => {
    if (!lib || !box.current) return;
    const instance = lib.echarts.init(box.current);
    chart.current = instance;
    if (group) {
      instance.group = group;
      lib.echarts.connect(group);
    }
    instance.on('click', (params) => {
      const id = (params as { seriesId?: string }).seriesId;
      if (id) pick.current?.(id);
    });
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(box.current);
    return () => {
      observer.disconnect();
      instance.dispose();
      chart.current = null;
    };
  }, [lib, group]);
  useEffect(() => {
    chart.current?.setOption(option, { replaceMerge: ['series'] });
  }, [lib, option]);
  return <div ref={box} style={{ height, width: '100%' }}>{!lib && <div className="ops-chart-pending"><Spin size="small" /></div>}</div>;
}

export function TrendPanel({ api, refreshKey }: { api: CengyanApi; refreshKey: number }) {
  const [range, setRange] = useState<OpsRange>(() => {
    const saved = localStorage.getItem('cengyan.ops.range');
    return saved === '1d' || saved === '7d' ? saved : '1h';
  });
  const [hostId, setHostId] = useState('');
  const [series, setSeries] = useState<OpsSeries | null | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const request = useRef(0);
  const load = useCallback(async () => {
    const ticket = ++request.current;
    setLoading(true);
    try {
      const next = await api.opsSeries(range);
      if (ticket !== request.current) return;
      setSeries(next);
      setError('');
    } catch (reason) {
      if (ticket === request.current) setError(reason instanceof Error ? reason.message : '趋势数据读取失败');
    } finally {
      if (ticket === request.current) setLoading(false);
    }
  }, [api, range]);
  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), range === '1h' ? 30_000 : 300_000);
    return () => window.clearInterval(timer);
  }, [load, range]);
  useEffect(() => { if (refreshKey > 0) void load(); }, [refreshKey]); // eslint-disable-line react-hooks/exhaustive-deps

  const hosts = series?.hosts || [];
  const host = hosts.find((item) => item.id === hostId);
  useEffect(() => { if (hostId && series && !host) setHostId(''); }, [hostId, series, host]);
  const hasData = hosts.some((item) => item.ts.length > 0);
  const compareOptions = useMemo(() => series ? METRICS.map((metric) => ({ metric, option: compareOption(series, metric) })) : [], [series]);
  const singleOption = useMemo(() => series && host ? hostOption(series, host) : null, [series, host]);
  const changeRange = (value: OpsRange) => {
    localStorage.setItem('cengyan.ops.range', value);
    setSeries(undefined);
    setRange(value);
  };

  const controls = <div className="ops-trend-controls">
    <Segmented<OpsRange> size="small" options={RANGES} value={range} onChange={changeRange} />
    <Select size="small" className="ops-host-select" value={hostId} onChange={setHostId} disabled={!hosts.length}
      options={[{ label: '全部服务器对比', value: '' }, ...hosts.map((item) => ({ label: item.address && item.address !== item.name ? `${item.name}（${item.address}）` : item.name, value: item.id }))]} />
  </div>;

  let body;
  if (series === undefined) body = <div className="ops-trend-empty"><Spin /></div>;
  else if (series === null) body = <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="Studio 没有托管集群，不记录服务器趋势" />;
  else if (!hasData) body = <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="这个时间范围内还没有采集记录" />;
  else if (host && singleOption) {
    body = <>
      <div className="ops-trend-sub">
        <Button size="small" type="text" icon={<ArrowLeft size={14} />} onClick={() => setHostId('')}>全部服务器</Button>
        <Text type="secondary">实线读左轴（占比），虚线读右轴（速率）· {RANGE_HINT[range]}</Text>
      </div>
      <EChart option={singleOption} height={380} />
    </>;
  } else {
    body = <>
      <div className="ops-trend-sub">
        <div className="ops-legend" role="list">
          {hosts.map((item, index) => <button key={item.id} type="button" role="listitem" className="ops-legend-item" title="只看这台服务器" onClick={() => setHostId(item.id)}>
            <span className="ops-legend-swatch" style={{ background: SERIES_COLORS[index % SERIES_COLORS.length] }} />
            <span>{item.name}</span>
          </button>)}
        </div>
        <Text type="secondary">{RANGE_HINT[range]} · 点线条或名称只看一台</Text>
      </div>
      <div className="ops-trend-grid">
        {compareOptions.map(({ metric, option }) => <div key={metric.key} className="ops-trend-cell">
          <div className="ops-trend-title">{metric.title}</div>
          <EChart option={option} height={150} group={GROUP} onPick={setHostId} />
        </div>)}
      </div>
    </>;
  }

  return <Card className="workspace-card ops-trend-card" bordered={false} title="服务器趋势" extra={controls} loading={false}>
    {error && <Alert className="ops-inline-alert" type="warning" showIcon message="趋势数据暂不可用" description={error} />}
    <Spin spinning={loading && series !== undefined} size="small">{body}</Spin>
  </Card>;
}
