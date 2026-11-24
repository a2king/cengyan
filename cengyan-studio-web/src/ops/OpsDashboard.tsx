import { useCallback, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Alert, Button, Card, Switch, Table, Tag, Typography } from 'antd';
import type { TableProps } from 'antd';
import type { CengyanApi } from '../api';
import type { OpsOverview } from '../types';
import { bytesText, identityText, numberValue, opsLabel, opsTag, recordValue, timeText } from './format';
import { TrendPanel } from './TrendPanel';

const { Text } = Typography;

/** 采集 15 秒一轮；超过这个时间没有新一轮，说明 Studio 的采集停了。 */
const STALE_MS = 75_000;

type Row = Record<string, unknown>;
type NodeRow = Row & { _server: string; _span: number };

function severity(state: unknown) {
  switch (String(state || '')) {
    case 'UNAVAILABLE': case 'BAD': return 0;
    case 'DEGRADED': case 'CONCERNING': return 1;
    default: return 2;
  }
}

function serverKey(row: Row) {
  return String(row.serverId || row.hostname || row.host || row.nodeId || '');
}

/** 同一台服务器的节点排在一起：Storage 在前，服务器单元格合并。 */
function groupByServer(storage: Row[], compute: Row[]): NodeRow[] {
  const groups = new Map<string, Row[]>();
  [...storage, ...compute].forEach((row) => {
    const key = serverKey(row);
    groups.set(key, [...(groups.get(key) || []), row]);
  });
  // 与趋势图图例同序：按服务器编号（server-1、server-2…），没有编号时按主机名。
  const ordered = [...groups.entries()].sort(([a], [b]) => a.localeCompare(b, 'zh-CN', { numeric: true }));
  return ordered.flatMap(([key, rows]) => rows.map((row, index) => ({ ...row, _server: key, _span: index === 0 ? rows.length : 0 })));
}

function loadText(row: Row) {
  if (row.role === 'COMPUTE') {
    const slots = recordValue(row.slots);
    const active = numberValue(slots?.active);
    const capacity = numberValue(slots?.capacity);
    const queued = numberValue(row.queuePending);
    return `执行 ${active ?? '—'} / ${capacity ?? '—'} · 排队 ${queued ?? '—'}`;
  }
  const active = numberValue(row.writeActive);
  const queued = numberValue(row.writeQueued);
  return active === undefined && queued === undefined ? '—' : `写入 ${active ?? '—'} · 排队 ${queued ?? '—'}`;
}

function nodeColumns(leader: string): NonNullable<TableProps<NodeRow>['columns']> {
  return [
    {
      title: '服务器', key: 'server', width: 190,
      onCell: (row) => ({ rowSpan: row._span }),
      render: (_, row) => <div className="ops-server-cell">
        <span className="ops-server-name" title={identityText(row.hostname)}>{identityText(row.hostname)}</span>
        {row.host && row.host !== row.hostname ? <Text type="secondary" className="ops-server-addr">{String(row.host)}</Text> : null}
      </div>,
    },
    {
      title: '节点', key: 'node', width: 220,
      render: (_, row) => <span className="ops-node-cell">
        <span className="code-pill" title={identityText(row.nodeId)}>{identityText(row.nodeId)}</span>
        <Tag className="ops-role-tag" color={row.role === 'COMPUTE' ? 'geekblue' : 'cyan'}>{row.role === 'COMPUTE' ? 'Compute' : 'Storage'}</Tag>
        {row.role !== 'COMPUTE' && leader && row.nodeId === leader ? <Tag className="ops-role-tag">Metadata Leader</Tag> : null}
      </span>,
    },
    { title: '状态', key: 'state', width: 96, render: (_, row) => <span title={identityText(row.message)}>{opsTag(row.state)}</span> },
    { title: '健康', key: 'health', width: 84, render: (_, row) => opsTag(row.health) },
    { title: '堆内存', key: 'heap', className: 'ops-num-col', width: 170, render: (_, row) => `${bytesText(row.heapUsedBytes)} / ${bytesText(row.heapMaxBytes)}` },
    { title: '线程', key: 'threads', className: 'ops-num-col', width: 72, render: (_, row) => numberValue(row.threadCount) ?? '—' },
    { title: '负载', key: 'load', className: 'ops-num-col', width: 190, render: (_, row) => loadText(row) },
    { title: '最近成功', key: 'success', className: 'ops-num-col', width: 180, render: (_, row) => timeText(row.successAt) },
  ];
}

const shardColumns: TableProps<Row>['columns'] = [
  { title: '图', dataIndex: 'graph', render: (value: unknown, row) => identityText(value || row.graphId) },
  { title: '分片', dataIndex: 'partId', width: 80 },
  { title: 'Leader', dataIndex: 'leaderId', render: (value: unknown) => identityText(value) },
  { title: 'Term', dataIndex: 'term', width: 90 },
  { title: 'Applied Index', dataIndex: 'appliedIndex', width: 140 },
  { title: '副本', render: (_, row) => Array.isArray(row.members) ? (row.members as unknown[]).join(', ') : '—' },
  { title: '状态', width: 100, render: (_, row) => opsTag(row.state) },
];

function Stat({ label, children, hint }: { label: string; children: ReactNode; hint?: ReactNode }) {
  return <div className="ops-stat">
    <div className="ops-stat-label">{label}</div>
    <div className="ops-stat-value">{children}</div>
    {hint ? <div className="ops-stat-hint">{hint}</div> : null}
  </div>;
}

export function OpsDashboard({ api, bindRefresh }: { api: CengyanApi; bindRefresh: (refresh: () => void, refreshing: boolean) => void }) {
  const [data, setData] = useState<OpsOverview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [refreshKey, setRefreshKey] = useState(0);
  const [shardsOpen, setShardsOpen] = useState(false);
  const [shards, setShards] = useState<Row[] | null>(null);
  const [shardError, setShardError] = useState('');
  const [onlyBad, setOnlyBad] = useState(true);
  const [now, setNow] = useState(() => Date.now());

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await api.opsOverview()); setError(''); }
    catch (reason) { setError(reason instanceof Error ? reason.message : '集群运维数据读取失败'); }
    finally { setLoading(false); setNow(Date.now()); }
  }, [api]);
  const loadShards = useCallback(async () => {
    try { setShards(await api.opsShards()); setShardError(''); }
    catch (reason) { setShardError(reason instanceof Error ? reason.message : '分片拓扑读取失败'); }
  }, [api]);
  useEffect(() => { void load(); const timer = window.setInterval(() => void load(), 15_000); return () => window.clearInterval(timer); }, [load]);
  useEffect(() => {
    bindRefresh(() => {
      void load();
      setRefreshKey((key) => key + 1);
      if (shardsOpen) void loadShards();
    }, loading);
  }, [bindRefresh, load, loadShards, loading, shardsOpen]);
  // 未托管集群时 Compute 汇总直接带分片；托管集群展开时才去取。
  const inlineShards = Array.isArray(data?.shards) && !data?.shardSummary ? data.shards : null;
  useEffect(() => { if (shardsOpen && !inlineShards) void loadShards(); }, [shardsOpen, inlineShards, loadShards, data?.collectedAt]);

  const storageNodes = data?.storageNodes || [];
  const computeNodes = data?.computeNodes || [];
  const rows = useMemo(() => groupByServer(storageNodes, computeNodes), [storageNodes, computeNodes]);
  const leader = String(data?.metadataLeaderId || storageNodes.find((row) => row.metadataLeaderId)?.metadataLeaderId || '');
  const columns = useMemo(() => nodeColumns(leader), [leader]);

  const reachable = (list: Row[]) => list.filter((row) => row.state === 'REACHABLE').length;
  const summary = data?.shardSummary || (inlineShards ? {
    total: inlineShards.length,
    healthy: inlineShards.filter((row) => !row.state || row.state === 'HEALTHY').length,
    degraded: inlineShards.filter((row) => row.state === 'DEGRADED').length,
    unavailable: inlineShards.filter((row) => row.state === 'UNAVAILABLE').length,
  } : undefined);
  const slots = computeNodes.reduce<{ active: number; capacity: number; queued: number }>((acc, row) => {
    const slot = recordValue(row.slots);
    return {
      active: acc.active + (numberValue(slot?.active) ?? 0),
      capacity: acc.capacity + (numberValue(slot?.capacity) ?? 0),
      queued: acc.queued + (numberValue(row.queuePending) ?? 0),
    };
  }, { active: 0, capacity: 0, queued: 0 });
  const stale = Boolean(data?.collectedAt && now - Number(data.collectedAt) > STALE_MS);
  const shardRows = useMemo(() => {
    const list = [...(inlineShards || shards || [])].sort((a, b) => severity(a.state) - severity(b.state));
    return onlyBad ? list.filter((row) => severity(row.state) < 2) : list;
  }, [inlineShards, shards, onlyBad]);
  const badShards = (summary?.degraded || 0) + (summary?.unavailable || 0);

  return <div className="page-stack workspace-page ops-dashboard">
    {error && <Alert type="warning" showIcon message="集群运维数据暂不可用" description={error} closable onClose={() => setError('')} />}
    {data?.message && <Alert type="warning" showIcon message={data.message} />}
    <Card className="workspace-card ops-status-card" bordered={false}>
      <div className="ops-status-strip">
        <Stat label="集群状态">{opsTag(data?.clusterHealth)}</Stat>
        <Stat label="节点可达" hint={`Storage ${reachable(storageNodes)}/${storageNodes.length} · Compute ${reachable(computeNodes)}/${computeNodes.length}`}>
          {reachable(storageNodes) + reachable(computeNodes)} / {storageNodes.length + computeNodes.length}
        </Stat>
        <Stat label="分片正常" hint={summary && badShards > 0
          ? <Text type="danger">{[summary.degraded ? `${summary.degraded} 个缺副本` : '', summary.unavailable ? `${summary.unavailable} 个不可用` : ''].filter(Boolean).join(' · ')}</Text>
          : summary?.total ? '全部正常' : undefined}>
          {summary ? `${summary.healthy} / ${summary.total}` : '—'}
        </Stat>
        <Stat label="查询执行" hint={`排队 ${slots.queued}`}>{slots.active} / {slots.capacity}</Stat>
        <Stat label="Metadata Leader"><span className="ops-stat-text">{identityText(leader)}</span></Stat>
        <Stat label="最近采集" hint={stale ? <Text type="warning">超过 75 秒没有新数据</Text> : '15 秒一轮，记录保留 7 天'}>
          <span className="ops-stat-text">{data?.collectedAt ? new Date(Number(data.collectedAt)).toLocaleTimeString() : '—'}</span>
        </Stat>
      </div>
    </Card>
    <TrendPanel api={api} refreshKey={refreshKey} />
    <Card className="workspace-card" bordered={false} title="节点">
      <Table<NodeRow> className="ops-resource-table ops-node-table" size="small" rowKey={(row) => String(row.nodeId)} pagination={false}
        dataSource={rows} columns={columns} scroll={{ x: 1100 }} locale={{ emptyText: '还没有节点运维数据' }} />
    </Card>
    <Card className="workspace-card ops-shard-card" bordered={false} title="分片拓扑"
      extra={<div className="ops-shard-extra">
        {summary ? <Text type={badShards ? 'danger' : 'secondary'}>{summary.total} 个分片{badShards ? `，${badShards} 个异常` : '，全部正常'}</Text> : null}
        <Button size="small" onClick={() => setShardsOpen((open) => !open)}>{shardsOpen ? '收起' : '查看明细'}</Button>
      </div>}>
      {shardsOpen && <>
        {shardError && <Alert className="ops-inline-alert" type="warning" showIcon message="分片拓扑暂不可用" description={shardError} />}
        <div className="ops-shard-filter"><Switch size="small" checked={onlyBad} onChange={setOnlyBad} /><Text type="secondary">只看异常分片</Text></div>
        <Table<Row> size="small" rowKey={(row) => `${row.graphId}-${row.partId}`} loading={!inlineShards && shards === null && !shardError}
          pagination={{ pageSize: 20, hideOnSinglePage: true }} dataSource={shardRows} columns={shardColumns}
          locale={{ emptyText: onlyBad ? '没有异常分片' : '没有分片拓扑数据' }} />
      </>}
    </Card>
    <Card className="workspace-card" bordered={false} title="最近记录">
      <Table size="small" rowKey={(row) => `${row.ts}-${row.event}-${row.nodeId}-${row.message}`} pagination={{ pageSize: 10, hideOnSinglePage: true }}
        dataSource={data?.events || []}
        columns={[
          { title: '时间', render: (_, row) => timeText(row.ts), width: 180 },
          { title: '级别', width: 80, render: (_, row) => <span title={opsLabel(row.level)}>{opsTag(row.level)}</span> },
          { title: '节点', dataIndex: 'nodeId', width: 140, render: (value: unknown) => identityText(value) },
          { title: '说明', dataIndex: 'message' },
        ]}
        locale={{ emptyText: '最近 7 天还没有状态变化' }} />
    </Card>
  </div>;
}
