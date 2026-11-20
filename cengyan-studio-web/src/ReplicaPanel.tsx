import { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Empty, InputNumber, Modal, Progress, Space, Table, Tag, Tooltip, Typography, message } from 'antd';
import { RefreshCw } from 'lucide-react';
import type { CengyanApi, ReplicaPartition, ReplicaStatus } from './api';

const { Title, Text } = Typography;

const STAGE: Record<string, { text: string; color: string; tip?: string }> = {
  DONE: { text: '完成', color: 'green' },
  PENDING: { text: '排队', color: 'default', tip: '同时最多两个分片在同步，前面的完成后自动开始' },
  CREATED: { text: '新节点加入', color: 'processing', tip: '新节点作为只读副本加入分片，主副本在后台发送快照和日志' },
  CATCHING_UP: { text: '追平并转为投票成员', color: 'processing', tip: '新副本追上主副本后才会转为投票成员' },
  UPDATING_TOPOLOGY: { text: '切换路由', color: 'processing' },
  ROLLING_BACK: { text: '回滚中', color: 'warning', tip: '取消后把还没切换路由的分片恢复成原来的成员' },
  CANCELED: { text: '已回滚', color: 'default' },
  NOT_CHANGED: { text: '未改变', color: 'default' },
};

const JOB: Record<string, string> = { RUNNING: '同步中', CANCELING: '正在取消', DONE: '已完成', CANCELED: '已取消', FAILED: '失败' };

function syncPercent(row: ReplicaPartition) {
  if (!row.commitIndex || !row.match || !row.targets?.length) return null;
  const values = row.targets.map((id) => row.match?.[id] ?? 0);
  // 新副本还在收快照时 match 为 -1，没有日志位置，不能按日志算。
  if (values.some((value) => value <= 0)) return null;
  return Math.min(100, Math.floor((Math.min(...values) / row.commitIndex) * 100));
}

function formatBytes(bytes: number) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit++; }
  return `${unit === 0 ? value : value.toFixed(value < 10 ? 2 : 1)} ${units[unit]}`;
}

/** 快照传输进度，如"快照 3.2 GB / 6.1 GB"。新副本还没开始写或已经装完时返回 null。 */
function snapshotProgress(row: ReplicaPartition) {
  const received = row.targets?.map((id) => row.receivedBytes?.[id]).filter((value): value is number => typeof value === 'number') ?? [];
  if (!received.length) return null;
  const least = Math.min(...received);
  return row.snapshotBytes && row.snapshotBytes > 0
    ? `快照 ${formatBytes(least)} / ${formatBytes(row.snapshotBytes)}`
    : `快照已收 ${formatBytes(least)}`;
}

function progressText(row: ReplicaPartition) {
  const pct = syncPercent(row);
  if (pct !== null) return `${pct}%`;
  return snapshotProgress(row) ?? (row.targets?.length && row.match ? '等待快照' : null);
}

/** 副本数用"每个分片多保留几份"表示：0 表示只有一份数据。 */
export function ReplicaPanel({ api, graph, canWrite, active }: { api: CengyanApi; graph: string; canWrite: boolean; active: boolean }) {
  const [status, setStatus] = useState<ReplicaStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [target, setTarget] = useState<number | null>(null);
  const load = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true);
    try { setStatus(await api.replicaStatus(graph)); setError(''); }
    catch (reason) { setError(reason instanceof Error ? reason.message : '读取副本失败'); }
    finally { if (!quiet) setLoading(false); }
  }, [api, graph]);
  useEffect(() => { if (active) void load(); }, [active, load]);
  const job = status?.job;
  const running = Boolean(job && (job.state === 'RUNNING' || job.state === 'CANCELING'));
  // 作业进行中每 3 秒刷新一次进度。
  useEffect(() => {
    if (!active || !running) return undefined;
    const timer = window.setInterval(() => void load(true), 3000);
    return () => window.clearInterval(timer);
  }, [active, running, load]);

  const partitions = status?.partitions ?? [];
  const counts = partitions.map((p) => p.members.length);
  const minCopies = counts.length ? Math.min(...counts) : 1;
  const maxCopies = counts.length ? Math.max(...counts) : 1;
  const replicaCount = Math.max(0, minCopies - 1);
  useEffect(() => { setTarget(replicaCount); }, [replicaCount]);

  const add = () => {
    const replicas = target ?? replicaCount;
    if (replicas <= replicaCount) { setError('副本数只能增加。0 表示只有一份数据、没有副本。'); return; }
    Modal.confirm({
      title: `把副本数增加到 ${replicas}？`,
      content: `每个分片将保留 ${replicas + 1} 份数据。数据在后台逐个分片复制，同时最多两个分片，期间图空间可以正常读写；可以随时取消。`,
      okText: '开始',
      cancelText: '取消',
      onOk: async () => {
        setBusy(true); setError('');
        try { await api.addReplica(graph, replicas + 1); message.success('已开始在后台同步副本'); await load(true); }
        catch (reason) { setError(reason instanceof Error ? reason.message : '增加副本失败'); throw reason; }
        finally { setBusy(false); }
      },
    });
  };
  const cancel = () => Modal.confirm({
    title: '取消副本同步？',
    content: '还没切换完成的分片会回滚到原来的成员，已经完成的分片保留新副本。',
    okText: '取消同步', okButtonProps: { danger: true }, cancelText: '继续同步',
    onOk: async () => {
      try { await api.cancelReplica(graph); await load(true); }
      catch (reason) { setError(reason instanceof Error ? reason.message : '取消失败'); throw reason; }
    },
  });

  const working = partitions.filter((p) => !['DONE', 'PENDING', 'CANCELED', 'NOT_CHANGED'].includes(p.state));
  const percent = status && job ? Math.round(((status.done) / Math.max(1, status.parts)) * 100) : 0;
  return <div className="panel-stack">
    <div className="panel-toolbar">
      <div>
        <Title level={4}>副本管理</Title>
        <Text type="secondary">
          {status ? `当前副本数 ${replicaCount}${maxCopies !== minCopies ? `（各分片 ${minCopies - 1}～${maxCopies - 1} 不一致）` : ''}，分片 ${status.parts}。` : '打开后读取这个图空间的副本。'}
          0 表示只有一份数据、没有副本，最多 5。增加副本在后台复制数据，期间可以正常读写。
        </Text>
      </div>
      <Space>
        {canWrite && <>
          <InputNumber min={0} max={5} precision={0} value={target ?? replicaCount} disabled={running}
            onChange={(value) => { const next = typeof value === 'number' ? value : Number(value); setTarget(Number.isFinite(next) ? Math.min(5, Math.max(0, Math.trunc(next))) : 0); }} />
          <Tooltip title={running ? '有副本任务正在进行' : ''}>
            <Button type="primary" loading={busy} disabled={running || (target ?? replicaCount) <= replicaCount} onClick={add}>增加副本</Button>
          </Tooltip>
        </>}
        <Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load()} loading={loading}>刷新</Button>
      </Space>
    </div>
    {error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}
    {job && running && <Alert type={job.state === 'CANCELING' ? 'warning' : 'info'} showIcon
      message={`${JOB[job.state]}：副本数 ${job.previous - 1} → ${job.replicas - 1}，已完成 ${status?.done ?? 0}/${status?.parts ?? job.parts} 个分片`}
      description={<Space direction="vertical" style={{ width: '100%' }}>
        <Progress percent={percent} size="small" status="active" />
        {working.length > 0 && <Text type="secondary">正在处理：{working.map((p) => `分片 ${p.part}（${STAGE[p.state]?.text ?? p.state}${progressText(p) ? ` ${progressText(p)}` : ''}）`).join('、')}</Text>}
        {working.filter((p) => p.error).map((p) => <Text key={p.part} type="warning">分片 {p.part} 最近一次出错，会自动重试：{p.error}</Text>)}
      </Space>}
      action={canWrite && job.state === 'RUNNING' && <Button size="small" danger onClick={cancel}>取消</Button>} />}
    {job && !running && <Alert closable type={job.state === 'DONE' ? 'success' : job.state === 'FAILED' ? 'error' : 'info'} showIcon
      message={`上一次副本调整${JOB[job.state] ?? job.state}`} description={job.message || undefined} />}
    {partitions.length ? <Table rowKey="part" loading={loading} pagination={false} dataSource={partitions} size="middle"
      columns={[
        { title: '分片', dataIndex: 'part', width: 90 },
        { title: '数据所在节点', dataIndex: 'members', render: (members: string[]) => <Space size={[4, 4]} wrap>{members.map((id) => <Tag key={id}>{id}</Tag>)}</Space> },
        { title: '份数', key: 'copies', width: 80, render: (_, row) => row.members.length },
        { title: '状态', key: 'state', width: 260, render: (_, row) => {
          const stage = STAGE[row.state] ?? { text: row.state, color: 'default' };
          const progress = progressText(row);
          return <Space size={4} wrap>
            <Tooltip title={stage.tip}><Tag color={stage.color}>{stage.text}</Tag></Tooltip>
            {row.targets?.length ? <Text type="secondary">→ {row.targets.join('、')}</Text> : null}
            {progress && <Text type="secondary">{progress}</Text>}
            {row.error && <Tooltip title={row.error}><Tag color="orange">重试中</Tag></Tooltip>}
          </Space>;
        } },
      ]} /> : !error && <Empty description={loading ? '正在读取副本' : '没有分片'} />}
  </div>;
}
