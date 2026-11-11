import { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Button, Card, Drawer, Input, InputNumber, List, Modal, Segmented, Select, Space, Table, Tag, Tooltip, Typography, message } from 'antd';
import type { TableProps } from 'antd';
import { CircleHelp, History, KeyRound, Save, Undo2 } from 'lucide-react';
import { errorText, timeText } from '../clusterApi';
import type { Catalog, CatalogEntry, Change, ConfigState, ConfigVersion, Role } from '../clusterApi';
import type { ConsoleContext } from './ClusterConsole';
import { configConflicts } from './configConflicts';

const { Text, Paragraph } = Typography;
type Draft = Pick<ConfigVersion, 'roleDefaults' | 'nodeOverrides'>;
type Row = CatalogEntry & { value: string; source: 'default' | 'role' | 'node'; inherited: string };

function bytesHint(value: string) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return '';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `≈ ${Number.isInteger(v) ? v : v.toFixed(1)} ${units[i]}`;
}

export function ConfigTab({ ctx }: { ctx: ConsoleContext }) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [config, setConfig] = useState<ConfigState | null>(null);
  const [draft, setDraft] = useState<Draft>({ roleDefaults: {}, nodeOverrides: {} });
  const [role, setRole] = useState<Role>('STORAGE');
  const [scope, setScope] = useState('role');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loadError, setLoadError] = useState('');
  const [saving, setSaving] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [note, setNote] = useState('');

  const load = useCallback(async () => {
    try {
      const [c, s] = await Promise.all([ctx.api.catalog(), ctx.api.config()]);
      setCatalog(c); setConfig(s);
      setDraft({ roleDefaults: structuredClone(s.current.roleDefaults), nodeOverrides: structuredClone(s.current.nodeOverrides) });
      setErrors({}); setLoadError('');
    } catch (reason) { setLoadError(errorText(reason)); }
  }, [ctx.api]);
  useEffect(() => { void load(); }, [load]);

  const nodes = ctx.overview.nodes.filter((n) => n.role === role);
  useEffect(() => { if (scope !== 'role' && !nodes.some((n) => n.id === scope)) setScope('role'); }, [nodes, scope]);
  const dirty = config ? JSON.stringify(draft) !== JSON.stringify({ roleDefaults: config.current.roleDefaults, nodeOverrides: config.current.nodeOverrides }) : false;

  const rows = useMemo<Row[]>(() => (catalog?.[role] || []).map((entry) => {
    const roleValue = draft.roleDefaults[role]?.[entry.key];
    const nodeValue = scope === 'role' ? undefined : draft.nodeOverrides[scope]?.[entry.key];
    const inherited = roleValue ?? entry.defaultValue;
    const value = nodeValue ?? inherited;
    return { ...entry, value, inherited, source: nodeValue !== undefined ? 'node' : roleValue !== undefined ? 'role' : 'default' };
  }), [catalog, draft, role, scope]);
  const conflicts = useMemo(() => configConflicts(role, rows), [role, rows]);

  const change = (entry: CatalogEntry, value: string) => setDraft((current) => {
    const next = structuredClone(current);
    if (scope === 'role') {
      const values = { ...(next.roleDefaults[role] || {}) };
      if (value === entry.defaultValue) delete values[entry.key]; else values[entry.key] = value;
      next.roleDefaults[role] = values;
    } else {
      const values = { ...(next.nodeOverrides[scope] || {}) };
      const inherited = next.roleDefaults[role]?.[entry.key] ?? entry.defaultValue;
      if (value === inherited) delete values[entry.key]; else values[entry.key] = value;
      if (Object.keys(values).length) next.nodeOverrides[scope] = values; else delete next.nodeOverrides[scope];
    }
    return next;
  });

  const save = async () => {
    setSaving(true); setErrors({});
    try {
      const saved = await ctx.api.saveConfig(draft.roleDefaults, draft.nodeOverrides, note);
      message.success(`已保存为配置 v${saved.version}`);
      setNote('');
      await Promise.all([load(), ctx.refresh()]);
    } catch (reason) {
      const details = (reason as { details?: Record<string, string> }).details || {};
      setErrors(details);
      Modal.error({ title: '保存失败', content: Object.keys(details).length ? `${Object.keys(details).length} 项参数未通过校验，已在表格中标出` : errorText(reason) });
    } finally { setSaving(false); }
  };

  const errorFor = (key: string) => errors[`${scope === 'role' ? role : scope}.${key}`];
  const isModified = (row: Row) => scope === 'role' ? row.source === 'role' : row.source === 'node';
  const columns: TableProps<Row>['columns'] = [
    { title: '参数', key: 'key', width: 280, render: (_, row) => <span className="code-pill">{row.key}</span> },
    { title: '说明', key: 'description', width: 460, render: (_, row) => <Space size={8} wrap className="config-description">
      <Text>{row.label}</Text>
      <Tag color={isModified(row) ? 'blue' : 'default'}>{isModified(row) ? '已修改' : '未修改'}</Tag>
      {conflicts[row.key] && <Tooltip title={<div className="config-tooltip">
        <div>{conflicts[row.key].summary}</div>
        <div>冲突规则：</div>
        {conflicts[row.key].rules.map((rule) => <div key={rule}>{rule}</div>)}
      </div>}>
        <span className="config-conflict"><Tag color="orange">冲突失效</Tag></span>
      </Tooltip>}
      <Tooltip title={<div className="config-tooltip">
        <div>{row.description}</div>
        <div>默认值：{row.defaultValue || '空'}</div>
        {row.min != null && <div>取值范围：{row.min} ~ {row.max}</div>}
        {scope !== 'role' && <div>角色值：{row.inherited || '空'}</div>}
        <div>修改后需重启节点生效</div>
      </div>}>
        <span className="config-help" aria-label={`${row.label}详情`}><CircleHelp size={15} /></span>
      </Tooltip>
    </Space> },
    { title: '当前值', key: 'value', width: 300, render: (_, row) => {
      const error = errorFor(row.key);
      const editor = row.type === 'int' || row.type === 'bytes'
        ? <InputNumber<string> stringMode value={row.value} min={String(row.min ?? '')} max={String(row.max ?? '')} status={error ? 'error' : undefined}
            style={{ width: '100%' }} onChange={(v) => change(row, v == null ? '' : String(v))} />
        : <Input value={row.value} status={error ? 'error' : undefined} placeholder={row.type === 'label' ? '空表示不设置' : ''} onChange={(e) => change(row, e.target.value)} />;
      return <Space direction="vertical" size={2} style={{ width: '100%' }}>
        {editor}
        {row.type === 'bytes' && <Text type="secondary" className="config-meta">{bytesHint(row.value)}</Text>}
        {error && <Text type="danger" className="config-meta">{error}</Text>}
      </Space>;
    } },
    { title: '', key: 'reset', width: 90, render: (_, row) => {
      const canReset = scope === 'role' ? row.source === 'role' : row.source === 'node';
      return canReset && <Tooltip title={scope === 'role' ? '恢复为默认值' : '取消单独设置，使用角色值'}>
        <Button type="text" size="small" icon={<Undo2 size={14} />} onClick={() => change(row, scope === 'role' ? row.defaultValue : row.inherited)} />
      </Tooltip>;
    } },
  ];

  if (loadError) return <Alert type="error" showIcon message="配置读取失败" description={loadError} action={<Button size="small" onClick={() => void load()}>重试</Button>} />;
  if (!catalog || !config) return <Card loading bordered={false} />;
  return <div className="cluster-tab">
    <RootPasswordCard ctx={ctx} />
    <div className="cluster-tab-head">
      <Space wrap>
        <Segmented<Role> value={role} onChange={(v) => { setRole(v); setScope('role'); }} options={[{ value: 'STORAGE', label: '存储节点' }, { value: 'COMPUTE', label: '计算节点' }]} />
        <Select value={scope} onChange={setScope} style={{ minWidth: 260 }} options={[
          { value: 'role', label: `所有${role === 'STORAGE' ? '存储' : '计算'}节点（角色默认）` },
          ...nodes.map((n) => ({ value: n.id, label: `${n.id} · ${n.host}${draft.nodeOverrides[n.id] ? `（${Object.keys(draft.nodeOverrides[n.id]).length} 项单独设置）` : ''}` })),
        ]} />
      </Space>
      <Space>
        <Button icon={<History size={15} />} onClick={() => setHistoryOpen(true)}>历史版本</Button>
        <Button disabled={!dirty} onClick={() => void load()}>放弃修改</Button>
        <Input placeholder="修改说明（可选）" value={note} onChange={(e) => setNote(e.target.value)} style={{ width: 200 }} maxLength={80} disabled={!dirty} />
        <Button type="primary" icon={<Save size={15} />} disabled={!dirty} loading={saving} onClick={() => void save()}>保存</Button>
      </Space>
    </div>
    {scope !== 'role' && <Alert type="info" showIcon className="config-scope-note" message={`正在编辑 ${scope} 的单独配置`} description="这里修改的值只作用于这一个节点，其余参数沿用角色默认值。" />}
    {dirty && <Alert type="warning" showIcon className="config-scope-note" message="有未保存的修改" description="保存后相关节点会标记为待重启，在概览页或节点页执行滚动重启后生效。" />}
    <Table<Row> rowKey="key" columns={columns} dataSource={rows} pagination={false} scroll={{ x: 'max-content' }} />
    <HistoryDrawer ctx={ctx} open={historyOpen} current={config.current.version} onClose={() => setHistoryOpen(false)} onRolledBack={() => void load()} />
  </div>;
}

function HistoryDrawer({ ctx, open, current, onClose, onRolledBack }: { ctx: ConsoleContext; open: boolean; current: number; onClose: () => void; onRolledBack: () => void }) {
  const [versions, setVersions] = useState<ConfigVersion[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [changes, setChanges] = useState<Change[]>([]);
  const [error, setError] = useState('');
  useEffect(() => {
    if (!open) return;
    void ctx.api.history().then((list) => setVersions([...list, { version: 0, savedAt: 0, note: '初始配置（全部默认值）', roleDefaults: {}, nodeOverrides: {} }]))
      .catch((reason) => setError(errorText(reason)));
  }, [ctx.api, open, current]);
  useEffect(() => {
    if (selected == null) { setChanges([]); return; }
    void ctx.api.diff(selected, current).then(setChanges).catch((reason) => setError(errorText(reason)));
  }, [ctx.api, selected, current]);
  const rollback = (version: number) => Modal.confirm({
    title: `回滚到 v${version}？`,
    content: '会生成一个新版本，内容与所选版本相同。已删除节点的单独配置不会恢复。回滚后需要重启节点才能生效。',
    okText: '回滚',
    onOk: async () => {
      try { const saved = await ctx.api.rollback(version); message.success(`已回滚，当前为 v${saved.version}`); setSelected(null); onRolledBack(); await ctx.refresh(); }
      catch (reason) { Modal.error({ title: '回滚失败', content: errorText(reason) }); }
    },
  });
  return <Drawer title="配置历史" width={720} open={open} onClose={onClose}>
    {error && <Alert type="error" showIcon message={error} style={{ marginBottom: 12 }} />}
    <List dataSource={versions} renderItem={(v) => <List.Item className={selected === v.version ? 'history-selected' : ''}
      actions={[
        v.version !== current && <Button key="diff" size="small" onClick={() => setSelected(selected === v.version ? null : v.version)}>{selected === v.version ? '收起' : '与当前比较'}</Button>,
        v.version !== current && <Button key="rollback" size="small" onClick={() => rollback(v.version)}>回滚到此版本</Button>,
      ].filter(Boolean)}>
      <List.Item.Meta title={<Space>v{v.version}{v.version === current && <Tag color="green">当前</Tag>}</Space>}
        description={<Space direction="vertical" size={0}><Text type="secondary">{v.savedAt ? timeText(v.savedAt) : '—'}</Text>{v.note && <Text>{v.note}</Text>}</Space>} />
    </List.Item>} />
    {selected != null && <Card size="small" title={`v${selected} → v${current} 的变化`} className="diff-card">
      {changes.length === 0 ? <Text type="secondary">没有差异</Text> : <Table size="small" rowKey={(c) => `${c.scope}.${c.key}`} pagination={false} dataSource={changes} columns={[
        { title: '范围', dataIndex: 'scope', render: (s: string) => s === 'STORAGE' ? '存储节点' : s === 'COMPUTE' ? '计算节点' : s },
        { title: '参数', dataIndex: 'key', render: (k: string) => <span className="code-pill">{k}</span> },
        { title: `v${selected}`, dataIndex: 'before', render: (v: string) => v || <Text type="secondary">默认</Text> },
        { title: `v${current}`, dataIndex: 'after', render: (v: string) => v || <Text type="secondary">默认</Text> },
      ]} />}
    </Card>}
  </Drawer>;
}

function RootPasswordCard({ ctx }: { ctx: ConsoleContext }) {
  const [password, setPassword] = useState('');
  const [saving, setSaving] = useState(false);
  const set = ctx.overview.rootPasswordSet;
  const joinedCompute = ctx.overview.nodes.some((n) => n.role === 'COMPUTE' && n.joined);
  const save = async () => {
    setSaving(true);
    try { await ctx.api.setRootPassword(password); message.success('数据库 root 密码已更新'); setPassword(''); await ctx.refresh(); }
    catch (reason) { Modal.error({ title: '修改失败', content: errorText(reason) }); }
    finally { setSaving(false); }
  };
  return <Card className="info-card root-card" bordered={false}>
    <div className="info-icon"><KeyRound size={18} /></div>
    <div className="root-card-body">
      <Space><Text strong>数据库 root 密码</Text>{set ? <Tag color="green">已设置</Tag> : <Tag color="orange">未设置</Tag>}</Space>
      <Paragraph type="secondary" style={{ margin: 0 }}>{joinedCompute
        ? '修改后立即在数据库中生效，并更新计算节点配置文件，不需要重启。'
        : '第一个计算节点启动时用它初始化 root 账号。添加计算节点前必须设置。'}</Paragraph>
    </div>
    <Space.Compact>
      <Input.Password placeholder={set ? '新密码（至少 8 位）' : '设置密码（至少 8 位）'} value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" style={{ width: 240 }} />
      <Button type="primary" disabled={password.length < 8 || Boolean(ctx.overview.activeTask)} loading={saving} onClick={() => void save()}>{set ? '修改' : '设置'}</Button>
    </Space.Compact>
  </Card>;
}
