import { useEffect, useState } from 'react';
import { Alert, Button, Drawer, Form, Input, InputNumber, Modal, Radio, Select, Space, Table, Tag, Tooltip, Typography } from 'antd';
import type { TableProps } from 'antd';
import { FileText, Plus, RotateCcw, Trash2 } from 'lucide-react';
import { STATUS_TEXT, errorText } from '../clusterApi';
import type { NodeRow, Role } from '../clusterApi';
import type { ConsoleContext } from './ClusterConsole';

const { Text } = Typography;

export function NodesTab({ ctx }: { ctx: ConsoleContext }) {
  const [adding, setAdding] = useState<Role | null>(null);
  const [removing, setRemoving] = useState<NodeRow | null>(null);
  const [logs, setLogs] = useState<{ id: string; text: string; loading: boolean } | null>(null);
  const { overview: o, runtime } = ctx;
  const busy = Boolean(o.activeTask);
  const restart = (row: NodeRow) => Modal.confirm({
    title: `重启 ${row.id}？`,
    content: row.role === 'STORAGE'
      ? '存储节点重启前集群进入维护模式（暂停写入）。数据由 Raft 日志和快照保留，重启后不需要重新导入，完成后自动退出维护模式。'
      : '计算节点先停止接收新查询并等待进行中的查询结束，然后重启。',
    okText: '重启',
    onOk: async () => { try { ctx.started(await ctx.api.restart([row.id])); } catch (reason) { Modal.error({ title: '无法重启', content: errorText(reason) }); } },
  });
  const showLogs = async (id: string) => {
    setLogs({ id, text: '', loading: true });
    try { const result = await ctx.api.logs(id, 500); setLogs({ id, text: result.text, loading: false }); }
    catch (reason) { setLogs({ id, text: errorText(reason), loading: false }); }
  };
  const columns: TableProps<NodeRow>['columns'] = [
    { title: '节点 ID', dataIndex: 'id', render: (id: string, row) => <Space size={4}><Text strong>{id}</Text>{row.overrides > 0 && <Tooltip title={`${row.overrides} 项单独配置`}><Tag>个性化</Tag></Tooltip>}</Space> },
    { title: '角色', dataIndex: 'role', width: 90, render: (role: Role) => <Tag color={role === 'STORAGE' ? 'blue' : 'purple'}>{role === 'STORAGE' ? '存储' : '计算'}</Tag> },
    { title: '服务器', dataIndex: 'host', render: (host: string) => <span className="code-pill">{host}</span> },
    { title: '端口', key: 'ports', render: (_, row) => <Text type="secondary">{row.role === 'STORAGE' ? `raft ${row.raftPort} · rpc ${row.rpcPort}` : `client ${row.clientPort} · rpc ${row.rpcPort} · raft ${row.raftPort}`}</Text> },
    { title: '安装状态', dataIndex: 'status', width: 110, render: (status: string, row) => <Tooltip title={row.message}><Tag color={STATUS_TEXT[status]?.color}>{STATUS_TEXT[status]?.text || status}</Tag></Tooltip> },
    { title: '运行状态', key: 'live', width: 110, render: (_, row) => {
      const state = row.joined ? runtime?.nodes[row.id] : undefined;
      return state ? <Tag color={STATUS_TEXT[state]?.color}>{STATUS_TEXT[state]?.text || state}</Tag> : <Text type="secondary">—</Text>;
    } },
    { title: '配置', key: 'pending', width: 140, render: (_, row) => <Space size={4}>
      {row.pendingRestart && <Tag color="orange">待重启</Tag>}
      {row.upgradeAvailable && <Tag color="gold">可升级</Tag>}
      {!row.pendingRestart && !row.upgradeAvailable && row.joined && <Text type="secondary">已生效</Text>}
    </Space> },
    { title: '数据目录', dataIndex: 'dataDir', ellipsis: true, render: (dir: string) => <span className="code-pill">{dir}</span> },
    { title: '操作', key: 'actions', fixed: 'right', width: 220, render: (_, row) => <span className="table-actions">
      <Button type="link" size="small" icon={<FileText size={14} />} onClick={() => void showLogs(row.id)}>日志</Button>
      <Button type="link" size="small" icon={<RotateCcw size={14} />} disabled={busy || !row.joined} onClick={() => restart(row)}>重启</Button>
      <Button type="link" danger size="small" icon={<Trash2 size={14} />} disabled={busy} onClick={() => setRemoving(row)}>移除</Button>
    </span> },
  ];
  const noServers = !o.servers.some((s) => s.status === 'READY');
  const noStorage = !o.nodes.some((n) => n.role === 'STORAGE' && n.joined && n.status === 'RUNNING');
  return <div className="cluster-tab">
    <div className="cluster-tab-head">
      <Text type="secondary">{busy ? '有任务进行中，完成后才能添加、重启或移除节点。' : '节点添加后自动启动，主节点由集群自动选举。'}</Text>
      <Space>
        <Tooltip title={noServers ? '请先添加服务器' : ''}><Button icon={<Plus size={15} />} disabled={busy || noServers} onClick={() => setAdding('STORAGE')}>添加存储节点</Button></Tooltip>
        <Tooltip title={noServers ? '请先添加服务器' : noStorage ? '请先添加并启动一个存储节点' : !o.rootPasswordSet ? '请先在配置页设置数据库 root 密码' : ''}>
          <Button icon={<Plus size={15} />} disabled={busy || noServers || noStorage || !o.rootPasswordSet} onClick={() => setAdding('COMPUTE')}>添加计算节点</Button>
        </Tooltip>
      </Space>
    </div>
    <Table<NodeRow> rowKey="id" columns={columns} dataSource={o.nodes} pagination={false} scroll={{ x: 'max-content' }}
      locale={{ emptyText: noServers ? '请先添加服务器' : '还没有节点' }} />
    {adding && <AddNodeModal ctx={ctx} role={adding} onClose={() => setAdding(null)} />}
    {removing && <RemoveNodeModal ctx={ctx} node={removing} onClose={() => setRemoving(null)} />}
    <Drawer title={`${logs?.id || ''} 日志（journalctl 最近 500 行）`} width={860} open={Boolean(logs)} onClose={() => setLogs(null)}
      extra={logs && <Button size="small" onClick={() => void showLogs(logs.id)} loading={logs.loading}>刷新</Button>}>
      <pre className="node-logs">{logs?.loading ? '读取中…' : logs?.text || '没有日志'}</pre>
    </Drawer>
  </div>;
}

function AddNodeModal({ ctx, role, onClose }: { ctx: ConsoleContext; role: Role; onClose: () => void }) {
  const [form] = Form.useForm();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const servers = ctx.overview.servers.filter((s) => s.status === 'READY');
  const serverId = Form.useWatch('serverId', form) as string | undefined;
  useEffect(() => {
    if (!serverId) return;
    void ctx.api.suggest(role, serverId).then((s) => form.setFieldsValue(s)).catch((reason) => setError(errorText(reason)));
  }, [ctx.api, form, role, serverId]);
  const submit = async () => {
    const values = await form.validateFields();
    setLoading(true); setError('');
    try {
      const task = await ctx.api.addNode({ role, serverId: values.serverId, id: values.id, raftPort: values.raftPort, rpcPort: values.rpcPort,
        clientPort: role === 'COMPUTE' ? values.clientPort : 0, dataDir: values.dataDir });
      onClose(); ctx.started(task);
    } catch (reason) { setError(errorText(reason)); }
    finally { setLoading(false); }
  };
  const port = { rules: [{ required: true, message: '必填' }] };
  return <Modal title={role === 'STORAGE' ? '添加存储节点' : '添加计算节点'} open onCancel={onClose} onOk={() => void submit()} okText="添加并启动" confirmLoading={loading} width={560} destroyOnHidden>
    <Form form={form} layout="vertical" initialValues={{ serverId: servers[0]?.id }}>
      <Form.Item name="serverId" label="服务器" rules={[{ required: true }]}>
        <Select options={servers.map((s) => ({ value: s.id, label: `${s.host}${s.hostname ? `（${s.hostname}）` : ''} · ${s.nodes} 个节点` }))} />
      </Form.Item>
      <Form.Item name="id" label="节点 ID" extra="添加后不能修改" rules={[{ required: true }, { pattern: /^[a-z][a-z0-9_-]{0,31}$/, message: '小写字母开头，只能包含小写字母、数字、_ 和 -' }]}><Input /></Form.Item>
      <Space size={12} className="port-row">
        {role === 'COMPUTE' && <Form.Item name="clientPort" label="客户端端口" {...port}><InputNumber min={1024} max={65535} /></Form.Item>}
        <Form.Item name="rpcPort" label="RPC 端口" {...port}><InputNumber min={1024} max={65535} /></Form.Item>
        <Form.Item name="raftPort" label="Raft 端口" {...port}><InputNumber min={1024} max={65535} /></Form.Item>
      </Space>
      <Form.Item name="dataDir" label="数据目录" extra="必须是不存在或为空的目录，添加后不能修改" rules={[{ required: true }, { pattern: /^\/[A-Za-z0-9_./-]+$/, message: '请填写绝对路径' }]}><Input /></Form.Item>
      {role === 'STORAGE' && ctx.overview.nodes.filter((n) => n.role === 'STORAGE').length < 2 &&
        <Alert type="info" showIcon message="生产环境建议至少 3 个存储节点" description="副本数为 1 时，节点宕机期间它负责的分片不可用，数据仍由 Raft 日志和快照保留，恢复后不需要重新导入。" />}
      {error && <Alert type="error" showIcon message={error} style={{ marginTop: 12 }} />}
    </Form>
  </Modal>;
}

function RemoveNodeModal({ ctx, node, onClose }: { ctx: ConsoleContext; node: NodeRow; onClose: () => void }) {
  const [deleteData, setDeleteData] = useState(false);
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const submit = async () => {
    setLoading(true); setError('');
    try { const task = await ctx.api.removeNode(node.id, deleteData, confirm); onClose(); ctx.started(task); }
    catch (reason) { setError(errorText(reason)); }
    finally { setLoading(false); }
  };
  return <Modal title={`移除节点 ${node.id}`} open onCancel={onClose} onOk={() => void submit()} okText="移除" confirmLoading={loading}
    okButtonProps={{ danger: true, disabled: deleteData && confirm !== node.id }} destroyOnHidden>
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Text>{!node.joined ? '节点尚未加入集群，将直接停止并删除服务。'
        : node.role === 'STORAGE' ? '先检查剩余存储节点能否满足图空间副本数，然后把该节点上的分片迁移到其他节点，迁移完成后移除。数据量大时可能需要较长时间。'
        : '先停止接收新查询，等待进行中的查询结束（最多 5 分钟），然后从计算成员中移除。'}</Text>
      <Radio.Group value={deleteData} onChange={(e) => setDeleteData(e.target.value)}>
        <Space direction="vertical">
          <Radio value={false}>保留数据目录 <Text type="secondary">{node.dataDir}</Text></Radio>
          <Radio value={true}><Text type="danger">同时删除数据目录</Text></Radio>
        </Space>
      </Radio.Group>
      {deleteData && <Input placeholder={`输入 ${node.id} 确认删除数据`} value={confirm} onChange={(e) => setConfirm(e.target.value)} status={confirm && confirm !== node.id ? 'error' : undefined} />}
      {error && <Alert type="error" showIcon message={error} />}
    </Space>
  </Modal>;
}