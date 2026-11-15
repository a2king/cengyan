import { useState } from 'react';
import { Alert, Button, Form, Input, InputNumber, Modal, Space, Table, Tag, Tooltip, Typography } from 'antd';
import type { TableProps } from 'antd';
import { Plus, Trash2 } from 'lucide-react';
import { STATUS_TEXT, errorText } from '../clusterApi';
import type { ServerRow } from '../clusterApi';
import type { ConsoleContext } from './ClusterConsole';

const { Text } = Typography;

export function ServersTab({ ctx }: { ctx: ConsoleContext }) {
  const [open, setOpen] = useState(false);
  const busy = Boolean(ctx.overview.activeTask);
  const remove = (row: ServerRow) => Modal.confirm({
    title: `移除服务器 ${row.host}？`,
    content: '会停止并删除服务器上的层岩程序、JDK 和 systemd 服务，并从 root 的 authorized_keys 中删除 Studio 公钥。/opt/cengyan/data 下还有数据时保留该目录。',
    okText: '移除', okButtonProps: { danger: true },
    onOk: async () => {
      try { ctx.started(await ctx.api.removeServer(row.id)); }
      catch (reason) { Modal.error({ title: '无法移除', content: errorText(reason) }); }
    },
  });
  const columns: TableProps<ServerRow>['columns'] = [
    { title: '地址', dataIndex: 'host', render: (host: string, row) => <Space direction="vertical" size={0}><span className="code-pill">{host}:{row.sshPort}</span>{row.note && <Text type="secondary">{row.note}</Text>}</Space> },
    { title: '状态', dataIndex: 'status', width: 120, render: (status: string, row) => <Tooltip title={row.message}><Tag color={STATUS_TEXT[status]?.color}>{STATUS_TEXT[status]?.text || status}</Tag></Tooltip> },
    { title: '主机名', dataIndex: 'hostname', render: (value: string) => value || '—' },
    { title: '系统', dataIndex: 'os', render: (value: string, row) => value ? `${value} · ${row.arch}` : '—' },
    { title: '程序包', dataIndex: 'releaseId', render: (value: string) => value ? <Tag color={value === ctx.overview.release ? 'green' : 'orange'}>{value}</Tag> : '—' },
    { title: '节点数', dataIndex: 'nodes', width: 90 },
    { title: '主机指纹', dataIndex: 'hostKey', ellipsis: true, render: (value: string) => value ? <Tooltip title="之后每次连接都校验这个指纹，变化即拒绝"><span className="code-pill">{value}</span></Tooltip> : '—' },
    { title: '操作', key: 'actions', width: 100, fixed: 'right', render: (_, row) => (
      <Tooltip title={row.nodes > 0 ? '请先移除服务器上的节点' : busy ? '有任务进行中' : ''}>
        <Button type="link" danger size="small" icon={<Trash2 size={14} />} disabled={row.nodes > 0 || busy} onClick={() => remove(row)}>移除</Button>
      </Tooltip>) },
  ];
  return <div className="cluster-tab">
    <div className="cluster-tab-head">
      <Text type="secondary">添加服务器时 Studio 只用一次 root 密码：配置免密登录后立即丢弃，不保存。</Text>
      <Button type="primary" icon={<Plus size={15} />} disabled={busy} onClick={() => setOpen(true)}>添加服务器</Button>
    </div>
    <Table<ServerRow> rowKey="id" columns={columns} dataSource={ctx.overview.servers} pagination={false} scroll={{ x: 'max-content' }}
      locale={{ emptyText: '还没有服务器，点击右上角添加' }} />
    <AddServerModal ctx={ctx} open={open} onClose={() => setOpen(false)} />
  </div>;
}

function AddServerModal({ ctx, open, onClose }: { ctx: ConsoleContext; open: boolean; onClose: () => void }) {
  const [form] = Form.useForm();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const submit = async () => {
    const values = await form.validateFields();
    setLoading(true); setError('');
    try { const task = await ctx.api.addServer(values.host.trim(), values.sshPort, values.password, values.note || ''); onClose(); form.resetFields(); ctx.started(task); }
    catch (reason) { setError(errorText(reason)); }
    finally { setLoading(false); }
  };
  return <Modal title="添加服务器" open={open} onCancel={onClose} onOk={() => void submit()} okText="添加" confirmLoading={loading} destroyOnHidden width={520}>
    <Form form={form} layout="vertical" initialValues={{ sshPort: 22 }} preserve={false}>
      <Form.Item name="host" label="服务器内网 IP" extra="节点会监听这个地址，请填写其他服务器能访问到的内网 IP"
        rules={[{ required: true, message: '请输入 IP' }, { pattern: /^[A-Za-z0-9.-]{1,253}$/, message: 'IP 或主机名格式不正确' }]}>
        <Input placeholder="10.0.0.11" autoFocus />
      </Form.Item>
      <Form.Item name="sshPort" label="SSH 端口" rules={[{ required: true }]}><InputNumber min={1} max={65535} style={{ width: '100%' }} /></Form.Item>
      <Form.Item name="password" label="root 密码" rules={[{ required: true, message: '请输入 root 密码' }]}><Input.Password autoComplete="off" /></Form.Item>
      <Form.Item name="note" label="备注"><Input maxLength={64} placeholder="可选，例如 机房 A" /></Form.Item>
      <Alert type="info" showIcon message="添加后自动执行" description="检测系统（Linux x86_64/aarch64、glibc 2.17+、systemd）→ 安装 Studio 公钥并验证免密登录 → 安装 JDK 21 → 同步程序包。" />
      {error && <Alert type="error" showIcon message={error} style={{ marginTop: 12 }} />}
    </Form>
  </Modal>;
}
