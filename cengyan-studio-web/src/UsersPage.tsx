import { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Card, Form, Input, Modal, Select, Space, Table, Tag, Tooltip, Typography, message } from 'antd';
import type { TableProps } from 'antd';
import { KeyRound, Plus, RefreshCw, Trash2, UserCog } from 'lucide-react';
import type { CengyanApi } from './api';
import type { UserRow } from './types';

const { Title, Text } = Typography;

export const ROLE_TEXT: Record<string, { text: string; color: string }> = {
  read: { text: '只读', color: 'default' },
  write: { text: '读写', color: 'blue' },
  admin: { text: '管理员', color: 'purple' },
};

export const PASSWORD_RULES = [
  { required: true, message: '请输入密码' },
  { min: 8, message: '至少 8 位' },
  { max: 128, message: '最多 128 位' },
];

function errorText(reason: unknown) { return reason instanceof Error ? reason.message : String(reason); }

/**
 * 用户管理（docs/user-permissions.md 第 4.1 节）。只有管理员能进来；root 可以管理所有人，
 * 其他管理员只能管理只读、读写用户。按钮置灰只是提示，最终以服务端判断为准。
 */
export function UsersPage({ api, me }: { api: CengyanApi; me: string }) {
  const [rows, setRows] = useState<UserRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [creating, setCreating] = useState(false);
  const [resetting, setResetting] = useState<UserRow | null>(null);
  const [messageApi, holder] = message.useMessage();
  const root = me === 'root';

  const load = useCallback(async () => {
    setLoading(true);
    try { setRows(await api.listUsers()); setError(''); }
    catch (reason) { setError(errorText(reason)); }
    finally { setLoading(false); }
  }, [api]);
  useEffect(() => { void load(); }, [load]);

  /** 不能操作的原因；空字符串表示可以。 */
  const blocked = (row: UserRow, action: 'role' | 'password' | 'drop'): string => {
    if (row.user === 'root') {
      if (action === 'password') return root ? '请在左下角“修改密码”中修改自己的密码' : '只有 root 可以修改 root 的密码';
      return action === 'role' ? '不能修改 root 的角色' : '不能删除 root';
    }
    if (row.user === me && action !== 'password') return action === 'drop' ? '不能删除自己' : '不能修改自己的角色';
    if (row.user === me) return '请在左下角“修改密码”中修改自己的密码';
    if (!root && row.role === 'admin') return '只有 root 可以管理管理员';
    return '';
  };

  const run = async (action: () => Promise<void>, done: string) => {
    try { await action(); messageApi.success(done); await load(); }
    catch (reason) { Modal.error({ title: '操作失败', content: errorText(reason) }); }
  };

  const roleOptions = (row?: UserRow) => [
    { value: 'read', label: '只读：查询、浏览、运维监控' },
    { value: 'write', label: '读写：只读 + 写入、修改、删除数据' },
    ...(root ? [{ value: 'admin', label: '管理员：读写 + DDL、用户管理、集群管理' }] : []),
  ].filter((option) => root || option.value !== 'admin' || row?.role === 'admin');

  const drop = (row: UserRow) => {
    let typed = '';
    Modal.confirm({
      title: `删除用户 ${row.user}？`,
      content: <Space direction="vertical" style={{ width: '100%' }}>
        <Text>删除后不能恢复，该用户的登录会立即失效（其他计算节点最长 30 秒）。之后可以重新创建同名用户。</Text>
        <Input placeholder={`输入 ${row.user} 确认`} onChange={(e) => { typed = e.target.value; }} />
      </Space>,
      okText: '删除', okButtonProps: { danger: true },
      onOk: async () => {
        if (typed !== row.user) { messageApi.error(`请输入 ${row.user} 确认`); throw new Error('未确认'); }
        await run(() => api.dropUser(row.user), `已删除 ${row.user}`);
      },
    });
  };

  const columns: TableProps<UserRow>['columns'] = [
    { title: '用户名', dataIndex: 'user', render: (value: string) => <Space size={6}><span className="code-pill">{value}</span>
      {value === 'root' && <Tag color="gold">内置</Tag>}{value === me && <Tag>当前登录</Tag>}</Space> },
    { title: '角色', dataIndex: 'role', width: 300, render: (value: string, row) => {
      const reason = blocked(row, 'role');
      if (reason) return <Tooltip title={reason}><Tag color={ROLE_TEXT[value]?.color}>{ROLE_TEXT[value]?.text || value}</Tag></Tooltip>;
      return <Select size="small" value={value} style={{ width: 260 }} options={roleOptions(row)}
        onChange={(next) => void run(() => api.setRole(row.user, next), `${row.user} 已改为${ROLE_TEXT[next]?.text || next}，需要重新登录`)} />;
    } },
    { title: '状态', dataIndex: 'enabled', width: 180, render: (enabled: boolean) => enabled
      ? <Tag color="green">正常</Tag>
      : <Tooltip title="旧版本“删除”只是停用，记录还在。现在可以真正删除。"><Tag color="orange">已停用（旧版本删除）</Tag></Tooltip> },
    { title: '操作', key: 'actions', width: 200, render: (_, row) => {
      const password = blocked(row, 'password'), remove = blocked(row, 'drop');
      return <Space size={4}>
        <Tooltip title={password}><Button type="link" size="small" icon={<KeyRound size={14} />} disabled={Boolean(password) || !row.enabled} onClick={() => setResetting(row)}>重置密码</Button></Tooltip>
        <Tooltip title={remove}><Button type="link" size="small" danger icon={<Trash2 size={14} />} disabled={Boolean(remove)} onClick={() => drop(row)}>删除</Button></Tooltip>
      </Space>;
    } },
  ];

  return <div className="page-stack workspace-page">
    {holder}
    <Card className="workspace-card" bordered={false}>
      <div className="panel-stack">
        <div className="panel-toolbar">
          <div><Title level={4}>用户管理</Title>
            <Text type="secondary">{root ? 'root 可以管理所有用户，包括创建和提升管理员。' : '管理员可以管理只读、读写用户；创建或调整管理员需要 root。'}修改角色或密码后，该用户需要重新登录。</Text></div>
          <Space>
            <Button type="primary" icon={<Plus size={15} />} onClick={() => setCreating(true)}>新建用户</Button>
            <Button type="text" icon={<RefreshCw size={15} />} loading={loading} onClick={() => void load()}>刷新</Button>
          </Space>
        </div>
        {error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}
        <Table rowKey="user" loading={loading} columns={columns} dataSource={rows} pagination={{ pageSize: 20, hideOnSinglePage: true }} />
      </div>
    </Card>
    <CreateUserModal open={creating} roles={roleOptions()} onClose={() => setCreating(false)}
      onCreate={(user, password, role) => run(() => api.createUser(user, password, role), `已创建 ${user}`)} />
    <ResetPasswordModal user={resetting} onClose={() => setResetting(null)}
      onReset={(user, password) => run(() => api.setPassword(user, password), `已重置 ${user} 的密码，该用户需要重新登录`)} />
  </div>;
}

function CreateUserModal({ open, roles, onClose, onCreate }: { open: boolean; roles: Array<{ value: string; label: string }>; onClose: () => void;
  onCreate: (user: string, password: string, role: string) => Promise<void> }) {
  const [form] = Form.useForm();
  return <Modal title={<Space><UserCog size={18} />新建用户</Space>} open={open} okText="创建" destroyOnHidden onCancel={onClose}
    onOk={async () => { const v = await form.validateFields(); await onCreate(v.user, v.password, v.role); onClose(); }}>
    <Form form={form} layout="vertical" preserve={false} initialValues={{ role: 'read' }}>
      <Form.Item name="user" label="用户名" rules={[{ required: true, message: '请输入用户名' },
        { pattern: /^[\p{L}][\p{L}\p{N}_]{0,127}$/u, message: '以字母开头，只含字母、数字、下划线，最长 128 位' }]}>
        <Input autoComplete="off" autoFocus />
      </Form.Item>
      <Form.Item name="password" label="密码" rules={PASSWORD_RULES}><Input.Password autoComplete="new-password" /></Form.Item>
      <Form.Item name="role" label="角色" rules={[{ required: true }]}><Select options={roles} /></Form.Item>
    </Form>
  </Modal>;
}

function ResetPasswordModal({ user, onClose, onReset }: { user: UserRow | null; onClose: () => void; onReset: (user: string, password: string) => Promise<void> }) {
  const [form] = Form.useForm();
  return <Modal title={`重置 ${user?.user || ''} 的密码`} open={user !== null} okText="重置" destroyOnHidden onCancel={onClose}
    onOk={async () => { const v = await form.validateFields(); if (user) await onReset(user.user, v.password); onClose(); }}>
    <Form form={form} layout="vertical" preserve={false}>
      <Form.Item name="password" label="新密码" rules={PASSWORD_RULES}><Input.Password autoComplete="new-password" autoFocus /></Form.Item>
    </Form>
  </Modal>;
}

/** 修改自己的密码。所有角色可用；root 改成功后 Studio 同步保存的 root 密码。 */
export function ChangePasswordModal({ api, open, onClose, onChanged }: { api: CengyanApi; open: boolean; onClose: () => void; onChanged: () => void }) {
  const [form] = Form.useForm();
  const [error, setError] = useState('');
  return <Modal title="修改密码" open={open} okText="修改" destroyOnHidden onCancel={() => { setError(''); onClose(); }}
    onOk={async () => {
      const v = await form.validateFields();
      try { await api.changePassword(v.current, v.password); setError(''); onChanged(); }
      catch (reason) { setError(errorText(reason)); throw reason; }
    }}>
    <Form form={form} layout="vertical" preserve={false}>
      <Form.Item name="current" label="当前密码" rules={[{ required: true, message: '请输入当前密码' }]}><Input.Password autoComplete="current-password" autoFocus /></Form.Item>
      <Form.Item name="password" label="新密码" rules={PASSWORD_RULES}><Input.Password autoComplete="new-password" /></Form.Item>
      <Form.Item name="confirm" label="确认新密码" dependencies={['password']} rules={[{ required: true, message: '请再次输入' },
        ({ getFieldValue }) => ({ validator: (_, value) => value === getFieldValue('password') ? Promise.resolve() : Promise.reject(new Error('两次输入不一致')) })]}>
        <Input.Password autoComplete="new-password" />
      </Form.Item>
      {error && <Alert type="error" showIcon message={error} />}
    </Form>
  </Modal>;
}
