import { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Button, Card, Col, Form, Input, Modal, Progress, Row, Space, Statistic, Tabs, Tag, Typography, message } from 'antd';
import { Boxes, KeyRound, LogOut, RefreshCw, RotateCcw, Server, Settings2, ShieldCheck, ListChecks, ArrowUpCircle } from 'lucide-react';
import { ClusterApi, errorText } from '../clusterApi';
import type { Overview, Runtime, TaskView } from '../clusterApi';
import { ServersTab } from './ServersTab';
import { NodesTab } from './NodesTab';
import { ConfigTab } from './ConfigTab';
import { TasksTab } from './TasksTab';

const { Title, Text, Paragraph } = Typography;
export type ClusterTab = 'overview' | 'servers' | 'nodes' | 'config' | 'tasks';

/** 首次打开时设置管理员密码，之后用它登录。只保护集群管理，不影响图数据库账号。 */
export function AdminGate({ api, onReady }: { api: ClusterApi; onReady: () => void }) {
  const [initialized, setInitialized] = useState<boolean | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  useEffect(() => { void api.status().then((s) => setInitialized(s.initialized)).catch((reason) => setError(errorText(reason))); }, [api]);
  const submit = async (values: { password: string; confirm?: string }) => {
    setLoading(true); setError('');
    try {
      if (initialized) await api.login(values.password);
      else await api.setup(values.password);
      onReady();
    } catch (reason) { setError(errorText(reason)); } finally { setLoading(false); }
  };
  return <div className="cluster-gate">
    <Card className="login-card" bordered={false}>
      <div className="login-card-head"><div className="soft-icon"><ShieldCheck size={19} /></div><div>
        <Title level={3}>{initialized === false ? '设置 Studio 管理员密码' : '集群管理登录'}</Title>
        <Text type="secondary">{initialized === false ? '首次使用，请设置管理员密码，用于添加服务器、节点和修改配置' : '输入 Studio 管理员密码'}</Text>
      </div></div>
      {initialized !== null && <Form layout="vertical" onFinish={(values) => void submit(values)}>
        <Form.Item name="password" label="管理员密码" rules={[{ required: true, message: '请输入密码' }, ...(initialized ? [] : [{ min: 8, message: '至少 8 位' }])]}>
          <Input.Password size="large" prefix={<KeyRound size={17} />} autoComplete={initialized ? 'current-password' : 'new-password'} autoFocus />
        </Form.Item>
        {!initialized && <Form.Item name="confirm" label="确认密码" dependencies={['password']} rules={[{ required: true, message: '请再次输入' },
          ({ getFieldValue }) => ({ validator: (_, value) => value === getFieldValue('password') ? Promise.resolve() : Promise.reject(new Error('两次输入不一致')) })]}>
          <Input.Password size="large" prefix={<KeyRound size={17} />} autoComplete="new-password" />
        </Form.Item>}
        {error && <Alert type="error" showIcon message={error} style={{ marginBottom: 16 }} />}
        <Button type="primary" htmlType="submit" size="large" block loading={loading}>{initialized ? '登录' : '设置并进入'}</Button>
      </Form>}
      {initialized === null && !error && <Text type="secondary">正在连接 Studio…</Text>}
      {initialized === null && error && <Alert type="error" showIcon message={error} />}
    </Card>
  </div>;
}

export interface ConsoleContext {
  api: ClusterApi;
  overview: Overview;
  runtime: Runtime | null;
  refresh: () => Promise<void>;
  go: (tab: ClusterTab, taskId?: string) => void;
  /** 提交任务后统一处理：提示并跳到任务页。 */
  started: (task: TaskView) => void;
}

export function ClusterConsole({ api }: { api: ClusterApi }) {
  const [authed, setAuthed] = useState(() => Boolean(api.token));
  const [tab, setTab] = useState<ClusterTab>('overview');
  const [focusTask, setFocusTask] = useState('');
  const [overview, setOverview] = useState<Overview | null>(null);
  const [runtime, setRuntime] = useState<Runtime | null>(null);
  const [error, setError] = useState('');
  const [passwordOpen, setPasswordOpen] = useState(false);
  const [messageApi, holder] = message.useMessage();

  useEffect(() => { api.onUnauthorized = () => setAuthed(false); }, [api]);
  const refresh = useCallback(async () => {
    try { setOverview(await api.overview()); setError(''); }
    catch (reason) { setError(errorText(reason)); }
  }, [api]);
  const refreshRuntime = useCallback(async () => {
    try { setRuntime(await api.runtime()); } catch { /* 运行状态读取失败只影响状态列 */ }
  }, [api]);
  useEffect(() => {
    if (!authed) return undefined;
    void refresh(); void refreshRuntime();
    const fast = window.setInterval(() => void refresh(), 3000);
    const slow = window.setInterval(() => void refreshRuntime(), 10000);
    return () => { window.clearInterval(fast); window.clearInterval(slow); };
  }, [authed, refresh, refreshRuntime]);

  const context = useMemo<ConsoleContext | null>(() => overview && ({
    api, overview, runtime, refresh,
    go: (next, taskId) => { setTab(next); if (taskId) setFocusTask(taskId); },
    started: (task) => { messageApi.success(`已开始：${task.title}`); setFocusTask(task.id); setTab('tasks'); void refresh(); },
  }), [api, overview, runtime, refresh, messageApi]);

  if (!authed) return <AdminGate api={api} onReady={() => setAuthed(true)} />;
  if (!context) return <div className="page-stack">{error ? <Alert type="error" showIcon message={error} /> : <Card loading bordered={false} />}</div>;

  const pending = context.overview.pendingRestart;
  return <div className="page-stack workspace-page cluster-console">
    {holder}
    <div className="ops-toolbar">
      <Text type="secondary">集群 {context.overview.clusterId} · 程序包 {context.overview.release || '未找到'} · 配置 v{context.overview.configVersion}</Text>
      <Space>
        <Button icon={<RefreshCw size={15} />} onClick={() => { void refresh(); void refreshRuntime(); }}>刷新</Button>
        <Button icon={<KeyRound size={15} />} onClick={() => setPasswordOpen(true)}>管理员密码</Button>
        <Button icon={<LogOut size={15} />} onClick={() => void api.logout().finally(() => setAuthed(false))}>退出管理</Button>
      </Space>
    </div>
    {error && <Alert type="warning" showIcon message="状态刷新失败" description={error} />}
    <Card className="workspace-card" bordered={false}>
      <Tabs activeKey={tab} onChange={(key) => setTab(key as ClusterTab)} items={[
        { key: 'overview', label: <span className="tab-label"><Boxes size={16} />概览</span>, children: <OverviewTab ctx={context} /> },
        { key: 'servers', label: <span className="tab-label"><Server size={16} />服务器</span>, children: <ServersTab ctx={context} /> },
        { key: 'nodes', label: <span className="tab-label"><Boxes size={16} />节点{pending > 0 && <Tag color="orange" className="tab-badge">{pending} 待重启</Tag>}</span>, children: <NodesTab ctx={context} /> },
        { key: 'config', label: <span className="tab-label"><Settings2 size={16} />配置</span>, children: <ConfigTab ctx={context} /> },
        { key: 'tasks', label: <span className="tab-label"><ListChecks size={16} />任务{context.overview.activeTask && <Tag color="blue" className="tab-badge">进行中</Tag>}</span>,
          children: <TasksTab ctx={context} focus={focusTask} onFocus={setFocusTask} /> },
      ]} />
    </Card>
    <AdminPasswordModal api={api} open={passwordOpen} onClose={() => setPasswordOpen(false)} />
  </div>;
}

function OverviewTab({ ctx }: { ctx: ConsoleContext }) {
  const { overview: o, runtime, api } = ctx;
  const [busy, setBusy] = useState('');
  const maintenance = runtime?.maintenance;
  const storage = o.nodes.filter((n) => n.role === 'STORAGE');
  const compute = o.nodes.filter((n) => n.role === 'COMPUTE');
  const online = (role: string) => o.nodes.filter((n) => n.role === role && runtime?.nodes[n.id] === 'ACTIVE').length;
  const run = async (key: string, action: () => Promise<TaskView | unknown>) => {
    setBusy(key);
    try {
      const result = await action();
      if (result && typeof result === 'object' && 'steps' in result) ctx.started(result as TaskView);
      else await ctx.refresh();
    } catch (reason) { Modal.error({ title: '操作失败', content: errorText(reason) }); }
    finally { setBusy(''); }
  };
  const task = o.activeTask;
  const done = task ? task.steps.filter((s) => s.state === 'DONE' || s.state === 'SKIPPED').length : 0;
  return <div className="cluster-overview">
    <Row gutter={[16, 16]}>
      <Col xs={12} md={6}><Card className="metric-card"><Statistic title="服务器" value={`${o.servers.filter((s) => s.status === 'READY').length} / ${o.servers.length}`} prefix={<Server size={18} />} /></Card></Col>
      <Col xs={12} md={6}><Card className="metric-card"><Statistic title="存储节点（在线 / 全部）" value={`${runtime ? online('STORAGE') : '—'} / ${storage.length}`} prefix={<Boxes size={18} />} /></Card></Col>
      <Col xs={12} md={6}><Card className="metric-card"><Statistic title="计算节点（在线 / 全部）" value={`${runtime ? online('COMPUTE') : '—'} / ${compute.length}`} prefix={<Boxes size={18} />} /></Card></Col>
      <Col xs={12} md={6}><Card className="metric-card"><Statistic title="程序包版本" value={o.version} prefix={<ArrowUpCircle size={18} />} /></Card></Col>
    </Row>
    <div className="cluster-alerts">
      {o.bundleError && <Alert type="error" showIcon message="Studio 发布包不完整" description={o.bundleError} />}
      {maintenance?.active && <Alert type="warning" showIcon message="集群处于维护模式，写入已暂停"
        description={`分片 ${maintenance.partitions.ready}/${maintenance.partitions.total} 就绪${maintenance.partitions.lost ? `，${maintenance.partitions.lost} 个丢失` : ''}。滚动重启完成后会自动退出；如果任务已终止，确认节点正常后手动退出。`}
        action={!task && <Button size="small" loading={busy === 'exit'} onClick={() => Modal.confirm({ title: '退出维护模式？', content: '退出后恢复写入。请确认所有存储节点已正常运行。', okText: '退出维护模式', onOk: () => run('exit', () => api.exitMaintenance()) })}>退出维护模式</Button>} />}
      {o.pendingRestart > 0 && <Alert type="info" showIcon message={`${o.pendingRestart} 个节点配置已变更，需要重启后生效`}
        description="滚动重启会一次重启一台：存储节点在维护模式下重启并比对本地文件，计算节点先排空查询。"
        action={<Button type="primary" size="small" icon={<RotateCcw size={14} />} disabled={Boolean(task)} loading={busy === 'restart'} onClick={() => run('restart', () => api.restart([]))}>滚动重启</Button>} />}
      {o.upgradeAvailable > 0 && <Alert type="info" showIcon message={`${o.upgradeAvailable} 个节点的程序版本低于 Studio 随附版本 ${o.release}`}
        action={<Button size="small" icon={<ArrowUpCircle size={14} />} disabled={Boolean(task)} loading={busy === 'upgrade'}
          onClick={() => Modal.confirm({ title: '升级集群？', content: '先把新程序包同步到所有服务器，再按滚动重启的方式逐台升级。存储节点重启期间写入暂停。', okText: '开始升级', onOk: () => run('upgrade', () => api.upgrade()) })}>升级集群</Button>} />}
      {runtime?.maintenanceError && storage.some((n) => n.joined) && <Alert type="warning" showIcon message="无法读取存储集群状态" description={runtime.maintenanceError} />}
    </div>
    {task && <Card className="info-card task-card" bordered={false} onClick={() => ctx.go('tasks', task.id)}>
      <div className="info-icon"><ListChecks size={18} /></div>
      <div className="task-card-body">
        <Text strong>{task.title}</Text>
        <Text type={task.state === 'RUNNING' ? 'secondary' : 'warning'}>{task.state === 'RUNNING' ? task.steps[task.current]?.title || '准备中' : `已暂停：${task.error}`}</Text>
        <Progress percent={Math.round((done / Math.max(1, task.steps.length)) * 100)} size="small" status={task.state === 'RUNNING' ? 'active' : 'exception'} />
      </div>
    </Card>}
    {(o.servers.length === 0 || storage.length === 0 || compute.length === 0) && <Card className="guide-card" bordered={false} title="开始部署">
      <ol className="guide-steps">
        <li className={o.servers.some((s) => s.status === 'READY') ? 'done' : ''}><Text strong>添加服务器</Text><Paragraph type="secondary">输入 IP 和 root 密码，Studio 自动检查系统、配置免密登录、安装 JDK 21 并同步程序。</Paragraph>
          <Button size="small" onClick={() => ctx.go('servers')}>去添加</Button></li>
        <li className={storage.some((n) => n.joined) ? 'done' : ''}><Text strong>添加存储节点</Text><Paragraph type="secondary">选择服务器，节点自动启动并加入集群。生产环境至少 3 个存储节点。</Paragraph>
          <Button size="small" onClick={() => ctx.go('nodes')}>去添加</Button></li>
        <li className={o.rootPasswordSet ? 'done' : ''}><Text strong>设置数据库 root 密码</Text><Paragraph type="secondary">第一个计算节点启动时用它初始化 root 账号。</Paragraph>
          <Button size="small" onClick={() => ctx.go('config')}>去设置</Button></li>
        <li className={compute.some((n) => n.joined) ? 'done' : ''}><Text strong>添加计算节点</Text><Paragraph type="secondary">计算节点就绪后即可登录图数据库，开始建图和查询。</Paragraph>
          <Button size="small" onClick={() => ctx.go('nodes')}>去添加</Button></li>
      </ol>
    </Card>}
  </div>;
}

function AdminPasswordModal({ api, open, onClose }: { api: ClusterApi; open: boolean; onClose: () => void }) {
  const [form] = Form.useForm();
  const [error, setError] = useState('');
  return <Modal title="修改 Studio 管理员密码" open={open} onCancel={onClose} okText="修改" destroyOnHidden
    onOk={async () => {
      const values = await form.validateFields();
      try { await api.changePassword(values.current, values.password); message.success('已修改，请重新登录'); onClose(); api.token = ''; api.onUnauthorized(); }
      catch (reason) { setError(errorText(reason)); throw reason; }
    }}>
    <Form form={form} layout="vertical" preserve={false}>
      <Form.Item name="current" label="当前密码" rules={[{ required: true }]}><Input.Password autoComplete="current-password" /></Form.Item>
      <Form.Item name="password" label="新密码" rules={[{ required: true }, { min: 8, message: '至少 8 位' }]}><Input.Password autoComplete="new-password" /></Form.Item>
      {error && <Alert type="error" showIcon message={error} />}
    </Form>
  </Modal>;
}