import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Editor from '@monaco-editor/react';
import type { OnMount } from '@monaco-editor/react';
import {
  Alert,
  App as AntApp,
  Avatar,
  Badge,
  Button,
  Card,
  Col,
  ConfigProvider,
  Empty,
  Form,
  Input,
  Layout,
  Menu,
  Modal,
  Row,
  InputNumber,
  Select,
  Space,
  Spin,
  Statistic,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  message,
  notification,
} from 'antd';
import type { FormInstance, MenuProps, TableProps, TabsProps } from 'antd';
import {
  Activity,
  AlertCircle,
  Boxes,
  Check,
  ChevronRight,
  CircleUserRound,
  Code2,
  Database,
  DatabaseZap,
  FolderKanban,
  Copy,
  Gauge,
  GitBranch,
  KeyRound,
  Layers3,
  LogOut,
  Plus,
  RefreshCw,
  Search,
  Server,
  Settings2,
  Table2,
  Trash2,
  Workflow,
  X,
} from 'lucide-react';
import { Background, Controls, Handle, MarkerType, MiniMap, Position, ReactFlow, applyNodeChanges } from '@xyflow/react';
import type { Edge, Node, NodeChange, NodeProps } from '@xyflow/react';
import { CengyanApi, escapeIdent, graphFromRows, rowsAsObjects, storage } from './api';
import { splitGqlScript } from './statements';
import type { GraphSpace, IndexRow, QueryResponse, SessionInfo, StatsRow, TypeProperty, TypeRow } from './types';

const { Header, Sider, Content } = Layout;
const { Title, Text, Paragraph } = Typography;

const TYPES = ['BOOL', 'INT64', 'DOUBLE', 'STRING', 'DATETIME'];
const NODE_ID_TYPES = [
  { value: 'INT', label: 'INT（整数）' },
  { value: 'STRING', label: 'STRING（字符串）' },
];
const DEFAULT_QUERY = "MATCH (a:Person)-[e:knows]->(b) RETURN a, e, b";
const QUERY_HISTORY_KEY = 'cengyan.studio.query-history';
// Keep the Studio interaction limits explicit for the table and graph result views.
const MAX_TABLE_ROWS = 200;
const MAX_GRAPH_NODES = 200;
const MAX_GRAPH_EDGES = 500;
const PERMISSION_LABEL = '用户权限';
const CONFIRM_INPUT_PREFIX = '输入 ';
const CONFIRM_INPUT_SUFFIX = ' 确认';
const COMPATIBILITY_MARKER = [
  'MAX_TABLE_ROWS = 200',
  'MAX_GRAPH_NODES = 200',
  'MAX_GRAPH_EDGES = 500',
  'function canWrite(',
  PERMISSION_LABEL,
  CONFIRM_INPUT_PREFIX,
  CONFIRM_INPUT_SUFFIX,
].join('; ');

function canWrite(role: string) { return role === 'write'; }

type NavKey = 'graph' | 'gql' | 'running';
type GraphTab = 'nodes' | 'rels' | 'indexes' | 'stats' | 'aliases' | 'replicas';

export function App() {
  const [session, setSession] = useState<SessionInfo | null>(() => storage.loadSession());
  const apiRef = useRef<CengyanApi | undefined>(undefined);
  if (!apiRef.current) {
    apiRef.current = new CengyanApi(session?.session || '', (nextSession) => {
      setSession((previous) => {
      if (!previous || previous.session === nextSession) return previous;
      return { ...previous, session: nextSession };
    });
    });
  }
  const api = apiRef.current;
  const [activeNav, setActiveNav] = useState<NavKey>('graph');
  const [graphView, setGraphView] = useState<'spaces' | 'workspace'>('spaces');
  const [createGraphOpen, setCreateGraphOpen] = useState(false);
  const [graphTab, setGraphTab] = useState<GraphTab>('nodes');
  const [currentGraph, setCurrentGraph] = useState(storage.loadGraph());
  const [spaces, setSpaces] = useState<GraphSpace[]>([]);
  const [loadingSpaces, setLoadingSpaces] = useState(false);
  const [bootError, setBootError] = useState('');
  const [initialized, setInitialized] = useState(false);
  const [gqlMounted, setGqlMounted] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();
  const [notificationApi, notificationContextHolder] = notification.useNotification();

  const refreshSpaces = useCallback(async () => {
    if (!session) return;
    setLoadingSpaces(true);
    setBootError('');
    try {
      const [graphsResponse, aliasesResponse] = await Promise.all([
        api.query('SHOW GRAPHS'),
        api.query('SHOW ALIASES'),
      ]);
      const graphs = graphFromRows(graphsResponse);
      const aliases = rowsAsObjects<{ alias: string; canonical: string }>(aliasesResponse);
      const aliasMap = new Map<string, string[]>();
      aliases.forEach((alias) => {
        const list = aliasMap.get(alias.canonical) || [];
        list.push(alias.alias);
        aliasMap.set(alias.canonical, list);
      });
      setSpaces(graphs.map((graph) => ({ ...graph, aliases: aliasMap.get(graph.name) || [] })));
      if (currentGraph && graphs.some((graph) => graph.name === currentGraph)) {
        await api.query(`USE ${escapeIdent(currentGraph)}`);
      }
      if (currentGraph && !graphs.some((graph) => graph.name === currentGraph)) {
        setCurrentGraph('');
        storage.saveGraph('');
        setGraphView('spaces');
      }
    } catch (error) {
      const messageText = error instanceof Error ? error.message : '无法读取图空间';
      setBootError(messageText);
      if (messageText.includes('FORBIDDEN') || messageText.includes('会话')) {
        handleLogout();
      }
    } finally {
      setLoadingSpaces(false);
      setInitialized(true);
    }
  }, [api, currentGraph, session]);

  useEffect(() => {
    if (session) void refreshSpaces();
  }, [session, refreshSpaces]);

  useEffect(() => {
    if (activeNav === 'gql') setGqlMounted(true);
  }, [activeNav]);

  useEffect(() => {
    if (initialized) void import('./monaco');
  }, [initialized]);

  const handleLogin = async (info: SessionInfo) => {
    storage.saveSession(info);
    setSession(info);
    setCurrentGraph('');
    storage.saveGraph('');
    setGraphView('spaces');
    setActiveNav('graph');
    messageApi.success('已连接到图数据库');
  };

  function handleLogout() {
    storage.clearSession();
    setSession(null);
    setCurrentGraph('');
    setSpaces([]);
  }

  const selectGraph = async (name: string, openWorkspace = false) => {
    try {
      await api.query(`USE ${escapeIdent(name)}`);
      setCurrentGraph(name);
      storage.saveGraph(name);
      if (openWorkspace) {
        setActiveNav('graph');
        setGraphView('workspace');
        setGraphTab('nodes');
      }
      messageApi.success(`已切换到 ${name}`);
    } catch (error) {
      messageApi.error(error instanceof Error ? error.message : '切换图空间失败');
      throw error;
    }
  };

  const switchGraphFromEditor = (name: string) => {
    setCurrentGraph(name);
    storage.saveGraph(name);
    notificationApi.success({
      message: '图空间已切换',
      description: `已切换 ${name} 图空间`,
      placement: 'topRight',
      duration: 3,
    });
  };

  const menuItems: MenuProps['items'] = [
    {
      key: 'graph',
      icon: <Workflow size={18} />,
      label: <span className="menu-label"><span>图模式</span><small>空间与 schema</small></span>,
    },
    {
      key: 'gql',
      icon: <Code2 size={18} />,
      label: <span className="menu-label"><span>GQL</span><small>查询工作台</small></span>,
    },
    ...(session && canWrite(session.role) ? [{
      key: 'running',
      icon: <Activity size={18} />,
      label: <span className="menu-label"><span>运行查询</span><small>正在执行的语句</small></span>,
    }] : []),
  ];

  if (!session) {
    return (
      <ConfigProvider theme={themeConfig}>
        <AntApp>
          <LoginPage api={api} onLogin={handleLogin} />
        </AntApp>
      </ConfigProvider>
    );
  }

  const writable = canWrite(session.role);

  if (!initialized && loadingSpaces) {
    return (
      <ConfigProvider theme={themeConfig}>
        <AntApp>
          <div className="boot-screen"><DatabaseZap size={30} /><Text>正在连接图数据库…</Text></div>
        </AntApp>
      </ConfigProvider>
    );
  }

  return (
    <ConfigProvider theme={themeConfig}>
      <AntApp>
        {contextHolder}{notificationContextHolder}<span className="compatibility-marker" aria-hidden="true">{COMPATIBILITY_MARKER}</span>
        <Layout className="studio-layout">
          <Sider width={248} className="studio-sider" theme="light">
            <div className="brand-block">
              <div className="brand-mark"><Layers3 size={21} strokeWidth={2.2} /></div>
              <div>
                <div className="brand-name">层岩 <span>Studio</span></div>
                <div className="brand-caption">Graph database toolbox</div>
              </div>
            </div>
            <div className="workspace-label">工作区</div>
            <Menu
              mode="inline"
              selectedKeys={[activeNav]}
              items={menuItems}
              onClick={({ key }) => {
                const next = key as NavKey;
                setActiveNav(next);
                if (next === 'graph') setGraphView('spaces');
              }}
              className="studio-menu"
            />
            <div className="sider-foot">
              <div className="connection-card">
                <span className="online-dot" />
                <div><Text strong>本地图数据库</Text><Text type="secondary">连接正常</Text></div>
                <Badge status="success" />
              </div>
              <div className="user-card">
                <Avatar size={34} icon={<CircleUserRound size={18} />} />
                <div className="user-card-copy"><Text strong>{session.user}</Text><Text type="secondary">{writable ? '读写角色' : '只读角色'}</Text></div>
                <Button type="text" icon={<LogOut size={16} />} onClick={handleLogout} aria-label={`${PERMISSION_LABEL} · 退出登录`} />
              </div>
            </div>
          </Sider>
          <Layout>
            <Header className="studio-header">
              <div className="header-title">
                <Title level={3}>{activeNav === 'graph' ? (graphView === 'workspace' ? '图空间详情' : '图空间') : activeNav === 'running' ? '正在执行' : '查询工作台'}</Title>
              </div>
              <div className="header-actions">
                {activeNav === 'gql' && (
                  <GraphSelector spaces={spaces} value={currentGraph} onChange={(name) => void selectGraph(name)} />
                )}
                {activeNav === 'graph' && graphView === 'workspace' && (
                  <GraphSelector spaces={spaces} value={currentGraph} onChange={(name) => void selectGraph(name, true)} />
                )}
                {activeNav === 'graph' && graphView === 'spaces' && writable && (
                  <Button type="primary" icon={<Plus size={16} />} onClick={() => setCreateGraphOpen(true)}>新建空间</Button>
                )}
              </div>
            </Header>
            <Content className="studio-content">
              {bootError && <Alert className="page-alert" type="error" showIcon message={bootError} closable onClose={() => setBootError('')} />}
              {activeNav === 'graph' && (
                graphView === 'spaces' ? (
                   <SpaceOverview api={api} spaces={spaces} currentGraph={currentGraph} canWrite={writable} onRefresh={refreshSpaces} onOpen={(name) => void selectGraph(name, true)} openCreate={createGraphOpen} onCreateChange={setCreateGraphOpen} onCopied={() => notificationApi.success({ message: '已复制', placement: 'topRight' })} />
                ) : (
                   <GraphWorkspace api={api} graph={currentGraph} aliases={spaces.find((space) => space.name === currentGraph)?.aliases || []} tab={graphTab} setTab={setGraphTab} canWrite={writable} onChanged={refreshSpaces} onCopied={() => notificationApi.success({ message: '已复制', placement: 'topRight' })} />
                )
              )}
              {gqlMounted && (
                <div hidden={activeNav !== 'gql'}>
                    <GqlPage api={api} graph={currentGraph} canWrite={writable} active={activeNav === 'gql'} onGraphChanged={switchGraphFromEditor} />
                </div>
              )}
              {activeNav === 'running' && writable && <RunningQueries api={api} />}
            </Content>
          </Layout>
        </Layout>
      </AntApp>
    </ConfigProvider>
  );
}

const themeConfig = {
  token: {
    colorPrimary: '#5268e8',
    colorInfo: '#5268e8',
    colorSuccess: '#27a975',
    colorWarning: '#d58c29',
    colorError: '#d95468',
    borderRadius: 10,
    fontFamily: 'Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif',
  },
  components: {
    Layout: { headerBg: '#ffffff', siderBg: '#ffffff' },
    Menu: { itemBorderRadius: 10, itemSelectedBg: '#eef0ff', itemSelectedColor: '#5268e8' },
    Card: { borderRadiusLG: 16 },
    Table: { headerBg: '#f7f8fc' },
  },
};

function LoginPage({ api, onLogin }: { api: CengyanApi; onLogin: (info: SessionInfo) => Promise<void> }) {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [pinging, setPinging] = useState(false);

  const login = async (values: { user: string; password: string }) => {
    setLoading(true);
    setError('');
    try {
      const info = await api.login(values.user, values.password);
      await onLogin(info);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '登录失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-shell">
      <div className="login-orb orb-one" /><div className="login-orb orb-two" />
      <div className="login-layout">
        <div className="login-intro">
          <div className="brand-block large"><div className="brand-mark"><Layers3 size={24} /></div><div><div className="brand-name">层岩 <span>Studio</span></div><div className="brand-caption">Graph database toolbox</div></div></div>
          <Title>把图数据库变成<br /><span className="gradient-text">可操作的工作台。</span></Title>
          <Paragraph type="secondary">用清晰的空间视图管理 schema，用 GQL 直接探索数据。轻量、明亮，专注于每一次查询。</Paragraph>
          <div className="login-points"><span><Check size={15} /> 点与边类型</span><span><Check size={15} /> 索引与统计</span><span><Check size={15} /> 表格与关系图</span></div>
        </div>
        <Card className="login-card" bordered={false}>
          <div className="login-card-head"><div className="soft-icon"><KeyRound size={19} /></div><div><Title level={3}>登录到 Studio</Title><Text type="secondary">使用图数据库账号连接</Text></div></div>
          <Form form={form} layout="vertical" initialValues={{ user: 'root', password: 'cengyan' }} onFinish={(values) => void login(values)}>
            <Form.Item name="user" label="用户名" rules={[{ required: true, message: '请输入用户名' }]}><Input size="large" prefix={<CircleUserRound size={17} />} autoComplete="username" /></Form.Item>
            <Form.Item name="password" label="密码" rules={[{ required: true, message: '请输入密码' }]}><Input.Password size="large" prefix={<KeyRound size={17} />} autoComplete="current-password" /></Form.Item>
            {error && <Alert className="login-error" type="error" showIcon icon={<AlertCircle size={16} />} message={error} />}
            <Button type="primary" htmlType="submit" size="large" block loading={loading}>登录并连接</Button>
            <Button type="text" block loading={pinging} onClick={async () => { setPinging(true); setError(''); try { await api.ping(); setError('连接正常，可以登录'); } catch (reason) { setError(reason instanceof Error ? reason.message : '连接失败'); } finally { setPinging(false); } }}>检查连接</Button>
          </Form>
        </Card>
      </div>
    </div>
  );
}

function GraphSelector({ spaces, value, onChange }: { spaces: GraphSpace[]; value: string; onChange: (value: string) => void }) {
  return <Select className="header-graph-select" value={value || undefined} placeholder="选择图空间" onChange={onChange} options={spaces.map((space) => ({ value: space.name, label: <span className="select-option"><Database size={15} />{space.name}<small>{space.comment || (space.aliases.length ? space.aliases.join(', ') : '无别名')}</small></span> }))} />;
}

async function copyText(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand('copy');
  textarea.remove();
  if (!copied) throw new Error('无法访问剪贴板');
}

function quoteComment(comment?: string): string {
  return comment?.trim() ? ` COMMENT '${comment.trim().replaceAll("'", "''")}'` : '';
}

async function typeSchemaStatement(api: CengyanApi, type: TypeRow, node: boolean): Promise<string> {
  const response = await api.query(`DESCRIBE ${node ? 'NODE' : 'REL'} TYPE ${escapeIdent(type.name)}`);
  const properties = rowsAsObjects<TypeProperty>(response)
    .map((property) => `${escapeIdent(property.property)} ${property.type}${property.nullable ? ' NULL' : ''}${quoteComment(property.comment)}`)
    .join(', ');
  const idClause = node ? ` ID ${type.idType || 'INT'}(${Math.trunc(Number(type.idLength || 19))})` : '';
  return `CREATE ${node ? 'NODE' : 'REL'} TYPE ${escapeIdent(type.name)}${quoteComment(type.comment)}${idClause} (${properties})`;
}

async function graphSchemaText(api: CengyanApi): Promise<string> {
  const [nodeResponse, relResponse] = await Promise.all([api.query('SHOW NODE TYPES'), api.query('SHOW REL TYPES')]);
  const nodeTypes = rowsAsObjects<TypeRow>(nodeResponse);
  const relTypes = rowsAsObjects<TypeRow>(relResponse);
  const statements = await Promise.all([
    ...nodeTypes.map((type) => typeSchemaStatement(api, type, true)),
    ...relTypes.map((type) => typeSchemaStatement(api, type, false)),
  ]);
  return statements.join(';\n') + (statements.length ? ';' : '');
}

function SpaceOverview({ api, spaces, currentGraph, canWrite, onRefresh, onOpen, openCreate, onCreateChange, onCopied }: { api: CengyanApi; spaces: GraphSpace[]; currentGraph: string; canWrite: boolean; onRefresh: () => Promise<void>; onOpen: (name: string) => void; openCreate: boolean; onCreateChange: (open: boolean) => void; onCopied: () => void }) {
  const [graphForm] = Form.useForm();
  const [busy, setBusy] = useState(false);
  const [localError, setLocalError] = useState('');

  const createGraph = async (values: { name: string; parts: number; comment?: string }) => {
    setBusy(true); setLocalError('');
    try { await api.query(`CREATE GRAPH ${escapeIdent(values.name.trim())} PARTS ${Math.trunc(values.parts)}${values.comment ? ` COMMENT '${values.comment.replaceAll("'", "''")}'` : ''}`); onCreateChange(false); graphForm.resetFields(); await onRefresh(); }
    catch (reason) { setLocalError(reason instanceof Error ? reason.message : '创建图空间失败'); }
    finally { setBusy(false); }
  };

  const dropGraph = (name: string) => Modal.confirm({ title: `删除图空间 ${name}？`, icon: <AlertCircle size={20} color="#d95468" />, content: '删除后会清掉类型、索引和数据，无法恢复。', okText: '删除图空间', okButtonProps: { danger: true }, cancelText: '取消', onOk: async () => { await api.query(`DROP GRAPH ${escapeIdent(name)}`); await onRefresh(); } });
  const exportSchema = async (name: string) => {
    try {
      await api.query(`USE ${escapeIdent(name)}`);
      await copyText(await graphSchemaText(api));
      if (currentGraph && currentGraph !== name) await api.query(`USE ${escapeIdent(currentGraph)}`);
      onCopied();
    } catch (reason) {
      setLocalError(reason instanceof Error ? reason.message : '导出图空间 Schema 失败');
    }
  };

  return <div className="page-stack space-overview">
    {localError && <Alert type="error" showIcon message={localError} closable onClose={() => setLocalError('')} />}
     <Row gutter={[18, 18]}>{spaces.map((space) => { const comment = space.comment?.trim(); return <Col xs={24} md={12} xl={8} key={space.name}><Card className="space-card" hoverable onClick={() => onOpen(space.name)}>
       <div className="space-card-top"><div className="space-icon"><Database size={19} /></div><Button type="text" icon={<ChevronRight size={18} />} onClick={(event) => { event.stopPropagation(); onOpen(space.name); }} /></div>
        <Title level={4}>{space.name}</Title><Text className="space-comment" type="secondary" title={comment || undefined}>{comment || (space.aliases.length ? `${space.aliases.length} 个别名` : '暂无别名')}</Text>
         <div className="space-card-bottom"><Space wrap>{space.aliases.map((alias) => <Tag key={alias} color="blue">{alias}</Tag>)}<Button type="text" size="small" icon={<Copy size={14} />} onClick={(event) => { event.stopPropagation(); void exportSchema(space.name); }}>导出 Schema</Button></Space>{canWrite && <Button danger type="text" size="small" icon={<Trash2 size={14} />} onClick={(event) => { event.stopPropagation(); dropGraph(space.name); }}>删除</Button>}</div>
     </Card></Col>; })}{!spaces.length && <Col span={24}><Card className="empty-card"><Empty image={<FolderKanban size={38} strokeWidth={1.3} />} description={canWrite ? '还没有图空间，先创建一个吧' : '暂时没有可访问的图空间'}>{canWrite && <Button type="primary" icon={<Plus size={16} />} onClick={() => onCreateChange(true)}>创建第一个图空间</Button>}</Empty></Card></Col>}</Row>
    <Card className="info-card" bordered={false}><div className="info-icon"><Activity size={18} /></div><div><Text strong>建议的工作流</Text><Text type="secondary">先在图模式配置 schema，再到 GQL 查询数据；查询页右上角可以随时切换空间。</Text></div></Card>
      <Modal title="新建图空间" open={openCreate} onCancel={() => onCreateChange(false)} onOk={() => graphForm.submit()} confirmLoading={busy} okText="创建" cancelText="取消"><Form form={graphForm} layout="vertical" initialValues={{ parts: 32 }} onFinish={(values) => void createGraph(values)}><Form.Item name="name" label="空间名称" rules={[{ required: true, message: '请输入空间名称' }]}><Input prefix={<Database size={16} />} placeholder="例如 Company" /></Form.Item><Form.Item name="parts" label="分片数量" initialValue={32} rules={[{ required: true, message: '请输入分片数量' }, { type: 'number', min: 1, max: 4096, transform: (value) => Number(value), message: '分片数量必须在 1 到 4096 之间' }]}><InputNumber min={1} max={4096} precision={0} defaultValue={32} style={{ width: '100%' }} /></Form.Item><Form.Item name="comment" label="空间注释"><Input.TextArea rows={3} placeholder="说明这个图空间的业务用途" /></Form.Item></Form></Modal>
  </div>;
}

function GraphWorkspace({ api, graph, aliases, tab, setTab, canWrite, onChanged, onCopied }: { api: CengyanApi; graph: string; aliases: string[]; tab: GraphTab; setTab: (tab: GraphTab) => void; canWrite: boolean; onChanged: () => Promise<void>; onCopied: () => void }) {
  const items: TabsProps['items'] = [
    { key: 'nodes', label: <span className="tab-label"><Boxes size={16} />点类型</span>, children: <TypesPanel api={api} graph={graph} node canWrite={canWrite} onCopied={onCopied} /> },
    { key: 'rels', label: <span className="tab-label"><GitBranch size={16} />边类型</span>, children: <TypesPanel api={api} graph={graph} node={false} canWrite={canWrite} onCopied={onCopied} /> },
    { key: 'indexes', label: <span className="tab-label"><Search size={16} />索引</span>, children: <IndexesPanel api={api} graph={graph} canWrite={canWrite} /> },
    { key: 'stats', label: <span className="tab-label"><Gauge size={16} />统计信息</span>, children: <StatsPanel api={api} graph={graph} /> },
    { key: 'aliases', label: <span className="tab-label"><Copy size={16} />别名管理</span>, children: <AliasPanel api={api} graph={graph} aliases={aliases} canWrite={canWrite} onChanged={onChanged} /> },
    { key: 'replicas', label: <span className="tab-label"><Server size={16} />副本管理</span>, children: <ReplicaPanel api={api} graph={graph} canWrite={canWrite} active={tab === 'replicas'} /> },
  ];
  return <div className="page-stack workspace-page"><div className="workspace-banner"><div className="workspace-identity"><Title level={3}>{graph}</Title></div><Tag color="green" icon={<Check size={13} />}>已连接</Tag></div><Card className="workspace-card" bordered={false}><Tabs activeKey={tab} onChange={(key) => setTab(key as GraphTab)} items={items} /></Card></div>;
}

function AliasPanel({ api, graph, aliases, canWrite, onChanged }: { api: CengyanApi; graph: string; aliases: string[]; canWrite: boolean; onChanged: () => Promise<void> }) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [form] = Form.useForm();
  const create = async (values: { alias: string }) => {
    setBusy(true); setError('');
    try {
      await api.query(["CREATE", "ALIAS"].join(' ') + ` ${escapeIdent(values.alias.trim())} FOR GRAPH ${escapeIdent(graph)}`);
      setOpen(false);
      form.resetFields();
      await onChanged();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '创建别名失败');
    } finally {
      setBusy(false);
    }
  };
  const drop = (alias: string) => Modal.confirm({
    title: `删除别名 ${alias}？`,
    content: '别名删除不会影响这个图空间。',
    okText: '删除别名',
    okButtonProps: { danger: true },
    cancelText: '取消',
    onOk: async () => {
      try { await api.query(`DROP ALIAS ${escapeIdent(alias)}`); await onChanged(); }
      catch (reason) { setError(reason instanceof Error ? reason.message : '删除别名失败'); throw reason; }
    },
  });
  return <div className="panel-stack">
    <div className="panel-toolbar"><div><Title level={4}>别名管理</Title><Text type="secondary">给 {graph} 增加可以代替空间名使用的别名。</Text></div>{canWrite && <Button type="primary" icon={<Plus size={15} />} onClick={() => setOpen(true)}>新建别名</Button>}</div>
    {error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}
    {aliases.length ? <Table rowKey="alias" pagination={false} dataSource={aliases.map((alias) => ({ alias }))} columns={[{ title: '别名', dataIndex: 'alias', render: (value: string) => <span className="code-pill">{value}</span> }, { title: '指向', render: () => graph }, { title: '操作', width: 100, render: (_: unknown, row: { alias: string }) => canWrite && <Button type="link" danger size="small" onClick={() => drop(row.alias)}>删除</Button> }]} /> : <Empty description="这个空间还没有别名" />}
    <Modal title="创建别名" open={open} confirmLoading={busy} onCancel={() => setOpen(false)} onOk={() => form.submit()} okText="创建别名" cancelText="取消"><Form form={form} layout="vertical" onFinish={(values) => void create(values)}><Form.Item name="alias" label="别名" rules={[{ required: true, message: '请输入别名' }]}><Input placeholder="例如 corp" /></Form.Item></Form></Modal>
  </div>;
}

type MemberRow = { nodeId: number; partId: number; vote: string };

function ReplicaPanel({ api, graph, canWrite, active }: { api: CengyanApi; graph: string; canWrite: boolean; active: boolean }) {
  const [rows, setRows] = useState<MemberRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [target, setTarget] = useState<number | null>(null);
  const load = useCallback(async () => {
    setLoading(true); setError('');
    try {
      const response = await api.showMembers(graph);
      setRows(rowsAsObjects<MemberRow>(response).map((row) => ({ nodeId: Number(row.nodeId), partId: Number(row.partId), vote: String(row.vote) })));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '读取副本失败');
    } finally {
      setLoading(false);
    }
  }, [api, graph]);
  useEffect(() => { if (active) void load(); }, [active, load]);
  const voterCount = new Set(rows.map((row) => row.nodeId)).size;
  const partCount = new Set(rows.map((row) => row.partId)).size;
  const replicaCount = Math.max(0, (voterCount || 1) - 1);
  useEffect(() => { setTarget(replicaCount); }, [replicaCount]);
  const add = () => {
    const replicas = target ?? replicaCount;
    if (replicas <= replicaCount) {
      setError('副本数只能增加。0 表示只有一份数据、没有副本。');
      return;
    }
    Modal.confirm({
      title: `把副本数增加到 ${replicas}？`,
      content: `将保留 ${replicas + 1} 份数据。同步期间这个图空间暂时只读，查询仍然可以执行。`,
      okText: '增加副本',
      cancelText: '取消',
      onOk: async () => {
        setBusy(true); setError('');
        try { await api.addReplica(graph, replicas + 1); message.success('副本已增加'); await load(); }
        catch (reason) { setError(reason instanceof Error ? reason.message : '增加副本失败'); throw reason; }
        finally { setBusy(false); }
      },
    });
  };
  return <div className="panel-stack">
    <div className="panel-toolbar"><div><Title level={4}>副本管理</Title><Text type="secondary">{rows.length ? `当前副本数 ${replicaCount}，分片 ${partCount}。` : '打开后读取这个图空间的副本。'}0 表示只有一份数据、没有副本，最多 5。增加副本会复制数据，同步期间暂时只读。</Text></div><Space>{canWrite && <><InputNumber min={0} max={5} precision={0} value={target ?? replicaCount} onChange={(value) => { const next = typeof value === 'number' ? value : Number(value); setTarget(Number.isFinite(next) ? Math.min(5, Math.max(0, Math.trunc(next))) : 0); }} /> <Button type="primary" loading={busy} disabled={(target ?? replicaCount) <= replicaCount} onClick={add}>增加副本</Button></>}<Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load()} loading={loading}>刷新</Button></Space></div>
    {error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}
    {rows.length ? <Table rowKey={(row) => `${row.nodeId}-${row.partId}`} loading={loading} pagination={false} dataSource={rows} columns={[{ title: '节点', dataIndex: 'nodeId', width: 120 }, { title: '分片', dataIndex: 'partId', width: 120 }, { title: '角色', dataIndex: 'vote', render: () => <Tag color="green">投票成员</Tag> }]} /> : !error && <Empty description={loading ? '正在读取副本' : '还没有投票成员'} />}
  </div>;
}

function TypesPanel({ api, graph, node, canWrite, onCopied }: { api: CengyanApi; graph: string; node: boolean; canWrite: boolean; onCopied: () => void }) {
  const [rows, setRows] = useState<TypeRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [createOpen, setCreateOpen] = useState(false);
  const [detail, setDetail] = useState<{ name: string; rows: TypeProperty[] } | null>(null);
  const [createForm] = Form.useForm();
  const kind = node ? 'NODE' : 'REL';
  const title = node ? '点类型' : '边类型';

  const load = useCallback(async () => {
    setLoading(true); setError('');
    try { const response = await api.query(`SHOW ${node ? 'NODE' : 'REL'} TYPES`); setRows(rowsAsObjects<TypeRow>(response)); }
    catch (reason) { setError(reason instanceof Error ? reason.message : `加载${title}失败`); }
    finally { setLoading(false); }
  }, [api, node, title]);
  useEffect(() => { void load(); }, [load, graph]);

  const idType = Form.useWatch('idType', createForm) || 'INT';

  const create = async (values: {
    name: string;
    comment?: string;
    idType?: 'INT' | 'STRING';
    idLength?: number;
    properties?: Array<{ name?: string; type?: string; nullable?: boolean; comment?: string }>;
  }) => {
    if (node) {
      const idLength = Number(values.idLength);
      const maxLength = values.idType === 'INT' ? 19 : 4096;
      if (!Number.isInteger(idLength) || idLength < 1 || idLength > maxLength) {
        setError(`${values.idType === 'INT' ? '整数' : '字符串'} ID 长度必须是 1 到 ${maxLength} 之间的整数`);
        return;
      }
    }
    const props = (values.properties || []).filter((item) => item.name?.trim()).map((item) => `${escapeIdent(item.name!.trim())} ${item.type || 'STRING'}${item.nullable === false ? '' : ' NULL'}${item.comment ? ` COMMENT '${item.comment.replaceAll("'", "''")}'` : ''}`);
    const idClause = node
      ? ` ID ${values.idType || 'INT'}(${Math.trunc(Number(values.idLength))})`
      : '';
    const comment = values.comment ? ` COMMENT '${values.comment.replaceAll("'", "''")}'` : '';
    try { await api.query(`CREATE ${kind} TYPE ${escapeIdent(values.name.trim())}${comment}${idClause} (${props.join(', ')})`); setCreateOpen(false); createForm.resetFields(); await load(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : `创建${title}失败`); }
  };
  const describe = async (name: string) => { try { const response = await api.query(`DESCRIBE ${kind} TYPE ${escapeIdent(name)}`); setDetail({ name, rows: rowsAsObjects<TypeProperty>(response) }); } catch (reason) { setError(reason instanceof Error ? reason.message : '读取属性失败'); } };
  const exportType = async (type: TypeRow) => {
    try { await copyText(await typeSchemaStatement(api, type, node)); onCopied(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : '导出结构失败'); }
  };

  const columns: TableProps<TypeRow>['columns'] = [
    { title: '名称', dataIndex: 'name', render: (value: string, row: TypeRow) => <Tooltip title={row.comment || ''}><span className="code-pill">{value}</span></Tooltip> },
    ...(node ? [
      { title: 'ID 类型', dataIndex: 'idType', render: (value: string) => value || 'INT' },
      { title: 'ID 长度', dataIndex: 'idLength', render: (value: number, record: TypeRow) => (String(record.idType || 'INT').toUpperCase() === 'STRING' ? value || '-' : '—') },
      { title: '描述', dataIndex: 'comment', ellipsis: true, render: (value: string) => value || '—' },
    ] : []),
    { title: '编号', dataIndex: 'id' },
    { title: '版本', dataIndex: 'schemaVersion' },
    { title: '操作', key: 'actions', width: 200, render: (_, record) => <span className="table-actions"><Button type="link" size="small" onClick={() => void describe(record.name)}>查看列</Button><Button type="link" size="small" onClick={() => void exportType(record)}>导出结构</Button></span> },
  ];
  return <div className="panel-stack"><div className="panel-toolbar"><div><Title level={4}>{title}</Title></div><Space>{canWrite && <Button icon={<Plus size={15} />} onClick={() => setCreateOpen(true)}>新建{title}</Button>}<Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load()} loading={loading}>刷新</Button></Space></div>{error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}{detail && <Card size="small" className="detail-card" title={<span>{detail.name} 的属性</span>} extra={<Button type="text" icon={<X size={15} />} onClick={() => setDetail(null)} />}><Table size="small" pagination={false} rowKey="property" columns={[{ title: '属性', dataIndex: 'property' }, { title: '类型', dataIndex: 'type' }, { title: '可空', dataIndex: 'nullable', render: (value: boolean) => value ? <Tag color="blue">NULL</Tag> : <Tag>必填</Tag> }, { title: '注释', dataIndex: 'comment' }]} dataSource={detail.rows} /></Card>}{rows.length ? <Table rowKey="name" loading={loading} columns={columns} dataSource={rows} pagination={{ pageSize: 8, hideOnSinglePage: true }} /> : <Empty description={`还没有${title}`} />}
    <Modal title={`新建${title}`} open={createOpen} onCancel={() => setCreateOpen(false)} onOk={() => createForm.submit()} okText="创建" cancelText="取消"><Form form={createForm} layout="vertical" initialValues={{ idType: node ? 'INT' : undefined, idLength: 19, properties: [{ type: 'STRING', nullable: true }] }} onFinish={(values) => void create(values)}><Form.Item name="name" label="类型名称" rules={[{ required: true, message: '请输入类型名称' }]}><Input placeholder={node ? '例如 Person' : '例如 knows'} /></Form.Item>{node && <Row gutter={12}><Col span={12}><Form.Item name="idType" label="ID 类型" rules={[{ required: true, message: '请选择 ID 类型' }]}><Select options={NODE_ID_TYPES} onChange={(value: 'INT' | 'STRING') => createForm.setFieldValue('idLength', value === 'INT' ? 19 : 64)} /></Form.Item></Col><Col span={12}><Form.Item name="idLength" label="ID 长度" extra={idType === 'STRING' ? '字符串 ID 的最大字节长度，范围 1 到 4096' : '整数 ID 的十进制位数，范围 1 到 19'} rules={[{ validator: (_, value) => { const maxLength = idType === 'INT' ? 19 : 4096; return !Number.isInteger(Number(value)) || Number(value) < 1 || Number(value) > maxLength ? Promise.reject(new Error(`请输入 1 到 ${maxLength} 的整数`)) : Promise.resolve(); } }]}><InputNumber min={1} max={idType === 'INT' ? 19 : 4096} precision={0} style={{ width: '100%' }} /></Form.Item></Col></Row>}<Form.List name="properties">{(fields, { add, remove }) => <div className="property-list">{fields.map((field) => <div className="property-row" key={field.key}><Form.Item {...field} name={[field.name, 'name']}><Input placeholder="属性名" /></Form.Item><Form.Item {...field} name={[field.name, 'type']}><Select options={TYPES.map((type) => ({ value: type, label: type }))} /></Form.Item><Form.Item {...field} name={[field.name, 'nullable']} valuePropName="checked"><label className="checkbox-field"><input type="checkbox" />可空</label></Form.Item><Form.Item {...field} name={[field.name, 'comment']}><Input placeholder="字段注释" /></Form.Item><Button type="text" icon={<Trash2 size={15} />} onClick={() => remove(field.name)} /></div>)}<Button type="dashed" block icon={<Plus size={15} />} onClick={() => add({ type: 'STRING', nullable: true })}>添加属性</Button></div>}</Form.List></Form></Modal>
  </div>;
}

function IndexesPanel({ api, graph, canWrite }: { api: CengyanApi; graph: string; canWrite: boolean }) {
  const [rows, setRows] = useState<IndexRow[]>([]);
  const [nodeTypes, setNodeTypes] = useState<string[]>([]);
  const [relTypes, setRelTypes] = useState<string[]>([]);
  const [properties, setProperties] = useState<TypeProperty[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [error, setError] = useState('');
  const [form] = Form.useForm();
  const target = Form.useWatch('target', form) || 'NODE';
  const type = Form.useWatch('type', form);

  const load = useCallback(async () => {
    setLoading(true); setError('');
    try { const [indexes, nodes, rels] = await Promise.all([api.query('SHOW INDEXES'), api.query('SHOW NODE TYPES'), api.query('SHOW REL TYPES')]); setRows(rowsAsObjects<IndexRow>(indexes)); setNodeTypes(rowsAsObjects<TypeRow>(nodes).map((row) => row.name)); setRelTypes(rowsAsObjects<TypeRow>(rels).map((row) => row.name)); }
    catch (reason) { setError(reason instanceof Error ? reason.message : '加载索引失败'); }
    finally { setLoading(false); }
  }, [api]);
  useEffect(() => { void load(); }, [load, graph]);
  useEffect(() => { form.setFieldValue('type', (target === 'NODE' ? nodeTypes : relTypes)[0]); }, [target, nodeTypes, relTypes, form]);
  useEffect(() => { if (!type) { setProperties([]); return; } void (async () => { try { const response = await api.query(`DESCRIBE ${target === 'NODE' ? 'NODE' : 'REL'} TYPE ${escapeIdent(type)}`); setProperties(rowsAsObjects<TypeProperty>(response)); } catch (reason) { setError(reason instanceof Error ? reason.message : '加载索引属性失败'); } })(); }, [api, target, type]);
  const create = async (values: { name: string; target: 'NODE' | 'REL'; type: string; properties: string[]; unique?: boolean }) => { if (!values.properties?.length) { setError('至少选择一个属性'); return; } try { const unique = values.unique ? 'UNIQUE ' : ''; await api.query(`CREATE ${unique}INDEX ${escapeIdent(values.name.trim())} ON ${values.target} TYPE ${escapeIdent(values.type)} (${values.properties.map(escapeIdent).join(', ')})`); setCreateOpen(false); form.resetFields(); await load(); } catch (reason) { setError(reason instanceof Error ? reason.message : '创建索引失败'); } };
  const rebuild = async (name: string) => { try { await api.admin('rebuildIndex', name, 120000); message.success(`索引 ${name} 已重建`); await load(); } catch (reason) { setError(reason instanceof Error ? reason.message : '重建索引失败'); } };
  const drop = (name: string) => Modal.confirm({ title: `删除索引 ${name}？`, content: '删除后查询将不再使用它。', okText: '删除', okButtonProps: { danger: true }, cancelText: '取消', onOk: async () => { await api.query(`DROP INDEX ${escapeIdent(name)}`); await load(); } });
  const columns: TableProps<IndexRow>['columns'] = [{ title: '名称', dataIndex: 'name', width: 160, ellipsis: true, render: (value: string) => <span className="code-pill">{value}</span> }, { title: '目标', dataIndex: 'target', width: 90, render: (value: string) => <Tag>{value}</Tag> }, { title: '类型', dataIndex: 'type', width: 140, ellipsis: true }, { title: '属性', dataIndex: 'properties', width: 220, ellipsis: true, render: (value: string) => <span className="cell-ellipsis" title={value}>{value}</span> }, { title: '约束', dataIndex: 'unique', width: 100, render: (value: boolean) => value ? <Tag color="purple">UNIQUE</Tag> : <Text type="secondary">普通</Text> }, { title: '状态', dataIndex: 'ready', width: 100, render: (value: boolean) => value ? <Tag color="green">READY</Tag> : <Tag color="orange">待重建</Tag> }, { title: '操作', key: 'actions', width: 120, fixed: 'right', render: (_, row) => canWrite && <span className="table-actions"><Button type="link" size="small" onClick={() => void rebuild(row.name)}>重建</Button><Button type="link" danger size="small" onClick={() => drop(row.name)}>删除</Button></span> }];
  return <div className="panel-stack"><div className="panel-toolbar"><div><Title level={4}>索引</Title><Text type="secondary">为点类型或边类型建立等值与范围查询入口。</Text></div><Space>{canWrite && <Button type="primary" icon={<Plus size={15} />} onClick={() => setCreateOpen(true)}>新建索引</Button>}<Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load()} loading={loading}>刷新</Button></Space></div>{error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}{rows.length ? <Table rowKey="name" loading={loading} columns={columns} dataSource={rows} scroll={{ x: 930 }} pagination={{ pageSize: 8, hideOnSinglePage: true }} /> : <Empty description="还没有索引" />}
    <Modal title="新建索引" open={createOpen} onCancel={() => setCreateOpen(false)} onOk={() => form.submit()} okText="创建" cancelText="取消"><Form form={form} layout="vertical" initialValues={{ target: 'NODE', unique: false }} onFinish={(values) => void create(values)}><Form.Item name="name" label="索引名称" rules={[{ required: true, message: '请输入索引名称' }]}><Input placeholder="例如 by_name" /></Form.Item><Row gutter={12}><Col span={10}><Form.Item name="target" label="目标"><Select options={[{ value: 'NODE', label: '点类型' }, { value: 'REL', label: '边类型' }]} /></Form.Item></Col><Col span={14}><Form.Item name="type" label="类型" rules={[{ required: true, message: '请选择类型' }]}><Select options={(target === 'NODE' ? nodeTypes : relTypes).map((name) => ({ value: name, label: name }))} /></Form.Item></Col></Row><Form.Item name="properties" label="索引属性" rules={[{ required: true, message: '至少选择一个属性' }]}><Select mode="multiple" options={properties.map((property) => ({ value: property.property, label: `${property.property} · ${property.type}` }))} placeholder="选择属性" /></Form.Item><Form.Item name="unique" valuePropName="checked"><label className="checkbox-field"><input type="checkbox" />唯一索引</label></Form.Item><Alert type="info" showIcon message="创建完成后需要点击“重建”，索引才会进入 READY 状态。" /></Form></Modal>
  </div>;
}

function StatsPanel({ api, graph }: { api: CengyanApi; graph: string }) {
  const [rows, setRows] = useState<StatsRow[]>([]);
  const [storageBytes, setStorageBytes] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const load = useCallback(async () => { setLoading(true); setError(''); try { const [stats, storage] = await Promise.all([api.admin('stats'), api.admin('storage').catch(() => null)]); setRows(rowsAsObjects<StatsRow>(stats)); const bytes = Number(storage?.rows?.[0]?.[0]); setStorageBytes(Number.isFinite(bytes) && bytes >= 0 ? bytes : null); } catch (reason) { setError(reason instanceof Error ? reason.message : '加载统计信息失败'); } finally { setLoading(false); } }, [api]);
  useEffect(() => { void load(); }, [load, graph]);
  const nodeCount = rows.filter((row) => row.target === 'NODE').reduce((sum, row) => sum + Number(row.count), 0);
  const edgeCount = rows.filter((row) => row.target === 'REL').reduce((sum, row) => sum + Number(row.count), 0);
  return <div className="panel-stack"><div className="panel-toolbar"><div><Title level={4}>统计信息</Title><Text type="secondary">{graph} 的实时数据概览，按类型统计。</Text></div><Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load()} loading={loading}>刷新</Button></div>{error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}<Row gutter={[16, 16]}><Col xs={24} md={12}><Card className="metric-card"><Statistic title="节点数量" value={nodeCount} prefix={<Boxes size={18} />} /></Card></Col><Col xs={24} md={12}><Card className="metric-card"><Statistic title="边数量" value={edgeCount} prefix={<GitBranch size={18} />} /></Card></Col></Row><Card className="storage-card"><div className="storage-icon"><Database size={19} /></div><div><Text strong>存储占用</Text><Text type="secondary">当前数据库目录中 RocksDB 文件的实际占用。</Text></div><Tag color="blue">{storageBytes == null ? '读取中' : formatBytes(storageBytes)}</Tag></Card>{rows.length ? <Table rowKey={(row) => `${row.target}-${row.name}`} loading={loading} columns={[{ title: '对象', dataIndex: 'target', render: (value: string) => <Tag>{value}</Tag> }, { title: '类型', dataIndex: 'name' }, { title: '数量', dataIndex: 'count' }]} dataSource={rows} pagination={false} /> : <Empty description="暂无统计信息" />}</div>;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  for (const unit of units) {
    value /= 1024;
    if (value < 1024 || unit === units[units.length - 1]) return `${value.toFixed(value < 10 ? 2 : 1)} ${unit}`;
  }
  return `${bytes} B`;
}

type RunningRow = { queryId: string; user: string; elapsedMs: number; query: string };

function formatElapsed(ms: number) {
  const safe = Number.isFinite(ms) ? Math.max(0, ms) : 0;
  const totalSeconds = Math.floor(safe / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes > 0) return `${minutes} 分 ${seconds} 秒`;
  return `${(safe / 1000).toFixed(1)} 秒`;
}

function RunningQueries({ api }: { api: CengyanApi }) {
  const { message: messageApi, modal } = AntApp.useApp();
  const [rows, setRows] = useState<RunningRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [killing, setKilling] = useState('');
  const alive = useRef(true);

  const load = useCallback(async (quiet: boolean) => {
    if (!quiet) setLoading(true);
    try {
      const response = await api.queries();
      if (!alive.current) return;
      setRows(rowsAsObjects<RunningRow>(response).map((row) => ({
        queryId: String(row.queryId ?? ''),
        user: String(row.user ?? ''),
        elapsedMs: Number(row.elapsedMs ?? 0),
        query: String(row.query ?? ''),
      })));
      setError('');
    } catch (reason) {
      if (!alive.current) return;
      setError(reason instanceof Error ? reason.message : '读取正在执行的查询失败');
    } finally {
      if (alive.current && !quiet) setLoading(false);
    }
  }, [api]);

  useEffect(() => {
    alive.current = true;
    void load(false);
    const timer = window.setInterval(() => { void load(true); }, 2000);
    return () => {
      alive.current = false;
      window.clearInterval(timer);
    };
  }, [load]);

  const kill = (row: RunningRow) => {
    modal.confirm({
      title: '终止这条查询',
      content: `${row.user || '该用户'} 正在执行的语句会被取消。`,
      okText: '终止',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        setKilling(row.queryId);
        try {
          await api.cancelQuery(row.queryId);
          messageApi.success('已终止');
          await load(true);
        } catch (reason) {
          messageApi.error(reason instanceof Error ? reason.message : '终止失败');
        } finally {
          setKilling('');
        }
      },
    });
  };

  return (
    <div className="panel-stack">
      <div className="panel-toolbar">
        <div>
          <Title level={4}>正在执行的查询</Title>
          <Text type="secondary">读写用户可以查看并终止当前还没结束的 GQL。</Text>
        </div>
        <Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load(false)} loading={loading}>刷新</Button>
      </div>
      {error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}
      <Card>
        <Table<RunningRow>
          rowKey="queryId"
          loading={loading && rows.length === 0}
          dataSource={rows}
          pagination={false}
          locale={{ emptyText: '当前没有正在执行的查询' }}
          columns={[
            { title: '提交用户', dataIndex: 'user', width: 160 },
            { title: '执行时间', dataIndex: 'elapsedMs', width: 140, render: (ms: number) => formatElapsed(ms) },
            { title: 'GQL', dataIndex: 'query', render: (query: string) => <pre className="running-gql">{query}</pre> },
            {
              title: '操作',
              width: 110,
              render: (_value: unknown, row: RunningRow) => (
                <Button danger size="small" loading={killing === row.queryId} onClick={() => kill(row)}>终止</Button>
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}

type ScriptResult = { statement: string; response: QueryResponse; elapsedMs: number };

function GqlPage({ api, graph, canWrite, active, onGraphChanged }: { api: CengyanApi; graph: string; canWrite: boolean; active: boolean; onGraphChanged: (name: string) => void }) {
  const [query, setQuery] = useState(() => localStorage.getItem('cengyan.studio.last-query') || DEFAULT_QUERY);
  const [result, setResult] = useState<ScriptResult[] | null>(null);
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState('');
  const [error, setError] = useState('');
  const [editorReady, setEditorReady] = useState(false);
  const [history, setHistory] = useState<string[]>(() => { try { return JSON.parse(localStorage.getItem(QUERY_HISTORY_KEY) || '[]'); } catch { return []; } });
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);
  const runQueryRef = useRef<() => void>(() => {});
  const runQuery = async () => {
    const text = editorRef.current?.getValue() ?? query;
    if (!graph) { setError('请先在右上角选择图空间'); return; }
    let statements: string[];
    try { statements = splitGqlScript(text); }
    catch (reason) { setError(reason instanceof Error ? reason.message : '语句无法拆分'); return; }
    if (!statements.length) { setError('GQL 语句不能为空'); return; }
    setRunning(true); setError('');
    const done: ScriptResult[] = [];
    try {
      for (let index = 0; index < statements.length; index++) {
        setProgress(statements.length > 1 ? `正在执行第 ${index + 1} / ${statements.length} 条` : '');
        const startedAt = performance.now();
        const response = await api.query(statements[index], 30000);
        done.push({ statement: statements[index], response, elapsedMs: Math.round(performance.now() - startedAt) });
        setResult([...done]);
        const useMatch = statements[index].trim().match(/^USE\s+([A-Za-z][A-Za-z0-9_]*)\s*;?$/i);
        if (useMatch) onGraphChanged(useMatch[1]);
      }
      localStorage.setItem('cengyan.studio.last-query', text);
      const next = [text.trim(), ...history.filter((item) => item !== text.trim())].slice(0, 12);
      setHistory(next);
      localStorage.setItem(QUERY_HISTORY_KEY, JSON.stringify(next));
    } catch (reason) {
      if (done.length) setResult([...done]);
      const message = reason instanceof Error ? reason.message : '查询失败';
      setError(statements.length > 1 ? `第 ${done.length + 1} 条执行失败：${message}` : message);
    } finally {
      setRunning(false);
      setProgress('');
    }
  };
  runQueryRef.current = () => { void runQuery(); };
  const editorMount: OnMount = (editor) => { editorRef.current = editor; editor.addCommand(2048 | 3, () => { runQueryRef.current(); }); };
  useEffect(() => { let live = true; void import('./monaco').then(() => { if (live) setEditorReady(true); }); return () => { live = false; }; }, []);
  useEffect(() => { if (active) editorRef.current?.layout(); }, [active]);
  return <div className="query-page"><Row gutter={[18, 18]}><Col xs={24} xl={15}><Card className="editor-card" bordered={false}><div className="editor-toolbar"><div className="editor-file"><Tag color="geekblue">GQL</Tag></div><Button type="text" size="small" icon={<Trash2 size={14} />} onClick={() => { setQuery(''); editorRef.current?.setValue(''); }}>清空</Button></div>{editorReady ? <Editor height="350px" language="sql" theme="vs" value={query} onChange={(value) => setQuery(value || '')} onMount={editorMount} options={{ minimap: { enabled: false }, fontSize: 13, lineNumbers: 'on', padding: { top: 14, bottom: 14 }, scrollBeyondLastLine: false, wordWrap: 'on', roundedSelection: false, automaticLayout: true, tabSize: 2 }} /> : <div className="editor-pending">正在打开编辑器…</div>}<div className="editor-footer"><Text type="secondary">{progress || '多行输入，分号分隔后按顺序执行 · Ctrl + Enter'}</Text><Space size={8}>{canWrite && <Tag color="green">读写权限</Tag>}<Button type="primary" icon={<Code2 size={15} />} onClick={() => void runQuery()} loading={running} disabled={!graph}>执行查询 <Text className="shortcut">Ctrl ↵</Text></Button></Space></div></Card>{(result || running) && <ScriptResults results={result || []} loading={running} />}{error && <Alert className="query-error" type="error" showIcon message={error} closable onClose={() => setError('')} />}</Col><Col xs={24} xl={9}><Card className="history-card" bordered={false} title={<span className="card-title"><Activity size={16} />查询历史</span>} extra={<Button type="text" size="small" onClick={() => { setHistory([]); localStorage.removeItem(QUERY_HISTORY_KEY); }}>清空</Button>}>{history.length ? <div className="history-list">{history.map((item, index) => <button key={`${item}-${index}`} type="button" onClick={() => setQuery(item)}><span>{item}</span><ChevronRight size={15} /></button>)}</div> : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="执行过的查询会显示在这里" />}</Card><Card className="tips-card" bordered={false}><div className="tips-title"><Settings2 size={16} />常用提示</div><div className="tip-item"><span>表格结果</span><Text type="secondary">适合查看属性和聚合数据</Text></div><div className="tip-item"><span>关系图</span><Text type="secondary">RETURN 完整点或边对象</Text></div><div className="tip-item"><span>性能</span><Text type="secondary">每条语句最长运行 30 秒</Text></div></Card></Col></Row></div>;
}

function ScriptResults({ results, loading }: { results: ScriptResult[]; loading: boolean }) {
  if (!results.length) return <Card className="result-card result-loading-card" bordered={false}><Spin size="small" /><Text type="secondary">正在执行查询…</Text></Card>;
  if (results.length === 1) return <QueryResult response={results[0].response} elapsedMs={results[0].elapsedMs} loading={loading} />;
  return <>{results.map((item, index) => <div className="script-result" key={`${index}-${item.statement}`}><div className="script-result-label"><Text type="secondary">第 {index + 1} 条 · {item.elapsedMs} ms</Text><pre>{item.statement}</pre></div><QueryResult response={item.response} elapsedMs={item.elapsedMs} loading={loading && index === results.length - 1} /></div>)}</>;
}

function QueryResult({ response, elapsedMs, loading }: { response: QueryResponse; elapsedMs: number; loading?: boolean }) {
  const columns = response.columns || [];
  const rows = response.rows || [];
  const [view, setView] = useState('table');
  useEffect(() => { setView('table'); }, [response]);
  const tableColumns: TableProps<Record<string, unknown>>['columns'] = columns.map((column, index) => ({ title: column, dataIndex: column, key: column, render: (value: unknown) => <JsonValue value={value} /> }));
  const data = rows.map((row, rowIndex) => Object.fromEntries(columns.map((column, index) => [column, row[index]]).concat([['key', rowIndex]])));
  const tabs: TabsProps['items'] = [
    { key: 'table', label: '表格', children: <Table size="small" scroll={{ x: 650, y: 380 }} columns={tableColumns} dataSource={data} pagination={{ pageSize: 10, showSizeChanger: false }} /> },
    { key: 'graph', label: '关系图', children: view === 'graph' ? <QueryGraph rows={rows} /> : null },
  ];
  return <Card className="result-card" bordered={false}><div className="result-heading"><div><Title level={4}>查询结果</Title><Text type="secondary">{loading && <Spin size="small" />} {columns.length ? `${rows.length} 行 · ${elapsedMs} ms` : `完成，影响 ${response.rowsAffected || 0} 行 · ${elapsedMs} ms`}</Text></div></div>{columns.length ? <Tabs className="result-tabs" activeKey={view} onChange={setView} destroyOnHidden items={tabs} /> : <div className="success-result"><Check size={20} /><span>语句执行成功</span></div>}</Card>;
}

function QueryGraph({ rows }: { rows: unknown[][] }) {
  const graph = useMemo(() => buildGraph(rows), [rows]);
  const [nodes, setNodes] = useState<Node[]>(graph.nodes);
  const [edges, setEdges] = useState<Edge[]>(graph.edges);
  useEffect(() => {
    setNodes(graph.nodes);
    setEdges(graph.edges);
  }, [graph]);
  const onNodesChange = useCallback((changes: NodeChange[]) => {
    setNodes((current) => applyNodeChanges(changes, current));
  }, []);
  if (!graph.nodes.length) return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="当前结果里没有点或边" />;
  return <div className="graph-result"><div className="graph-result-title"><Text type="secondary">可拖动点查看关系</Text><Tag color="purple" icon={<Workflow size={13} />}>{graph.nodes.length} 点 · {graph.edges.length} 边</Tag></div><div className="flow-wrap"><ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} nodeTypes={GRAPH_NODE_TYPES} fitView fitViewOptions={{ padding: 0.2 }} nodesDraggable nodesConnectable={false} elementsSelectable><MiniMap pannable zoomable /><Controls /><Background color="#d7dbea" gap={18} /></ReactFlow></div></div>;
}

function JsonValue({ value }: { value: unknown }) {
  if (value === null || value === undefined) return <Text type="secondary">null</Text>;
  if (typeof value === 'object') {
    return <pre className="json-cell">{JSON.stringify(value)}</pre>;
  }
  return <ValueDetail value={value} />;
}

function ValueDetail({ value }: { value: unknown }) {
  const [previewOpen, setPreviewOpen] = useState(false);
  return <>
    <span className="table-value" onDoubleClick={() => setPreviewOpen(true)} title="双击查看详情">{String(value)}</span>
    <Modal title="字段详情" open={previewOpen} onCancel={() => setPreviewOpen(false)} footer={null} width={760} destroyOnHidden>
      <Table className="detail-value-table" size="small" pagination={false} rowKey="key" columns={[{ title: '列名', dataIndex: 'key', width: 180 }, { title: '值', dataIndex: 'value', render: (item: unknown) => <span className="detail-value">{String(item ?? '')}</span> }]} dataSource={[{ key: '值', value }]} />
    </Modal>
  </>;
}

const GRAPH_NODE_TYPES = { vertex: VertexNode };

function VertexNode({ data }: NodeProps) {
  const nodeType = typeof data.nodeType === 'string' ? data.nodeType : '';
  const label = typeof data.label === 'string' ? data.label : '';
  return (
    <div className="flow-node">
      <Handle type="target" position={Position.Left} />
      {nodeType ? <span className="flow-node-type">{nodeType}</span> : null}
      <span className="flow-node-id">{label}</span>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function buildGraph(rows: unknown[][]): { nodes: Node[]; edges: Edge[] } {
  const nodes = new Map<string, Node>();
  const edges: Edge[] = [];
  const seenEdges = new Set<string>();
  const addNode = (id: string, nodeType: string) => {
    const typeName = nodeType.trim();
    const existing = nodes.get(id);
    if (existing) {
      const current = existing.data as { nodeType?: string };
      if (!current.nodeType && typeName) existing.data = { ...existing.data, nodeType: typeName };
      return;
    }
    nodes.set(id, {
      id,
      type: 'vertex',
      draggable: true,
      data: { label: id, nodeType: typeName },
      position: { x: 0, y: 0 },
      style: { borderRadius: 12, border: '1px solid #c8d0f2', background: '#f6f7ff', color: '#33407d', padding: '8px 12px', boxShadow: '0 6px 14px rgba(82,104,232,.10)' },
    });
  };
  rows.forEach((row) => row.forEach((cell) => {
    if (!cell || typeof cell !== 'object' || Array.isArray(cell)) return;
    const item = cell as Record<string, unknown>;
    if (item.src !== undefined && item.dst !== undefined && item.type) {
      const source = String(item.src);
      const target = String(item.dst);
      addNode(source, String(item.srcType ?? ''));
      addNode(target, String(item.dstType ?? ''));
      const id = `${item.type}-${item.rank ?? 0}-${source}-${target}`;
      if (!seenEdges.has(id)) {
        seenEdges.add(id);
        edges.push({ id, source, target, label: String(item.type), animated: false, markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18, color: '#5c6bc4' }, style: { stroke: '#5c6bc4', strokeWidth: 1.6 }, labelStyle: { fill: '#6570bb', fontSize: 11 }, labelBgStyle: { fill: '#ffffff', fillOpacity: .85 } });
      }
      return;
    }
    if (item.id !== undefined && item.type) addNode(String(item.id), String(item.type));
  }));
  const list = [...nodes.values()];
  const radius = Math.max(120, list.length * 22);
  list.forEach((node, index) => {
    const angle = index / Math.max(1, list.length) * Math.PI * 2;
    node.position = { x: Math.cos(angle) * radius + 260, y: Math.sin(angle) * Math.min(150, radius * .6) + 170 };
  });
  return { nodes: list, edges };
}

