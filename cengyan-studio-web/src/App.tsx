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
  Download,
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
  Table2,
  Trash2,
  Workflow,
  X,
} from 'lucide-react';
import { CengyanApi, escapeIdent, graphFromRows, rowsAsObjects, storage } from './api';
import { ClusterApi } from './clusterApi';
import { ClusterConsole } from './cluster/ClusterConsole';
import { QueryGraph } from './QueryGraph';
import type { GraphSpace, IndexRow, OpsOverview, QueryResponse, SessionInfo, StatsRow, TypeProperty, TypeRow } from './types';

const { Header, Sider, Content } = Layout;
const { Title, Text, Paragraph } = Typography;

const TYPES = ['BOOL', 'INT64', 'DOUBLE', 'STRING', 'DATETIME'];
const NODE_ID_TYPES = [
  { value: 'INT', label: 'INT（整数）' },
  { value: 'STRING', label: 'STRING（字符串）' },
];
const DEFAULT_QUERY = '';
const LEGACY_SAMPLE_QUERY = "MATCH (a:Person)-[e:knows]->(b) RETURN a, e, b";
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

type NavKey = 'graph' | 'gql' | 'running' | 'ops' | 'cluster';
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
    }, handleLogout);
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
  const [inflightQuery, setInflightQuery] = useState('');
  const [graphMaxNodes, setGraphMaxNodes] = useState(2000);
  const [tableMaxNodes, setTableMaxNodes] = useState(2000);
  const opsRefresh = useRef<() => void>(() => {});
  const [opsRefreshing, setOpsRefreshing] = useState(false);
  const bindOpsRefresh = useCallback((refresh: () => void, refreshing: boolean) => {
    opsRefresh.current = refresh;
    setOpsRefreshing(refreshing);
  }, []);
  const [nodeHealth, setNodeHealth] = useState({ storageAlive: 0, storageTotal: 0, computeAlive: 0, computeTotal: 0, loading: true });
  const [clusterManaged, setClusterManaged] = useState(false);
  const [standaloneCluster, setStandaloneCluster] = useState(false);
  const clusterApiRef = useRef<ClusterApi | undefined>(undefined);
  if (!clusterApiRef.current) clusterApiRef.current = new ClusterApi();
  const clusterApi = clusterApiRef.current;
  const [messageApi, contextHolder] = message.useMessage();
  const [notificationApi, notificationContextHolder] = notification.useNotification();

  function handleLogout() {
    storage.clearSession();
    setSession(null);
    setCurrentGraph('');
    setSpaces([]);
  }

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
      if (messageText.includes('FORBIDDEN') || messageText.includes('请先登录')) {
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

  const refreshNodeHealth = useCallback(async () => {
    if (!session) return;
    try {
      const overview = await api.opsOverview();
      const storageNodes = overview.storageNodes || [];
      const computeNodes = overview.computeNodes || [];
      setNodeHealth({
        storageAlive: storageNodes.filter((row) => row.state !== 'UNAVAILABLE').length,
        storageTotal: storageNodes.length,
        computeAlive: computeNodes.filter((row) => row.state !== 'UNAVAILABLE').length,
        computeTotal: computeNodes.length,
        loading: false,
      });
      return;
    } catch {
      setNodeHealth((current) => ({ ...current, loading: false }));
    }
  }, [api, session]);

  useEffect(() => {
    void refreshNodeHealth();
    const timer = window.setInterval(() => void refreshNodeHealth(), 5000);
    return () => window.clearInterval(timer);
  }, [refreshNodeHealth]);

  useEffect(() => {
    if (!session) return undefined;
    let active = true;
    const validate = () => {
      if (active) void api.checkSession().catch(() => {
        // 网络暂时不可用时保留当前页面；FORBIDDEN 会由 API 统一触发登出。
      });
    };
    validate();
    const timer = window.setInterval(validate, 60_000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [api, session]);

  useEffect(() => {
    void api.config().then((config) => {
      setGraphMaxNodes(config.graphMaxNodes);
      setTableMaxNodes(config.tableMaxNodes);
      setClusterManaged(Boolean(config.clusterManaged));
    }).catch(() => {
      // 使用前端默认值，配置接口不可用时不影响查询工作台。
    });
  }, [api]);

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
     }, {
      key: 'ops',
      icon: <Server size={18} />,
      label: <span className="menu-label"><span>集群运维</span><small>实时资源与拓扑</small></span>,
    }] : []),
    ...(clusterManaged ? [{
      key: 'cluster',
      icon: <FolderKanban size={18} />,
      label: <span className="menu-label"><span>集群管理</span><small>服务器、节点与配置</small></span>,
    }] : []),
  ];

  if (!session && standaloneCluster) {
    return (
      <ConfigProvider theme={themeConfig}>
        <AntApp>
          <Layout className="studio-layout cluster-standalone">
            <Header className="studio-header">
              <div className="brand-block"><div className="brand-mark"><Layers3 size={21} strokeWidth={2.2} /></div><div><div className="brand-name">层岩 <span>Studio</span></div><div className="brand-caption">集群管理</div></div></div>
              <Button onClick={() => setStandaloneCluster(false)}>返回数据库登录</Button>
            </Header>
            <Content className="studio-content"><ClusterConsole api={clusterApi} /></Content>
          </Layout>
        </AntApp>
      </ConfigProvider>
    );
  }

  if (!session) {
    return (
      <ConfigProvider theme={themeConfig}>
        <AntApp>
          <LoginPage api={api} onLogin={handleLogin} onCluster={clusterManaged ? () => setStandaloneCluster(true) : undefined} />
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
                 <div><Text strong>集群节点状态</Text><Text type="secondary">存储: {nodeHealth.loading ? '读取中' : `${nodeHealth.storageAlive}/${nodeHealth.storageTotal}`}</Text><Text type="secondary">计算: {nodeHealth.loading ? '读取中' : `${nodeHealth.computeAlive}/${nodeHealth.computeTotal}`}</Text></div>
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
                  <Title level={3}>{activeNav === 'graph' ? (graphView === 'workspace' ? '图空间详情' : '图空间') : activeNav === 'running' ? '正在执行' : activeNav === 'ops' ? '集群运维' : activeNav === 'cluster' ? '集群管理' : '查询工作台'}</Title>
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
                {activeNav === 'ops' && (
                  <Button icon={<RefreshCw size={15} />} loading={opsRefreshing} onClick={() => opsRefresh.current()}>刷新</Button>
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
                    <GqlPage api={api} graph={currentGraph} canWrite={writable} active={activeNav === 'gql'} graphMaxNodes={graphMaxNodes} tableMaxNodes={tableMaxNodes} onGraphChanged={switchGraphFromEditor} onInflight={setInflightQuery} />
                </div>
              )}
              {activeNav === 'running' && writable && <RunningQueries api={api} inflight={inflightQuery} />}
              {activeNav === 'ops' && writable && <OpsDashboard api={api} bindRefresh={bindOpsRefresh} />}
              {activeNav === 'cluster' && clusterManaged && <ClusterConsole api={clusterApi} />}
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

function LoginPage({ api, onLogin, onCluster }: { api: CengyanApi; onLogin: (info: SessionInfo) => Promise<void>; onCluster?: () => void }) {
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
            <Alert
              className={error ? 'login-status' : 'login-status is-empty'}
              type={error.startsWith('连接正常') ? 'success' : 'error'}
              showIcon
              icon={error.startsWith('连接正常') ? <Check size={16} /> : <AlertCircle size={16} />}
              message={error || ' '}
            />
            <Button type="primary" htmlType="submit" size="large" block loading={loading}>登录</Button>
            <Button className="login-check" type="text" block loading={pinging} onClick={async () => { setPinging(true); setError(''); try { await api.ping(); setError('连接正常，可以登录'); } catch (reason) { setError(reason instanceof Error ? reason.message : '连接失败'); } finally { setPinging(false); } }}>检查连接</Button>
            {onCluster && <Button className="login-cluster" block icon={<FolderKanban size={16} />} onClick={onCluster}>集群管理：添加服务器和节点</Button>}
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
          <div className="space-card-aliases"><Space wrap>{space.aliases.map((alias) => <Tag key={alias} color="blue">{alias}</Tag>)}</Space></div>
          <div className="space-card-bottom"><div className="space-card-actions"><Button className="space-card-action-button" variant="outlined" color="primary" size="small" icon={<Copy size={14} />} onClick={(event) => { event.stopPropagation(); void exportSchema(space.name); }}>导出 Schema</Button>{canWrite && <Button className="space-card-action-button" variant="outlined" color="danger" size="small" icon={<Trash2 size={14} />} onClick={(event) => { event.stopPropagation(); dropGraph(space.name); }}>删除</Button>}</div></div>
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
    { title: '名称', dataIndex: 'name', width: 200, fixed: 'left', ellipsis: true, render: (value: string, row: TypeRow) => <Tooltip title={row.comment || value}><span className="code-pill">{value}</span></Tooltip> },
    ...(node ? [
      { title: 'ID 类型', dataIndex: 'idType', width: 120, render: (value: string) => value || 'INT' },
      { title: 'ID 长度', dataIndex: 'idLength', width: 120, render: (value: number, record: TypeRow) => (String(record.idType || 'INT').toUpperCase() === 'STRING' ? value || '-' : '—') },
      { title: '描述', dataIndex: 'comment', width: 240, ellipsis: true, render: (value: string) => value || '—' },
    ] : [
      { title: '边描述', dataIndex: 'comment', width: 240, ellipsis: true, render: (value: string) => value || '—' },
    ]),
    { title: '编号', dataIndex: 'id', width: 100 },
    { title: '版本', dataIndex: 'schemaVersion', width: 100 },
    { title: '操作', key: 'actions', width: 200, render: (_, record) => <span className="table-actions"><Button type="link" size="small" onClick={() => void describe(record.name)}>查看列</Button><Button type="link" size="small" onClick={() => void exportType(record)}>导出结构</Button></span> },
  ];
  const tableScrollX = columns.reduce((total, column) => total + Number(column.width ?? 0), 0);
  return <div className="panel-stack"><div className="panel-toolbar"><div><Title level={4}>{title}</Title></div><Space>{canWrite && <Button icon={<Plus size={15} />} onClick={() => setCreateOpen(true)}>新建{title}</Button>}<Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load()} loading={loading}>刷新</Button></Space></div>{error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}{detail && <Card size="small" className="detail-card" title={<span>{detail.name} 的属性</span>} extra={<Button type="text" icon={<X size={15} />} onClick={() => setDetail(null)} />}><Table size="small" pagination={false} rowKey="property" columns={[{ title: '属性', dataIndex: 'property' }, { title: '类型', dataIndex: 'type' }, { title: '可空', dataIndex: 'nullable', render: (value: boolean) => value ? <Tag color="blue">NULL</Tag> : <Tag>必填</Tag> }, { title: '注释', dataIndex: 'comment' }]} dataSource={detail.rows} /></Card>}{rows.length ? <Table rowKey="name" loading={loading} columns={columns} dataSource={rows} scroll={{ x: tableScrollX }} pagination={{ pageSize: 8, hideOnSinglePage: true }} /> : <Empty description={`还没有${title}`} />}
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
  const [countedAt, setCountedAt] = useState<number | null>(null);
  const [storageBytes, setStorageBytes] = useState<number | null>(null);
  const [storageAt, setStorageAt] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [storageLoading, setStorageLoading] = useState(false);
  const requestSequence = useRef(0);
  const load = useCallback(async (refresh: boolean) => {
    const sequence = ++requestSequence.current;
    setLoading(true);
    setError('');
    try {
      const stats = await api.admin('stats', '', undefined, refresh);
      if (sequence !== requestSequence.current) return;
      setRows(rowsAsObjects<StatsRow>(stats));
      setCountedAt(typeof stats.collectedAt === 'number' ? stats.collectedAt : null);
      // 数量来自分片计数元数据，先展示；文件大小查询较慢时不再阻塞节点/边数量。
      setStorageLoading(true);
      void api.admin('storage').then((storageResult) => {
        if (sequence !== requestSequence.current) return;
        const bytes = Number(storageResult?.rows?.[0]?.[0]);
        setStorageBytes(Number.isFinite(bytes) && bytes >= 0 ? bytes : null);
        setStorageAt(typeof storageResult?.collectedAt === 'number' ? storageResult.collectedAt : null);
      }).catch(() => {
        if (sequence === requestSequence.current) setStorageBytes(null);
      }).finally(() => {
        if (sequence === requestSequence.current) setStorageLoading(false);
      });
    } catch (reason) {
      if (sequence !== requestSequence.current) return;
      setError(reason instanceof Error ? reason.message : '加载统计信息失败');
    } finally {
      if (sequence === requestSequence.current) setLoading(false);
    }
  }, [api]);
  useEffect(() => {
    setRows([]);
    setCountedAt(null);
    setStorageBytes(null);
    setStorageLoading(false);
    void load(false);
    return () => { requestSequence.current++; };
  }, [load, graph]);
  const nodeCount = rows.filter((row) => row.target === 'NODE').reduce((sum, row) => sum + Number(row.count), 0);
  const edgeCount = rows.filter((row) => row.target === 'REL').reduce((sum, row) => sum + Number(row.count), 0);
  const countValue = countedAt == null ? '—' : undefined;
  return <div className="panel-stack">
    <div className="panel-toolbar">
      <div>
        <Title level={4}>统计信息</Title>
        <Text type="secondary">{graph} 按类型统计，直接汇总分片持久化计数，不扫描图数据。并发事务收尾期间计数可能短暂滞后。</Text>
        <div className="stats-cache-time"><Text type="secondary">计数读取时间：{loading && countedAt == null ? '读取中' : countedAt == null ? '尚未读取' : formatCollectedAt(countedAt)}</Text></div>
      </div>
      <Button type="text" icon={<RefreshCw size={15} />} onClick={() => void load(true)} loading={loading}>刷新</Button>
    </div>
    {error && <Alert type="error" showIcon message={error} closable onClose={() => setError('')} />}
    <Row gutter={[16, 16]}>
      <Col xs={24} md={12}><Card className="metric-card"><Statistic title="节点数量" value={countValue ?? nodeCount} prefix={<Boxes size={18} />} /></Card></Col>
      <Col xs={24} md={12}><Card className="metric-card"><Statistic title="边数量" value={countValue ?? edgeCount} prefix={<GitBranch size={18} />} /></Card></Col>
    </Row>
    <Card className="storage-card">
      <div className="storage-icon"><Database size={19} /></div>
      <div>
        <Text strong>存储占用</Text>
        <Text type="secondary">集群节点目录占用（含副本和日志），不是单图逻辑数据大小。</Text>
        {storageAt != null && <Text type="secondary">读取时间：{formatCollectedAt(storageAt)}</Text>}
      </div>
      <Tag color="blue">{storageLoading ? '读取中' : storageBytes == null ? '未能读取' : formatBytes(storageBytes)}</Tag>
    </Card>
    {rows.length ? <Table rowKey={(row) => `${row.target}-${row.name}`} loading={loading} columns={[{ title: '对象', dataIndex: 'target', render: (value: string) => <Tag>{value}</Tag> }, { title: '类型', dataIndex: 'name' }, { title: '数量', dataIndex: 'count' }]} dataSource={rows} pagination={false} /> : <Empty description={loading ? '读取计数中…' : countedAt == null ? '尚未读取计数，请重试' : '暂无类型统计'} />}
  </div>;
}

function freshnessText(kind: string, collectedAt: number): string {
  return `${kind}统计于 ${formatCollectedAt(collectedAt)}，30 分钟内不重新扫描。`;
}

function formatCollectedAt(millis: number): string {
  const date = new Date(millis);
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
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

type RunningRow = { queryId: string; user: string; elapsedMs: number; query: string; local?: boolean };

function withInflight(rows: RunningRow[], inflight: string): RunningRow[] {
  const text = inflight.trim();
  if (!text || rows.some((row) => row.query.trim() === text)) return rows;
  return [{ queryId: 'local', user: '', elapsedMs: 0, query: text, local: true }, ...rows];
}

function formatElapsed(ms: number) {
  const safe = Number.isFinite(ms) ? Math.max(0, ms) : 0;
  return `${Math.round(safe)} ms`;
}

function sleep(ms: number) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function RunningQueries({ api, inflight }: { api: CengyanApi; inflight: string }) {
  const { message: messageApi, modal } = AntApp.useApp();
  const [rows, setRows] = useState<RunningRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [stoppingIds, setStoppingIds] = useState<ReadonlySet<string>>(() => new Set());
  const stoppingRef = useRef(new Set<string>());
  const alive = useRef(true);

  const rememberStopping = (queryId: string, active: boolean) => {
    const next = new Set(stoppingRef.current);
    if (active) next.add(queryId);
    else next.delete(queryId);
    stoppingRef.current = next;
    setStoppingIds(next);
  };

  const load = useCallback(async (quiet: boolean): Promise<RunningRow[] | null> => {
    if (!quiet) setLoading(true);
    try {
      const response = await api.queries();
      if (!alive.current) return null;
      const next = rowsAsObjects<RunningRow>(response).map((row) => ({
        queryId: String(row.queryId ?? ''),
        user: String(row.user ?? ''),
        elapsedMs: Number(row.elapsedMs ?? 0),
        query: String(row.query ?? ''),
      }));
      setRows(next);
      setError('');
      return next;
    } catch (reason) {
      if (!alive.current) return null;
      setError(reason instanceof Error ? reason.message : '读取正在执行的查询失败');
      return null;
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

  const beginKill = (queryId: string) => {
    if (stoppingRef.current.has(queryId)) return;
    rememberStopping(queryId, true);
    void (async () => {
      try {
        await api.cancelQuery(queryId);
      } catch (reason) {
        if (!alive.current) return;
        const listed = await load(true);
        if (!alive.current) return;
        rememberStopping(queryId, false);
        if (listed && !listed.some((item) => item.queryId === queryId)) {
          messageApi.success('已终止');
          return;
        }
        messageApi.error(reason instanceof Error ? reason.message : '终止失败');
        return;
      }
      while (alive.current && stoppingRef.current.has(queryId)) {
        await sleep(1000);
        if (!alive.current || !stoppingRef.current.has(queryId)) return;
        const listed = await load(true);
        if (!alive.current || !stoppingRef.current.has(queryId)) return;
        if (listed && !listed.some((item) => item.queryId === queryId)) {
          rememberStopping(queryId, false);
          messageApi.success('已终止');
          return;
        }
      }
    })();
  };

  const kill = (row: RunningRow) => {
    if (stoppingRef.current.has(row.queryId)) return;
    modal.confirm({
      title: '终止这条查询',
      content: `${row.user || '该用户'} 正在执行的语句会被取消。`,
      okText: '终止',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: () => {
        beginKill(row.queryId);
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
          dataSource={withInflight(rows, inflight)}
          pagination={false}
          locale={{ emptyText: '当前没有正在执行的查询' }}
          columns={[
            { title: '提交用户', dataIndex: 'user', width: 160 },
            { title: '执行时间', dataIndex: 'elapsedMs', width: 140, render: (ms: number, row: RunningRow) => row.local ? '本页提交' : formatElapsed(ms) },
            { title: 'GQL', dataIndex: 'query', render: (query: string) => <pre className="running-gql">{query}</pre> },
            {
              title: '操作',
              width: 168,
              render: (_value: unknown, row: RunningRow) => {
                const stopping = stoppingIds.has(row.queryId);
                return (
                  <Button
                    danger
                    size="small"
                    className={stopping ? 'kill-stopping' : undefined}
                    disabled={stopping || row.local}
                    aria-busy={stopping}
                    icon={stopping ? <RefreshCw size={13} className="kill-spin" /> : undefined}
                    onClick={() => kill(row)}
                  >
                    {row.local ? '执行中' : stopping ? '执行终止中' : '终止'}
                  </Button>
                );
              },
            },
          ]}
        />
      </Card>
    </div>
  );
}

function bytesText(value?: unknown) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let index = 0;
  let amount = bytes;
  while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${amount.toFixed(index ? 1 : 0)} ${units[index]}`;
}

function percentText(value?: unknown) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? `${(number * 100).toFixed(1)}%` : '—';
}

function rateText(value?: unknown) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? `${bytesText(number)}/s` : '—';
}

const computeResourceColumns: TableProps<Record<string, unknown>>['columns'] = [
  { title: '节点', dataIndex: 'nodeId', width: 112, ellipsis: true },
  { title: '角色', dataIndex: 'role', width: 88 },
  { title: 'CPU', width: 72, className: 'ops-num-col', render: (_, row) => percentText(row.systemCpuLoad) },
  { title: '堆内存', width: 168, className: 'ops-num-col', render: (_, row) => `${bytesText(row.heapUsedBytes)} / ${bytesText(row.heapMaxBytes)}` },
  { title: '执行中', width: 72, className: 'ops-num-col', render: (_, row) => String((row.slots as Record<string, unknown> | undefined)?.active ?? '—') },
  { title: '排队', width: 64, className: 'ops-num-col', dataIndex: 'queuePending' },
  { title: '网络流入', width: 128, className: 'ops-num-col ops-fixed-col', render: (_, row) => rateText(row.netInBytesPerSec) },
  { title: '网络流出', width: 128, className: 'ops-num-col ops-fixed-col', render: (_, row) => rateText(row.netOutBytesPerSec) },
];

const storageResourceColumns: TableProps<Record<string, unknown>>['columns'] = [
  { title: '节点', dataIndex: 'nodeId', width: 88, ellipsis: true },
  { title: '角色', dataIndex: 'role', width: 88 },
  { title: 'CPU', width: 56, className: 'ops-num-col', render: (_, row) => percentText(row.systemCpuLoad) },
  { title: '堆内存', width: 148, className: 'ops-num-col', render: (_, row) => `${bytesText(row.heapUsedBytes)} / ${bytesText(row.heapMaxBytes)}` },
  { title: '磁盘可用', width: 84, className: 'ops-num-col', render: (_, row) => bytesText(row.diskUsableBytes) },
  { title: '磁盘繁忙', width: 104, className: 'ops-num-col ops-fixed-col', render: (_, row) => percentText(row.diskBusy) },
  { title: '读取速度', width: 128, className: 'ops-num-col ops-fixed-col', render: (_, row) => rateText(row.diskReadBytesPerSec) },
  { title: '写入速度', width: 128, className: 'ops-num-col ops-fixed-col', render: (_, row) => rateText(row.diskWriteBytesPerSec) },
  { title: 'Metadata Leader', dataIndex: 'metadataLeaderId', width: 140, ellipsis: true },
];

function numberValue(value: unknown): number | undefined {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : undefined;
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' ? value as Record<string, unknown> : undefined;
}

function OpsDashboard({ api, bindRefresh }: { api: CengyanApi; bindRefresh: (refresh: () => void, refreshing: boolean) => void }) {
  const [data, setData] = useState<OpsOverview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await api.opsOverview()); setError(''); }
    catch (reason) { setError(reason instanceof Error ? reason.message : '集群运维数据读取失败'); }
    finally { setLoading(false); }
  }, [api]);
  useEffect(() => { void load(); const timer = window.setInterval(() => void load(), 5000); return () => window.clearInterval(timer); }, [load]);
  useEffect(() => { bindRefresh(() => { void load(); }, loading); }, [bindRefresh, load, loading]);
  const storageNodes = data?.storageNodes || [];
  const computeNodes = data?.computeNodes || [];
  const shards = data?.shards || [];
  const computeSummary = useMemo(() => {
    const nodes = computeNodes.length > 0
      ? computeNodes
      : data ? [data as unknown as Record<string, unknown>] : [];
    const cpuValues = nodes.map((node) => numberValue(node.systemCpuLoad)).filter((value): value is number => value !== undefined);
    let heapUsed = 0;
    let heapMax = 0;
    let diskUsable = 0;
    let diskTotal = 0;
    let slotsActive = 0;
    let slotsCapacity = 0;
    let queuePending = 0;
    let threadCount = 0;
    let hasHeapUsed = false;
    let hasHeapMax = false;
    let hasDiskUsable = false;
    let hasDiskTotal = false;
    let hasThreadCount = false;

    nodes.forEach((node) => {
      const slots = recordValue(node.slots);
      const values = [
        ['heapUsedBytes', (value: number) => { heapUsed += value; hasHeapUsed = true; }],
        ['heapMaxBytes', (value: number) => { heapMax += value; hasHeapMax = true; }],
        ['diskUsableBytes', (value: number) => { diskUsable += value; hasDiskUsable = true; }],
        ['diskTotalBytes', (value: number) => { diskTotal += value; hasDiskTotal = true; }],
        ['threadCount', (value: number) => { threadCount += value; hasThreadCount = true; }],
      ] as const;
      values.forEach(([key, add]) => {
        const value = numberValue(node[key]);
        if (value !== undefined) add(value);
      });
      const active = numberValue(slots?.active);
      const capacity = numberValue(slots?.capacity);
      const queued = numberValue(node.queuePending);
      if (active !== undefined) slotsActive += active;
      if (capacity !== undefined) slotsCapacity += capacity;
      if (queued !== undefined) queuePending += queued;
    });

    return {
      averageCpu: cpuValues.length > 0 ? cpuValues.reduce((sum, value) => sum + value, 0) / cpuValues.length : undefined,
      heapUsed: hasHeapUsed ? heapUsed : undefined,
      heapMax: hasHeapMax ? heapMax : undefined,
      diskUsable: hasDiskUsable ? diskUsable : undefined,
      diskTotal: hasDiskTotal ? diskTotal : undefined,
      slotsActive,
      slotsCapacity,
      queuePending,
      threadCount: hasThreadCount ? threadCount : undefined,
    };
  }, [computeNodes, data]);
  return <div className="page-stack workspace-page ops-dashboard">
    {error && <Alert type="warning" showIcon message="集群运维数据暂不可用" description={error} closable onClose={() => setError('')} />}
    <Card className="workspace-card ops-summary-card" bordered={false} title="计算资源汇总情况">
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}><Card className="metric-card"><Statistic title="平均 CPU 值" value={percentText(computeSummary.averageCpu)} prefix={<Activity size={18} />} /></Card></Col>
        <Col xs={24} md={8}><Card className="metric-card"><Statistic title="堆内存汇总情况" value={`${bytesText(computeSummary.heapUsed)} / ${bytesText(computeSummary.heapMax)}`} prefix={<Database size={18} />} /></Card></Col>
        <Col xs={24} md={8}><Card className="metric-card"><Statistic title="磁盘汇总情况" value={`${bytesText(computeSummary.diskUsable)} / ${bytesText(computeSummary.diskTotal)}`} prefix={<Server size={18} />} /></Card></Col>
        <Col xs={24} md={8}><Card className="metric-card"><Statistic title="执行槽位情况" value={`${computeSummary.slotsActive} / ${computeSummary.slotsCapacity}`} prefix={<Gauge size={18} />} /></Card></Col>
        <Col xs={24} md={8}><Card className="metric-card"><Statistic title="排队情况" value={computeSummary.queuePending} prefix={<Activity size={18} />} /></Card></Col>
        <Col xs={24} md={8}><Card className="metric-card"><Statistic title="线程数情况" value={computeSummary.threadCount ?? '—'} prefix={<Workflow size={18} />} /></Card></Col>
      </Row>
    </Card>
    <Card className="workspace-card" bordered={false} title="Compute 节点资源"><Table className="ops-resource-table ops-compute-table" tableLayout="fixed" size="small" rowKey={(row) => String(row.nodeId)} pagination={false} dataSource={computeNodes} columns={computeResourceColumns} scroll={{ x: 832 }} locale={{ emptyText: '没有 Compute 运维数据' }} /></Card>
    <Card className="workspace-card" bordered={false} title="Storage 节点资源"><Table className="ops-resource-table ops-storage-table" tableLayout="fixed" size="small" rowKey={(row) => String(row.nodeId)} pagination={false} dataSource={storageNodes} columns={storageResourceColumns} scroll={{ x: 964 }} locale={{ emptyText: '没有 Storage 运维数据' }} /></Card>
    <Card className="workspace-card" bordered={false} title="分片拓扑"><Table size="small" rowKey={(row) => `${row.graphId}-${row.partId}`} pagination={{ pageSize: 10, hideOnSinglePage: true }} dataSource={shards} columns={[{ title: '图', dataIndex: 'graph' }, { title: '分片', dataIndex: 'partId' }, { title: 'Leader', dataIndex: 'leaderId' }, { title: 'Term', dataIndex: 'term' }, { title: 'Applied Index', dataIndex: 'appliedIndex' }, { title: '副本', render: (_, row) => Array.isArray(row.members) ? (row.members as unknown[]).join(', ') : '—' }, { title: '状态', render: (_, row) => <Tag color={row.state === 'UNAVAILABLE' ? 'red' : 'green'}>{String(row.state || 'HEALTHY')}</Tag> }]} locale={{ emptyText: '没有分片拓扑数据' }} /></Card>
  </div>;
}

type ScriptResult = { statement: string; response: QueryResponse; elapsedMs: number };

function GqlPage({ api, graph, canWrite, active, graphMaxNodes, tableMaxNodes, onGraphChanged, onInflight }: { api: CengyanApi; graph: string; canWrite: boolean; active: boolean; graphMaxNodes: number; tableMaxNodes: number; onGraphChanged: (name: string) => void; onInflight: (query: string) => void }) {
  const { notification: notificationApi } = AntApp.useApp();
  const [query, setQuery] = useState(() => {
    const saved = localStorage.getItem('cengyan.studio.last-query') || DEFAULT_QUERY;
    return saved === LEGACY_SAMPLE_QUERY ? DEFAULT_QUERY : saved;
  });
  const [result, setResult] = useState<ScriptResult[] | null>(null);
  const [queryError, setQueryError] = useState('');
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState('');
  const [editorReady, setEditorReady] = useState(false);
  const [exporting, setExporting] = useState(false);
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);
  const runQueryRef = useRef<() => void>(() => {});
  const exportingRef = useRef(false);
  const reportQueryError = (text: string) => {
    notificationApi.error({
      title: '查询失败',
      description: text,
      placement: 'topRight',
      duration: false,
      closable: true,
    });
  };
  const runQuery = async () => {
    if (exportingRef.current || exporting) return;
    const text = editorRef.current?.getValue() ?? query;
    if (!graph) { reportQueryError('请先在右上角选择图空间'); return; }
    const statements = [text.trim()];
    if (!statements[0]) { reportQueryError('GQL 语句不能为空'); return; }
    setQueryError('');
    setRunning(true);
    onInflight(text.trim());
    const done: ScriptResult[] = [];
    try {
      setProgress(text.includes(';') ? '正在执行脚本，返回最后一条语句的结果' : '');
       const response = await api.query(text, 30000);
       done.push({ statement: text.trim(), response, elapsedMs: Number(response.elapsedMs ?? 0) });
       setResult([...done]);
       setQueryError('');
      const uses = [...text.matchAll(/\bUSE\s+([A-Za-z][A-Za-z0-9_]*)/gi)];
      if (uses.length) onGraphChanged(uses[uses.length - 1][1]);
      localStorage.setItem('cengyan.studio.last-query', text);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : '查询失败';
      setResult(null);
      setQueryError(message);
      reportQueryError(message);
    } finally {
      setRunning(false);
      setProgress('');
      onInflight('');
    }
  };
  const exportCsv = async () => {
    if (exportingRef.current || running) return;
    const text = editorRef.current?.getValue() ?? query;
    if (!graph) { reportQueryError('请先在右上角选择图空间'); return; }
    if (!text.trim()) { reportQueryError('GQL 语句不能为空'); return; }
    exportingRef.current = true;
    setExporting(true);
    setProgress('正在重新执行查询并生成 CSV…');
    try {
      const response = await api.query(text, 30000);
      const columns = response.columns || [];
      if (!columns.length) {
        throw new Error('当前查询没有可导出的表格结果');
      }
      const csv = [columns, ...(response.rows || [])]
        .map((row) => row.map(csvCell).join(','))
        .join('\r\n');
      const blob = new Blob([`\uFEFF${csv}`], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `cengyan-result-${new Date().toISOString().replace(/[:.]/g, '-')}.csv`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      // 给浏览器一点时间启动下载，再释放 Blob URL，避免下载被过早中断。
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (reason) {
      reportQueryError(reason instanceof Error ? reason.message : '导出 CSV 失败');
    } finally {
      exportingRef.current = false;
      setExporting(false);
      setProgress('');
    }
  };
  runQueryRef.current = () => { void runQuery(); };
  const editorMount: OnMount = (editor) => { editorRef.current = editor; editor.addCommand(2048 | 3, () => { runQueryRef.current(); }); };
  useEffect(() => { let live = true; void import('./monaco').then(() => { if (live) setEditorReady(true); }); return () => { live = false; }; }, []);
  useEffect(() => { if (active) editorRef.current?.layout(); }, [active]);
  return <div className="query-page"><Row gutter={[18, 18]}><Col span={24}><Card className="editor-card" bordered={false}><div className="editor-toolbar"><div className="editor-file"><Tag color="geekblue">GQL</Tag></div><Button type="text" size="small" icon={<Trash2 size={14} />} onClick={() => { setQuery(''); editorRef.current?.setValue(''); }}>清空</Button></div>{editorReady ? <Editor height="350px" language="gql" theme="vs" value={query} onChange={(value) => setQuery(value || '')} onMount={editorMount} options={{ minimap: { enabled: false }, fontSize: 13, lineNumbers: 'on', lineNumbersMinChars: 3, lineDecorationsWidth: 8, padding: { top: 14, bottom: 14 }, scrollBeyondLastLine: false, wordWrap: 'on', roundedSelection: false, automaticLayout: true, tabSize: 2 }} /> : <div className="editor-pending">正在打开编辑器…</div>}<div className="editor-footer"><Text type="secondary">{progress || '多条语句用分号分隔，服务端顺序执行并返回最后一条 · Ctrl + Enter'}</Text><Space size={8}>{canWrite && <Tag color="green">读写权限</Tag>}<Button type="primary" icon={<Code2 size={15} />} onClick={() => void runQuery()} loading={running} disabled={!graph || exporting}>执行查询 <Text className="shortcut">Ctrl ↵</Text></Button></Space></div></Card>{(result || running || queryError) && <ScriptResults api={api} results={result || []} graphMaxNodes={graphMaxNodes} tableMaxNodes={tableMaxNodes} loading={running} error={queryError} exporting={exporting} onExport={() => void exportCsv()} />}</Col></Row></div>;
}

function ScriptResults({ api, results, loading, error, graphMaxNodes, tableMaxNodes, exporting, onExport }: { api: CengyanApi; results: ScriptResult[]; loading: boolean; error?: string; graphMaxNodes: number; tableMaxNodes: number; exporting: boolean; onExport: () => void }) {
  if (error) return <Card className="result-card result-error-card" bordered={false}><Alert type="error" showIcon message="查询失败" description={error} /></Card>;
  if (!results.length) return <Card className="result-card" bordered={false}><div className="result-card-loading-placeholder" /><div className="result-card-loading-overlay"><Spin description="查询中..." size="small"><div className="result-card-loading-content" /></Spin></div></Card>;
  if (results.length === 1) return <QueryResult api={api} response={results[0].response} elapsedMs={results[0].elapsedMs} graphMaxNodes={graphMaxNodes} tableMaxNodes={tableMaxNodes} loading={loading} exporting={exporting} onExport={onExport} />;
  return <>{results.map((item, index) => <div className="script-result" key={`${index}-${item.statement}`}><div className="script-result-label"><Text type="secondary">第 {index + 1} 条 · {item.elapsedMs} ms</Text><pre>{item.statement}</pre></div><QueryResult api={api} response={item.response} elapsedMs={item.elapsedMs} graphMaxNodes={graphMaxNodes} tableMaxNodes={tableMaxNodes} loading={loading && index === results.length - 1} exporting={exporting} onExport={onExport} /></div>)}</>;
}

function QueryResult({ api, response, elapsedMs, graphMaxNodes, tableMaxNodes, loading, exporting, onExport }: { api: CengyanApi; response: QueryResponse; elapsedMs: number; graphMaxNodes: number; tableMaxNodes: number; loading?: boolean; exporting: boolean; onExport: () => void }) {
  const columns = response.columns || [];
  const rows = response.rows || [];
  const [view, setView] = useState('table');
  useEffect(() => { setView('table'); }, [response]);
   const tableColumns: TableProps<Record<string, string>>['columns'] = columns.map((column) => ({ title: column, dataIndex: column, key: column }));
   const displayLimit = Math.max(1, tableMaxNodes || MAX_TABLE_ROWS);
   const displayRows = rows.slice(0, displayLimit);
   const data = displayRows.map((row, rowIndex) => Object.fromEntries(columns.map((column, index) => [column, tableCellText(row[index])]).concat([['key', String(rowIndex)]])) as Record<string, string>);
   const tabs: TabsProps['items'] = [
       { key: 'table', label: '表格', children: <Table className="result-data-table" size="small" rowKey="key" showHeader tableLayout="auto" scroll={{ x: 'max-content', y: 330 }} columns={tableColumns} dataSource={data} pagination={{ pageSize: 100, showSizeChanger: true, pageSizeOptions: ['50', '100', '200'], showTotal: (total, range) => `${range[0]}-${range[1]} / ${total} 条` }} /> },
      { key: 'graph', label: '关系图', children: view === 'graph' ? <QueryGraph api={api} rows={rows} maxNodes={graphMaxNodes} /> : null },
   ];
   return <Card className="result-card" bordered={false}><div className="result-heading"><div><Title level={4}>查询结果</Title><Text type="secondary">{columns.length ? `${rows.length} 行 · ${elapsedMs} ms${rows.length > displayLimit ? ` · 前 ${displayLimit} 行可分页查看` : ''}` : `完成，影响 ${response.rowsAffected || 0} 行 · ${elapsedMs} ms`}</Text></div>{columns.length && <Button icon={<Download size={15} />} loading={exporting} disabled={exporting || loading} onClick={onExport}>导出 CSV</Button>}</div>{columns.length && rows.length > displayLimit && <Alert className="result-limit-alert" type="info" showIcon message={`当前查询共 ${rows.length} 行，表格最多展示前 ${displayLimit} 行；导出 CSV 会重新执行查询并导出全部结果。`} />}{columns.length ? <Tabs className="result-tabs" activeKey={view} onChange={setView} destroyOnHidden items={tabs} /> : <div className="success-result"><Check size={20} /><span>语句执行成功</span></div>}{loading && <div className="result-card-loading-overlay"><Spin description="查询中..." size="small"><div className="result-card-loading-content" /></Spin></div>}</Card>;
}

function csvCell(value: unknown) {
  if (value == null) return '';
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function tableCellText(value: unknown) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}
