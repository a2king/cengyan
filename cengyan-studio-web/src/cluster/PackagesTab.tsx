import { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Modal, Radio, Select, Space, Table, Tag, Typography } from 'antd';
import type { TableProps } from 'antd';
import { ArrowUpCircle, FolderInput, Trash2 } from 'lucide-react';
import { errorText, timeText } from '../clusterApi';
import type { PackageRow, PackagesView, Role, UpgradePreview } from '../clusterApi';
import type { ConsoleContext } from './ClusterConsole';

const { Text, Paragraph } = Typography;
const short = (sha?: string) => (sha ? sha.slice(0, 8) : '—');

/** Studio 保存的节点程序包。新程序放进导入目录后扫描即可，不需要重启 Studio。 */
export function PackagesTab({ ctx }: { ctx: ConsoleContext }) {
  const [view, setView] = useState<PackagesView | null>(null);
  const [error, setError] = useState('');
  const [scanning, setScanning] = useState(false);
  const load = useCallback(async () => {
    try { setView(await ctx.api.packages()); setError(''); } catch (reason) { setError(errorText(reason)); }
  }, [ctx.api]);
  useEffect(() => { void load(); }, [load, ctx.overview.release]);
  const scan = async () => {
    setScanning(true);
    try { setView(await ctx.api.scanPackages()); setError(''); await ctx.refresh(); }
    catch (reason) { setError(errorText(reason)); }
    finally { setScanning(false); }
  };
  const remove = (row: PackageRow) => Modal.confirm({
    title: `删除程序包 ${row.id}？`,
    content: '只删除 Studio 保存的文件。服务器上的同名目录在下次升级的清理步骤中删除。',
    okText: '删除', okButtonProps: { danger: true },
    onOk: async () => {
      try { await ctx.api.removePackage(row.id); await load(); await ctx.refresh(); }
      catch (reason) { Modal.error({ title: '无法删除', content: errorText(reason) }); }
    },
  });
  const busy = Boolean(ctx.overview.activeTask);
  const columns: TableProps<PackageRow>['columns'] = [
    { title: '程序包', dataIndex: 'id', render: (id: string, row) => <Space size={4}><span className="code-pill">{id}</span>{row.latest && <Tag color="green">最新</Tag>}</Space> },
    { title: '来源', dataIndex: 'source', ellipsis: true },
    { title: '导入时间', dataIndex: 'importedAt', width: 170, render: (time: number) => timeText(time) },
    { title: 'storage / compute 摘要', key: 'jars', width: 190, render: (_, row) =>
      <Text type="secondary">{short(row.files['cengyan-storage.jar'])} / {short(row.files['cengyan-compute.jar'])}</Text> },
    { title: '在用节点', dataIndex: 'usedBy', render: (used: string[]) => used.length
      ? <Space size={[4, 4]} wrap>{used.map((id) => <Tag key={id}>{id}</Tag>)}</Space> : <Text type="secondary">—</Text> },
    { title: '操作', key: 'actions', fixed: 'right', width: 200, render: (_, row) => <span className="table-actions">
      <Button type="link" size="small" icon={<ArrowUpCircle size={14} />} disabled={busy} onClick={() => ctx.upgrade(row.id)}>升级到此版本</Button>
      <Button type="link" danger size="small" icon={<Trash2 size={14} />} disabled={busy || row.usedBy.length > 0} onClick={() => remove(row)}>删除</Button>
    </span> },
  ];
  return <div className="cluster-tab">
    <div className="cluster-tab-head">
      <Text type="secondary">把 cengyan-storage.jar、cengyan-compute.jar 或完整发布包 tar.gz 放进导入目录后扫描，不需要重启 Studio。</Text>
      <Button type="primary" icon={<FolderInput size={15} />} loading={scanning} onClick={() => void scan()}>扫描导入目录</Button>
    </div>
    {view && <Paragraph type="secondary">
      导入目录：<Text copyable code>{view.inbox}</Text>。单个 jar 会和最新程序包里的其余文件组成新程序包；
      同目录放 <Text code>文件名.sha256</Text> 会先核对摘要；上传时先用 <Text code>.part</Text> 后缀，传完再改名。
    </Paragraph>}
    {error && <Alert type="error" showIcon message={error} style={{ marginBottom: 12 }} />}
    {view && view.scannedAt > 0 && view.scan.length === 0 && <Alert type="info" showIcon style={{ marginBottom: 12 }}
      message={`${timeText(view.scannedAt)} 扫描：导入目录里没有新文件`} />}
    {view && view.scan.length > 0 && <Alert type={view.scan.every((r) => r.ok) ? 'success' : 'warning'} showIcon style={{ marginBottom: 12 }}
      message={`${timeText(view.scannedAt)} 扫描结果`}
      description={<ul style={{ margin: 0, paddingLeft: 18 }}>{view.scan.map((r) => <li key={r.file}>
        <Text strong>{r.file}</Text>：{r.ok ? `${r.message} → ${r.release}` : <Text type="danger">{r.message}</Text>}
      </li>)}</ul>} />}
    <Table rowKey="id" size="middle" loading={!view && !error} columns={columns} dataSource={view?.packages || []} pagination={false} scroll={{ x: 960 }} />
  </div>;
}

type Scope = 'ALL' | Role;

/** 选择目标程序包和范围，先预览会重启哪些节点、是否暂停写入，再提交。 */
export function UpgradeModal({ ctx, open, release, onClose }: { ctx: ConsoleContext; open: boolean; release: string; onClose: () => void }) {
  const [packages, setPackages] = useState<PackageRow[]>([]);
  const [target, setTarget] = useState(release);
  const [scope, setScope] = useState<Scope>('ALL');
  const [preview, setPreview] = useState<UpgradePreview | null>(null);
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const api = ctx.api;
  useEffect(() => {
    if (!open) return;
    setTarget(release);
    setScope('ALL');
    void api.packages().then((view) => setPackages(view.packages)).catch((reason) => setError(errorText(reason)));
  }, [open, release, api]);
  useEffect(() => {
    if (!open) return undefined;
    let live = true;
    setPreview(null);
    setError('');
    api.previewUpgrade({ release: target, roles: scope === 'ALL' ? [] : [scope] })
      .then((result) => { if (live) setPreview(result); })
      .catch((reason) => { if (live) setError(errorText(reason)); });
    return () => { live = false; };
  }, [open, target, scope, api]);
  const submit = async () => {
    if (!preview) return;
    setSubmitting(true);
    try {
      const task = await api.upgrade({ release: preview.release, roles: scope === 'ALL' ? [] : [scope] });
      onClose();
      ctx.started(task);
    } catch (reason) { setError(errorText(reason)); }
    finally { setSubmitting(false); }
  };
  const nothing = Boolean(preview && preview.targets.length === 0);
  return <Modal title="升级节点程序" open={open} onCancel={onClose} okText="开始升级" width={640} destroyOnHidden
    okButtonProps={{ disabled: !preview || nothing, danger: Boolean(preview?.storage) }} confirmLoading={submitting} onOk={() => void submit()}>
    <Space direction="vertical" size={14} style={{ width: '100%' }}>
      <div>
        <Text type="secondary">目标程序包</Text>
        <Select style={{ width: '100%', marginTop: 4 }} value={target || undefined} placeholder="最新导入的程序包" onChange={setTarget}
          options={packages.map((p) => ({ value: p.id, label: `${p.id}${p.latest ? '（最新）' : ''} · ${p.source}` }))} />
      </div>
      <div>
        <Text type="secondary">范围</Text>
        <div style={{ marginTop: 4 }}>
          <Radio.Group value={scope} optionType="button" onChange={(event) => setScope(event.target.value as Scope)} options={[
            { value: 'ALL', label: '所有需要升级的节点' }, { value: 'COMPUTE', label: '只升级计算节点' }, { value: 'STORAGE', label: '只升级存储节点' },
          ]} />
        </div>
      </div>
      {error && <Alert type="error" showIcon message={error} />}
      {preview && nothing && <Alert type="success" showIcon message="所选范围内节点的程序和目标程序包相同，不需要升级" />}
      {preview && !nothing && <Alert type={preview.storage ? 'warning' : 'info'} showIcon
        message={`将逐台重启 ${preview.targets.length} 个节点：${preview.targets.map((n) => n.id).join('、')}`}
        description={preview.storage
          ? '包含存储节点：存储节点重启期间集群进入维护模式，写入暂停，全部完成后自动恢复。'
          : '只涉及计算节点：逐台排空查询后重启，不暂停写入；有两个以上计算节点时查询不中断。'} />}
      {preview && preview.unchanged.length > 0 && <Text type="secondary">程序没有变化、不会重启：{preview.unchanged.map((n) => n.id).join('、')}</Text>}
    </Space>
  </Modal>;
}
