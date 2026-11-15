import { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Card, Checkbox, Collapse, Empty, Input, List, Modal, Space, Steps, Tag, Typography } from 'antd';
import { STATUS_TEXT, errorText, timeText } from '../clusterApi';
import type { TaskView } from '../clusterApi';
import type { ConsoleContext } from './ClusterConsole';

const { Text } = Typography;
const STEP_STATUS = { PENDING: 'wait', RUNNING: 'process', DONE: 'finish', FAILED: 'error', SKIPPED: 'finish' } as const;

export function TasksTab({ ctx, focus, onFocus }: { ctx: ConsoleContext; focus: string; onFocus: (id: string) => void }) {
  const [tasks, setTasks] = useState<TaskView[]>([]);
  const [detail, setDetail] = useState<TaskView | null>(null);
  const [error, setError] = useState('');
  const load = useCallback(async () => {
    try {
      const list = await ctx.api.tasks(30);
      setTasks(list);
      const id = focus || list[0]?.id;
      if (id) setDetail(await ctx.api.task(id));
      setError('');
    } catch (reason) { setError(errorText(reason)); }
  }, [ctx.api, focus]);
  useEffect(() => { void load(); }, [load, ctx.overview]);
  return <div className="cluster-tab task-layout">
    <Card size="small" className="task-list" title="最近任务">
      <List dataSource={tasks} locale={{ emptyText: '还没有任务' }} renderItem={(t) => <List.Item className={detail?.id === t.id ? 'history-selected' : ''} onClick={() => onFocus(t.id)}>
        <List.Item.Meta title={<Space size={6}><Text strong>{t.title}</Text><Tag color={STATUS_TEXT[t.state]?.color}>{STATUS_TEXT[t.state]?.text}</Tag></Space>}
          description={timeText(t.createdAt)} />
      </List.Item>} />
    </Card>
    <div className="task-detail">
      {error && <Alert type="error" showIcon message={error} />}
      {detail ? <TaskDetail ctx={ctx} task={detail} onChanged={() => void load()} /> : <Empty description="选择左侧任务查看进度" />}
    </div>
  </div>;
}

function TaskDetail({ ctx, task, onChanged }: { ctx: ConsoleContext; task: TaskView; onChanged: () => void }) {
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState('');
  const paused = task.state === 'PAUSED' || task.state === 'INTERRUPTED';
  const needsPassword = paused && task.type === 'server.add' && /root 密码/.test(task.error);
  const touchesStorage = task.type === 'restart' || task.type === 'upgrade';
  const act = async (action: 'retry' | 'skip' | 'abort', options: { password?: string; exitMaintenance?: boolean } = {}) => {
    setBusy(action);
    try { await ctx.api.act(task.id, action, options); setPassword(''); await ctx.refresh(); onChanged(); }
    catch (reason) { Modal.error({ title: '操作失败', content: errorText(reason) }); }
    finally { setBusy(''); }
  };
  const abort = () => {
    let exit = false;
    Modal.confirm({
      title: '终止任务？',
      content: <Space direction="vertical">
        <Text>已完成的步骤不会回退。</Text>
        {touchesStorage && <Checkbox onChange={(e) => { exit = e.target.checked; }}>同时退出维护模式（恢复写入）。只有确认所有存储节点正常时才勾选</Checkbox>}
      </Space>,
      okText: '终止', okButtonProps: { danger: true },
      onOk: () => act('abort', { exitMaintenance: exit }),
    });
  };
  return <Card size="small" title={<Space>{task.title}<Tag color={STATUS_TEXT[task.state]?.color}>{STATUS_TEXT[task.state]?.text}</Tag></Space>}
    extra={<Text type="secondary">开始于 {timeText(task.createdAt)}</Text>}>
    {paused && <Alert type="warning" showIcon className="task-alert"
      message={task.state === 'INTERRUPTED' ? 'Studio 重启导致任务中断' : `第 ${task.current + 1} 步失败，任务已暂停`}
      description={<Space direction="vertical" style={{ width: '100%' }}>
        <Text>{task.error}</Text>
        {needsPassword && <Input.Password placeholder="重新输入 root 密码" value={password} onChange={(e) => setPassword(e.target.value)} style={{ maxWidth: 320 }} autoComplete="off" />}
        <Space wrap>
          <Button type="primary" loading={busy === 'retry'} disabled={needsPassword && !password} onClick={() => void act('retry', needsPassword ? { password } : {})}>
            {task.state === 'INTERRUPTED' ? '继续执行' : '重试本步'}</Button>
          <Button loading={busy === 'skip'} onClick={() => Modal.confirm({ title: '跳过这一步？', content: '跳过后从下一步继续。只在确认这一步已经手工完成或可以忽略时使用。', okText: '跳过', onOk: () => act('skip') })}>跳过本步</Button>
          <Button danger loading={busy === 'abort'} onClick={abort}>终止任务</Button>
        </Space>
      </Space>} />}
    {task.state === 'ABORTED' && task.error && <Alert type="info" showIcon className="task-alert" message={task.error} />}
    <Steps direction="vertical" size="small" current={task.current} items={task.steps.map((step) => ({
      title: <Space size={6}>{step.title}{step.state === 'SKIPPED' && <Tag>已跳过</Tag>}</Space>,
      status: STEP_STATUS[step.state],
      description: step.log.length > 0 && <Collapse ghost size="small" defaultActiveKey={step.state === 'FAILED' || step.state === 'RUNNING' ? ['log'] : []}
        items={[{ key: 'log', label: `日志（${step.log.length} 行）`, children: <pre className="step-log">{step.log.join('\n')}</pre> }]} />,
    }))} />
  </Card>;
}
