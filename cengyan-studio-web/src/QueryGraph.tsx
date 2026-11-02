import { useEffect, useMemo, useRef, useState } from 'react';
import { Empty, Modal, Tag, Typography } from 'antd';
import { Maximize2, Minimize2, Workflow } from 'lucide-react';
import type { CengyanApi } from './api';

const { Text } = Typography;

const NODE_R = 18;
const SEPARATION = 108;
const COLORS = ['#69C0FF', '#95DE64', '#FF9C6E', '#B37FEB', '#5CDBD3', '#FF7875', '#FFC069', '#597EF7', '#FF85C0', '#36CFC9'];

type Props = Record<string, unknown>;
type GraphNode = { id: string; typeName: string; color: string; x: number; y: number; properties: Props };
type GraphLink = { id: string; source: GraphNode; target: GraphNode; label: string; rank: string; properties: Props };
type View = { x: number; y: number; k: number };
type Curve = { sx: number; sy: number; cx: number; cy: number; tx: number; ty: number };
type Detail =
  | { kind: 'node'; id: string; typeName: string; properties: Props }
  | { kind: 'edge'; id: string; label: string; source: string; target: string; rank: string; properties: Props };

function typeColor(typeName: string) {
  let hash = 0;
  for (let index = 0; index < typeName.length; index += 1) hash = (hash * 33 + typeName.charCodeAt(index)) >>> 0;
  return COLORS[hash % COLORS.length];
}

function shortId(id: string) {
  return id.length <= 12 ? id : `${id.slice(0, 4)}…${id.slice(-4)}`;
}

function asProps(value: unknown): Props {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value as Props;
}

function formatValue(value: unknown) {
  if (value == null) return 'null';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function gqlString(value: string) {
  return `'${value.replace(/\\/g, '\\\\').replace(/'/g, "''")}'`;
}

function lookupQuery(ids: string[]) {
  const patterns = ids.map((id, index) => `(n${index} {id:${gqlString(id)}})`).join(', ');
  const names = ids.map((_, index) => `n${index}`).join(', ');
  return `MATCH ${patterns} RETURN ${names}`;
}

function nodesFromResponse(rows: unknown[][]) {
  const found = new Map<string, { typeName: string; properties: Props }>();
  rows.forEach((row) => row.forEach((cell) => {
    if (!cell || typeof cell !== 'object' || Array.isArray(cell)) return;
    const item = cell as Record<string, unknown>;
    if (item.id == null || item.type == null || String(item.type).trim() === '') return;
    found.set(String(item.id), { typeName: String(item.type).trim(), properties: asProps(item.properties) });
  }));
  return found;
}

function parseGraph(rows: unknown[][]) {
  const nodes = new Map<string, GraphNode>();
  const links: GraphLink[] = [];
  const seen = new Set<string>();
  const ensure = (id: string, typeName: string, properties: Props) => {
    const name = typeName.trim();
    const existing = nodes.get(id);
    if (existing) {
      if (!existing.typeName && name) {
        existing.typeName = name;
        existing.color = typeColor(name);
      }
      if (!Object.keys(existing.properties).length && Object.keys(properties).length) existing.properties = properties;
      return existing;
    }
    const node = { id, typeName: name, color: name ? typeColor(name) : '#C5C8D4', x: 0, y: 0, properties };
    nodes.set(id, node);
    return node;
  };
  const absorb = (cell: unknown) => {
    if (!cell || typeof cell !== 'object') return;
    if (Array.isArray(cell)) {
      cell.forEach(absorb);
      return;
    }
    const item = cell as Record<string, unknown>;
    if (Array.isArray(item.nodes) || Array.isArray(item.edges)) {
      if (Array.isArray(item.nodes)) item.nodes.forEach(absorb);
      if (Array.isArray(item.edges)) item.edges.forEach(absorb);
    }
    if (item.src !== undefined && item.dst !== undefined && item.type) {
      const source = ensure(String(item.src), String(item.srcType ?? ''), {});
      const target = ensure(String(item.dst), String(item.dstType ?? ''), {});
      const id = `${item.type}-${item.rank ?? 0}-${source.id}-${target.id}`;
      if (!seen.has(id)) {
        seen.add(id);
        links.push({ id, source, target, label: String(item.type), rank: item.rank == null ? '0' : String(item.rank), properties: asProps(item.properties) });
      }
      return;
    }
    if (item.id !== undefined) ensure(String(item.id), item.type !== undefined ? String(item.type) : '', asProps(item.properties));
  };
  rows.forEach((row) => row.forEach(absorb));
  return { nodes: [...nodes.values()], links };
}

function hexSpiral(count: number): Array<[number, number]> {
  const coords: Array<[number, number]> = [[0, 0]];
  if (count <= 1) return coords.slice(0, count);
  let q = 0;
  let r = 0;
  const dirs: Array<[number, number]> = [[1, 0], [1, -1], [0, -1], [-1, 0], [-1, 1], [0, 1]];
  let ring = 1;
  while (coords.length < count) {
    q += dirs[4][0];
    r += dirs[4][1];
    for (let side = 0; side < 6 && coords.length < count; side += 1) {
      for (let step = 0; step < ring && coords.length < count; step += 1) {
        coords.push([q, r]);
        q += dirs[side][0];
        r += dirs[side][1];
      }
    }
    ring += 1;
  }
  return coords;
}

function layout(nodes: GraphNode[], links: GraphLink[]) {
  const count = nodes.length;
  if (!count) return;
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const neighbors = new Map<string, string[]>(nodes.map((node) => [node.id, []]));
  links.forEach((link) => {
    if (link.source.id === link.target.id) return;
    neighbors.get(link.source.id)?.push(link.target.id);
    neighbors.get(link.target.id)?.push(link.source.id);
  });
  const uniqueNeighbors = (id: string) => [...new Set(neighbors.get(id) ?? [])];
  const degree = (id: string) => uniqueNeighbors(id).length;
  const maxDegree = nodes.reduce((max, node) => Math.max(max, degree(node.id)), 0);
  const crowded = count >= 42 || maxDegree >= 18;
  const remaining = new Set(nodes.map((node) => node.id));
  let shift = 0;
  const placePacked = (ids: string[]) => {
    const coords = hexSpiral(ids.length);
    let minX = Infinity;
    let maxX = -Infinity;
    const points = coords.map(([q, r]) => {
      const x = SEPARATION * (q + r / 2);
      const y = SEPARATION * (Math.sqrt(3) / 2) * r;
      minX = Math.min(minX, x);
      maxX = Math.max(maxX, x);
      return { x, y };
    });
    ids.forEach((id, index) => {
      const node = byId.get(id);
      const point = points[index];
      if (!node || !point) return;
      node.x = shift + point.x - minX;
      node.y = point.y;
    });
    shift += maxX - minX + SEPARATION;
  };
  const placeRadial = (rootId: string) => {
    const children = new Map<string, string[]>();
    const depth = new Map<string, number>([[rootId, 0]]);
    const queue = [rootId];
    remaining.delete(rootId);
    while (queue.length) {
      const id = queue.shift()!;
      const nexts = [...new Set(neighbors.get(id) ?? [])].filter((next) => remaining.has(next));
      children.set(id, nexts);
      nexts.forEach((next) => {
        remaining.delete(next);
        depth.set(next, (depth.get(id) ?? 0) + 1);
        queue.push(next);
      });
    }
    const weight = new Map<string, number>();
    const weigh = (id: string): number => {
      const sum = (children.get(id) ?? []).reduce((total, child) => total + weigh(child), 1);
      weight.set(id, sum);
      return sum;
    };
    weigh(rootId);
    const slots: { id: string; depth: number; angle: number }[] = [];
    const assign = (id: string, start: number, end: number) => {
      slots.push({ id, depth: depth.get(id) ?? 0, angle: (start + end) / 2 });
      const kids = children.get(id) ?? [];
      const total = kids.reduce((sum, child) => sum + (weight.get(child) ?? 1), 0) || 1;
      let cursor = start;
      kids.forEach((child) => {
        const span = ((end - start) * (weight.get(child) ?? 1)) / total;
        assign(child, cursor, cursor + span);
        cursor += span;
      });
    };
    assign(rootId, -Math.PI / 2, (Math.PI * 3) / 2);
    const byDepth = new Map<number, typeof slots>();
    slots.forEach((slot) => {
      const list = byDepth.get(slot.depth) ?? [];
      list.push(slot);
      byDepth.set(slot.depth, list);
    });
    const maxDepth = slots.reduce((max, slot) => Math.max(max, slot.depth), 0);
    const radiusAt = [0];
    for (let level = 1; level <= maxDepth; level += 1) {
      const group = (byDepth.get(level) ?? []).slice().sort((left, right) => left.angle - right.angle);
      let minGap = Math.PI * 2;
      for (let index = 0; index < group.length; index += 1) {
        const current = group[index].angle;
        const next = group[(index + 1) % group.length].angle;
        const delta = index === group.length - 1 ? next + Math.PI * 2 - current : next - current;
        if (delta > 1e-4) minGap = Math.min(minGap, delta);
      }
      const spread = group.length > 1 ? SEPARATION / minGap : 0;
      radiusAt[level] = Math.max(radiusAt[level - 1] + SEPARATION, spread);
    }
    let maxX = shift;
    slots.forEach((slot) => {
      const node = byId.get(slot.id);
      if (!node) return;
      const radius = radiusAt[slot.depth] ?? 0;
      node.x = shift + Math.cos(slot.angle) * radius;
      node.y = Math.sin(slot.angle) * radius;
      maxX = Math.max(maxX, node.x);
    });
    shift = maxX + SEPARATION * 2;
  };
  const placeBlock = (hub: string, members: string[]) => {
    const allowed = new Set(members);
    const children = new Map<string, string[]>();
    const depth = new Map<string, number>([[hub, 0]]);
    const seen = new Set([hub]);
    const queue = [hub];
    while (queue.length) {
      const id = queue.shift()!;
      const nexts = uniqueNeighbors(id).filter((next) => allowed.has(next) && !seen.has(next));
      children.set(id, nexts);
      nexts.forEach((next) => {
        seen.add(next);
        depth.set(next, (depth.get(id) ?? 0) + 1);
        queue.push(next);
      });
    }
    const missed = members.filter((id) => !seen.has(id));
    if (missed.length) {
      const hubKids = children.get(hub) ?? [];
      missed.forEach((id) => {
        depth.set(id, 1);
        hubKids.push(id);
        children.set(id, []);
      });
      children.set(hub, hubKids);
    }
    const weight = new Map<string, number>();
    const weigh = (id: string): number => {
      const sum = (children.get(id) ?? []).reduce((total, child) => total + weigh(child), 1);
      weight.set(id, sum);
      return sum;
    };
    weigh(hub);
    const slots: { id: string; depth: number; angle: number }[] = [];
    const assign = (id: string, start: number, end: number) => {
      slots.push({ id, depth: depth.get(id) ?? 0, angle: (start + end) / 2 });
      const kids = children.get(id) ?? [];
      const total = kids.reduce((sum, child) => sum + (weight.get(child) ?? 1), 0) || 1;
      let cursor = start;
      kids.forEach((child) => {
        const span = ((end - start) * (weight.get(child) ?? 1)) / total;
        assign(child, cursor, cursor + span);
        cursor += span;
      });
    };
    assign(hub, -Math.PI / 2, (Math.PI * 3) / 2);
    const byDepth = new Map<number, typeof slots>();
    slots.forEach((slot) => {
      const list = byDepth.get(slot.depth) ?? [];
      list.push(slot);
      byDepth.set(slot.depth, list);
    });
    const deepest = slots.reduce((max, slot) => Math.max(max, slot.depth), 0);
    const radiusAt = [0];
    for (let level = 1; level <= deepest; level += 1) {
      const group = (byDepth.get(level) ?? []).slice().sort((left, right) => left.angle - right.angle);
      let minGap = Math.PI * 2;
      for (let index = 0; index < group.length; index += 1) {
        const current = group[index].angle;
        const next = group[(index + 1) % group.length].angle;
        const delta = index === group.length - 1 ? next + Math.PI * 2 - current : next - current;
        if (delta > 1e-4) minGap = Math.min(minGap, delta);
      }
      const spread = group.length > 1 ? SEPARATION / minGap : 0;
      radiusAt[level] = Math.max(radiusAt[level - 1] + SEPARATION, spread);
    }
    slots.forEach((slot) => {
      const node = byId.get(slot.id);
      if (!node) return;
      const radius = radiusAt[slot.depth] ?? 0;
      node.x = Math.cos(slot.angle) * radius;
      node.y = Math.sin(slot.angle) * radius;
    });
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    members.forEach((id) => {
      const node = byId.get(id);
      if (!node) return;
      minX = Math.min(minX, node.x - NODE_R);
      maxX = Math.max(maxX, node.x + NODE_R);
      minY = Math.min(minY, node.y - NODE_R);
      maxY = Math.max(maxY, node.y + NODE_R);
    });
    return { members, minX, minY, maxX, maxY, w: maxX - minX, h: maxY - minY };
  };
  const isCenter = (id: string) => {
    const deg = degree(id);
    if (deg < 3) return false;
    const leaves = uniqueNeighbors(id).filter((next) => degree(next) <= 2).length;
    return leaves >= 2 || deg >= Math.max(6, Math.ceil(maxDegree * 0.12));
  };
  const centers = nodes.map((node) => node.id).filter(isCenter)
    .sort((left, right) => degree(right) - degree(left) || left.localeCompare(right));
  if (!links.length) {
    placePacked(nodes.map((node) => node.id));
  } else if (crowded && centers.length >= 2) {
    const assigned = new Set<string>();
    const groups = centers.map((hub) => {
      assigned.add(hub);
      return { hub, members: [hub] };
    });
    const groupOf = new Map(groups.map((group, index) => [group.hub, index]));
    nodes.forEach((node) => {
      if (assigned.has(node.id) || isCenter(node.id)) return;
      let best = -1;
      let bestDegree = -1;
      uniqueNeighbors(node.id).forEach((next) => {
        const index = groupOf.get(next);
        if (index == null || degree(next) <= bestDegree) return;
        best = index;
        bestDegree = degree(next);
      });
      if (best >= 0) {
        groups[best].members.push(node.id);
        assigned.add(node.id);
      }
    });
    let attached = true;
    while (attached) {
      attached = false;
      nodes.forEach((node) => {
        if (assigned.has(node.id)) return;
        let best = -1;
        let bestDegree = -1;
        uniqueNeighbors(node.id).forEach((next) => {
          const index = groupOf.get(next) ?? groups.findIndex((group) => group.members.includes(next));
          if (index < 0) return;
          const anchor = groups[index].hub;
          if (degree(anchor) <= bestDegree) return;
          best = index;
          bestDegree = degree(anchor);
        });
        if (best >= 0) {
          groups[best].members.push(node.id);
          assigned.add(node.id);
          attached = true;
        }
      });
    }
    const boxes = groups.map((group) => placeBlock(group.hub, group.members));
    const stillLoose = nodes.filter((node) => !assigned.has(node.id)).map((node) => node.id);
    if (stillLoose.length) {
      stillLoose.forEach((id) => {
        const node = byId.get(id);
        if (node) {
          node.x = 0;
          node.y = 0;
        }
      });
      const coords = hexSpiral(stillLoose.length);
      let minX = Infinity;
      let maxX = -Infinity;
      let minY = Infinity;
      let maxY = -Infinity;
      const points = coords.map(([q, r]) => {
        const x = SEPARATION * (q + r / 2);
        const y = SEPARATION * (Math.sqrt(3) / 2) * r;
        minX = Math.min(minX, x);
        maxX = Math.max(maxX, x);
        minY = Math.min(minY, y);
        maxY = Math.max(maxY, y);
        return { x, y };
      });
      stillLoose.forEach((id, index) => {
        const node = byId.get(id);
        const point = points[index];
        if (!node || !point) return;
        node.x = point.x;
        node.y = point.y;
      });
      boxes.push({
        members: stillLoose,
        minX: minX - NODE_R,
        minY: minY - NODE_R,
        maxX: maxX + NODE_R,
        maxY: maxY + NODE_R,
        w: maxX - minX + NODE_R * 2,
        h: maxY - minY + NODE_R * 2,
      });
    }
    const gap = SEPARATION * 2.4;
    const total = boxes.reduce((sum, box) => sum + (box.w + gap) * (box.h + gap), 0);
    const rowLimit = Math.sqrt(total) * 1.45;
    boxes.sort((left, right) => right.w * right.h - left.w * left.h);
    let cursorX = 0;
    let cursorY = 0;
    let rowHeight = 0;
    boxes.forEach((box) => {
      if (cursorX > 0 && cursorX + box.w > rowLimit) {
        cursorX = 0;
        cursorY += rowHeight + gap;
        rowHeight = 0;
      }
      const dx = cursorX - box.minX;
      const dy = cursorY - box.minY;
      box.members.forEach((id) => {
        const node = byId.get(id);
        if (!node) return;
        node.x += dx;
        node.y += dy;
      });
      cursorX += box.w + gap;
      rowHeight = Math.max(rowHeight, box.h);
    });
  } else {
    while (remaining.size) {
      let rootId = remaining.values().next().value as string;
      remaining.forEach((id) => {
        if (degree(id) > degree(rootId)) rootId = id;
      });
      if (degree(rootId) === 0) {
        const ids = [...remaining];
        remaining.clear();
        placePacked(ids);
        break;
      }
      placeRadial(rootId);
    }
    const gap = NODE_R * 2 + 10;
    for (let tick = 0; tick < 12; tick += 1) {
      let moved = false;
      for (let i = 0; i < count; i += 1) {
        for (let j = i + 1; j < count; j += 1) {
          let dx = nodes[j].x - nodes[i].x;
          let dy = nodes[j].y - nodes[i].y;
          let dist = Math.hypot(dx, dy);
          if (dist < 1) {
            dx = 1;
            dy = 0;
            dist = 1;
          }
          if (dist >= gap) continue;
          moved = true;
          const push = (gap - dist) / 2;
          const ux = dx / dist;
          const uy = dy / dist;
          nodes[i].x -= ux * push;
          nodes[i].y -= uy * push;
          nodes[j].x += ux * push;
          nodes[j].y += uy * push;
        }
      }
      if (!moved) break;
    }
  }
}

function fit(nodes: GraphNode[], width: number, height: number): View {
  if (!nodes.length || width < 2 || height < 2) return { x: width / 2, y: height / 2, k: 1 };
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  nodes.forEach((node) => {
    minX = Math.min(minX, node.x - NODE_R - 24);
    maxX = Math.max(maxX, node.x + NODE_R + 24);
    minY = Math.min(minY, node.y - NODE_R - 24);
    maxY = Math.max(maxY, node.y + NODE_R + 28);
  });
  const boundsWidth = Math.max(1, maxX - minX);
  const boundsHeight = Math.max(1, maxY - minY);
  const k = Math.min(1.6, Math.max(0.12, Math.min((width - 28) / boundsWidth, (height - 28) / boundsHeight)));
  return { x: width / 2 - ((minX + maxX) / 2) * k, y: height / 2 - ((minY + maxY) / 2) * k, k };
}

function pairBends(links: GraphLink[]) {
  const groups = new Map<string, GraphLink[]>();
  links.forEach((link) => {
    const key = link.source.id === link.target.id
      ? `loop\0${link.source.id}`
      : link.source.id < link.target.id ? `${link.source.id}\0${link.target.id}` : `${link.target.id}\0${link.source.id}`;
    const list = groups.get(key) ?? [];
    list.push(link);
    groups.set(key, list);
  });
  const bends = new Map<string, number>();
  groups.forEach((list) => {
    list.forEach((link, index) => {
      const slot = index - (list.length - 1) / 2;
      bends.set(link.id, list.length === 1 && link.source.id !== link.target.id ? 0 : slot * 46);
    });
  });
  return bends;
}

function curveOf(link: GraphLink, bend: number): Curve {
  const sx = link.source.x;
  const sy = link.source.y;
  const tx = link.target.x;
  const ty = link.target.y;
  if (link.source.id === link.target.id) {
    return { sx, sy: sy - NODE_R, cx: sx + NODE_R + 26 + bend, cy: sy - 34, tx: sx + NODE_R, ty: sy };
  }
  const dx = tx - sx;
  const dy = ty - sy;
  const dist = Math.hypot(dx, dy) || 1;
  const ux = dx / dist;
  const uy = dy / dist;
  const px = -uy;
  const py = ux;
  return {
    sx: sx + ux * NODE_R + px * bend * 0.12,
    sy: sy + uy * NODE_R + py * bend * 0.12,
    cx: (sx + tx) / 2 + px * bend,
    cy: (sy + ty) / 2 + py * bend,
    tx: tx - ux * (NODE_R + 8) + px * bend * 0.12,
    ty: ty - uy * (NODE_R + 8) + py * bend * 0.12,
  };
}

function pointOnCurve(curve: Curve, t: number) {
  const u = 1 - t;
  return {
    x: u * u * curve.sx + 2 * u * t * curve.cx + t * t * curve.tx,
    y: u * u * curve.sy + 2 * u * t * curve.cy + t * t * curve.ty,
  };
}

function draw(ctx: CanvasRenderingContext2D, nodes: GraphNode[], links: GraphLink[], bends: Map<string, number>, view: View, hoverId: string, selectedId: string) {
  ctx.save();
  ctx.translate(view.x, view.y);
  ctx.scale(view.k, view.k);
  const nodeIds = new Set(nodes.map((node) => node.id));
  const focusNode = nodeIds.has(hoverId) ? hoverId : nodeIds.has(selectedId) ? selectedId : '';
  const many = links.length > 36;
  const paintLink = (link: GraphLink, emphasized: boolean) => {
    const curve = curveOf(link, bends.get(link.id) ?? 0);
    const alpha = emphasized ? 0.95 : many ? (focusNode ? 0.05 : 0.28) : 0.82;
    ctx.beginPath();
    ctx.moveTo(curve.sx, curve.sy);
    ctx.quadraticCurveTo(curve.cx, curve.cy, curve.tx, curve.ty);
    ctx.strokeStyle = emphasized ? `rgba(0, 145, 255, ${alpha})` : `rgba(125, 132, 154, ${alpha})`;
    ctx.lineWidth = (emphasized ? 2.2 : many ? 1.05 : 1.35) / view.k;
    ctx.stroke();
    const angle = Math.atan2(curve.ty - curve.cy, curve.tx - curve.cx);
    const size = 8 / view.k;
    ctx.beginPath();
    ctx.moveTo(curve.tx, curve.ty);
    ctx.lineTo(curve.tx - size * Math.cos(angle - 0.38), curve.ty - size * Math.sin(angle - 0.38));
    ctx.lineTo(curve.tx - size * Math.cos(angle + 0.38), curve.ty - size * Math.sin(angle + 0.38));
    ctx.closePath();
    ctx.fillStyle = emphasized ? `rgba(0, 145, 255, ${alpha})` : `rgba(125, 132, 154, ${alpha})`;
    ctx.fill();
  };
  const emphasizedLinks: GraphLink[] = [];
  links.forEach((link) => {
    const active = hoverId === link.id || selectedId === link.id;
    const incident = focusNode !== '' && (link.source.id === focusNode || link.target.id === focusNode);
    if (active || incident) emphasizedLinks.push(link);
    else paintLink(link, false);
  });
  emphasizedLinks.forEach((link) => paintLink(link, true));
  nodes.forEach((node) => {
    const hovered = hoverId === node.id;
    const selected = selectedId === node.id;
    ctx.beginPath();
    ctx.arc(node.x, node.y, NODE_R, 0, Math.PI * 2);
    ctx.fillStyle = node.color;
    ctx.fill();
    ctx.lineWidth = 2 / view.k;
    ctx.strokeStyle = '#ffffff';
    ctx.stroke();
    if (hovered || selected) {
      ctx.beginPath();
      ctx.arc(node.x, node.y, NODE_R + 4 / view.k, 0, Math.PI * 2);
      ctx.lineWidth = 3 / view.k;
      ctx.strokeStyle = selected ? 'rgba(32, 43, 73, 0.78)' : '#f5d84a';
      ctx.stroke();
    }
    const crowded = nodes.some((other) => other !== node && Math.hypot(other.x - node.x, other.y - node.y) * view.k < 64);
    if (!crowded || hovered || selected) {
      ctx.font = `${10 / view.k}px sans-serif`;
      ctx.fillStyle = '#4c5672';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillText(shortId(node.id), node.x, node.y + NODE_R + 4 / view.k);
    }
  });
  drawLabels(ctx, nodes, links, bends, view, hoverId, selectedId);
  ctx.restore();
}

function drawLabels(ctx: CanvasRenderingContext2D, nodes: GraphNode[], links: GraphLink[], bends: Map<string, number>, view: View, hoverId: string, selectedId: string) {
  const placed: { x: number; y: number }[] = [];
  ctx.font = `${11 / view.k}px sans-serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  links.forEach((link) => {
    if (!link.label) return;
    const active = hoverId === link.id || selectedId === link.id;
    if (!active && links.length > 40) return;
    const curve = curveOf(link, bends.get(link.id) ?? 0);
    const screenLen = Math.hypot(curve.tx - curve.sx, curve.ty - curve.sy) * view.k;
    if (!active && screenLen < 36) return;
    const textWidth = ctx.measureText(link.label).width;
    const worldW = textWidth + 8 / view.k;
    const worldH = 14 / view.k;
    let chosen = pointOnCurve(curve, 0.5);
    let clear = false;
    [0.5, 0.38, 0.66, 0.28, 0.78].forEach((t) => {
      if (clear) return;
      const point = pointOnCurve(curve, t);
      const sx = view.x + point.x * view.k;
      const sy = view.y + point.y * view.k;
      const nodeDist = nodes.reduce((min, node) => Math.min(min, Math.hypot(view.x + node.x * view.k - sx, view.y + node.y * view.k - sy)), 999);
      const labelDist = placed.reduce((min, box) => Math.min(min, Math.hypot(box.x - sx, box.y - sy)), 999);
      if (nodeDist >= NODE_R * view.k + 12 && labelDist >= 46) {
        chosen = point;
        clear = true;
      }
    });
    if (!active && !clear) return;
    placed.push({ x: view.x + chosen.x * view.k, y: view.y + chosen.y * view.k });
    ctx.save();
    ctx.translate(chosen.x, chosen.y);
    ctx.fillStyle = 'rgba(255,255,255,0.94)';
    ctx.fillRect(-worldW / 2, -worldH / 2, worldW, worldH);
    ctx.fillStyle = '#4c5672';
    ctx.fillText(link.label, 0, 0);
    ctx.restore();
  });
}

function nodeAt(nodes: GraphNode[], view: View, x: number, y: number) {
  const wx = (x - view.x) / view.k;
  const wy = (y - view.y) / view.k;
  for (let index = nodes.length - 1; index >= 0; index -= 1) {
    const node = nodes[index];
    if (Math.hypot(node.x - wx, node.y - wy) <= Math.max(NODE_R + 3, 9 / view.k)) return node;
  }
  return undefined;
}

function linkAt(links: GraphLink[], bends: Map<string, number>, view: View, x: number, y: number) {
  const wx = (x - view.x) / view.k;
  const wy = (y - view.y) / view.k;
  const limit = 10 / view.k;
  let found: GraphLink | undefined;
  let best = limit;
  links.forEach((link) => {
    const curve = curveOf(link, bends.get(link.id) ?? 0);
    const length = Math.hypot(curve.tx - curve.sx, curve.ty - curve.sy);
    const steps = Math.max(8, Math.ceil(length / 6));
    let previous = pointOnCurve(curve, 0);
    for (let step = 1; step <= steps; step += 1) {
      const point = pointOnCurve(curve, step / steps);
      const dx = point.x - previous.x;
      const dy = point.y - previous.y;
      const seg = dx * dx + dy * dy || 1;
      const t = Math.max(0, Math.min(1, ((wx - previous.x) * dx + (wy - previous.y) * dy) / seg));
      const dist = Math.hypot(previous.x + dx * t - wx, previous.y + dy * t - wy);
      if (dist < best) {
        best = dist;
        found = link;
      }
      previous = point;
    }
  });
  return found;
}

function nodeDetail(node: GraphNode): Detail {
  return { kind: 'node', id: node.id, typeName: node.typeName, properties: node.properties };
}

function edgeDetail(link: GraphLink): Detail {
  return { kind: 'edge', id: link.id, label: link.label, source: link.source.id, target: link.target.id, rank: link.rank, properties: link.properties };
}

export function QueryGraph({ rows, api }: { rows: unknown[][]; api?: CengyanApi }) {
  const graph = useMemo(() => parseGraph(rows), [rows]);
  const hostRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const sceneRef = useRef<{ nodes: GraphNode[]; paint: () => void } | null>(null);
  const [tip, setTip] = useState<{ x: number; y: number; text: string } | null>(null);
  const [detail, setDetail] = useState<Detail | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [resolved, setResolved] = useState<Map<string, { typeName: string; properties: Props }>>(() => new Map());
  const resolvedRef = useRef(resolved);
  resolvedRef.current = resolved;
  const types = useMemo(() => {
    const seen = new Map<string, string>();
    const add = (typeName: string) => {
      if (typeName && !seen.has(typeName)) seen.set(typeName, typeColor(typeName));
    };
    graph.nodes.forEach((node) => add(node.typeName));
    resolved.forEach((item) => add(item.typeName));
    return [...seen.entries()];
  }, [graph, resolved]);

  useEffect(() => {
    if (!graph.nodes.length) return undefined;
    let cancelled = false;
    let frame = 0;
    let tries = 0;
    let teardown = () => {};
    const boot = () => {
      if (cancelled) return;
      const host = hostRef.current;
      const canvas = canvasRef.current;
      if (!host || !canvas || host.clientWidth < 2 || host.clientHeight < 2) {
        if (tries < 90) {
          tries += 1;
          frame = requestAnimationFrame(boot);
        }
        return;
      }
      teardown = mountGraph(host, canvas);
    };
    const mountGraph = (host: HTMLDivElement, canvas: HTMLCanvasElement) => {
    const nodes = graph.nodes.map((node) => {
      const match = resolvedRef.current.get(node.id);
      const typeName = node.typeName || match?.typeName || '';
      const properties = Object.keys(node.properties).length ? { ...node.properties } : { ...(match?.properties || {}) };
      return { ...node, typeName, color: typeName ? typeColor(typeName) : node.color, properties };
    });
    const byId = new Map(nodes.map((node) => [node.id, node]));
    const links = graph.links.map((link) => ({ ...link, source: byId.get(link.source.id)!, target: byId.get(link.target.id)! }));
    layout(nodes, links);
    const bends = pairBends(links);
    const view: View = { x: 0, y: 0, k: 1 };
    let hoverId = '';
    let selectedId = '';
    let drag: { kind: 'pan' | 'node'; id?: string; x: number; y: number; ox: number; oy: number; moved: boolean } | null = null;

    let fittedWidth = 0;
    let fittedHeight = 0;
    const paint = () => {
      const width = host.clientWidth;
      const height = host.clientHeight;
      // 弹窗打开时祖先上的 scale 动画会把画布光栅化成低分辨率。至少按 2 倍像素绘制，避免全屏后被放大发糊。
      const ratio = Math.max(window.devicePixelRatio || 1, 2);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      canvas.width = Math.max(1, Math.floor(width * ratio));
      canvas.height = Math.max(1, Math.floor(height * ratio));
      const ctx = canvas.getContext('2d');
      if (!ctx) return;
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      ctx.imageSmoothingEnabled = true;
      ctx.imageSmoothingQuality = 'high';
      ctx.clearRect(0, 0, width, height);
      ctx.fillStyle = '#fbfcff';
      ctx.fillRect(0, 0, width, height);
      draw(ctx, nodes, links, bends, view, hoverId, selectedId);
    };
    const resize = () => {
      const width = host.clientWidth;
      const height = host.clientHeight;
      if (Math.abs(width - fittedWidth) < 2 && Math.abs(height - fittedHeight) < 2) {
        paint();
        return;
      }
      fittedWidth = width;
      fittedHeight = height;
      const next = fit(nodes, width, height);
      view.x = next.x;
      view.y = next.y;
      view.k = next.k;
      paint();
    };
    const pointerPoint = (event: { clientX: number; clientY: number }) => {
      const rect = canvas.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
    };
    const onPointerDown = (event: PointerEvent) => {
      const point = pointerPoint(event);
      const node = nodeAt(nodes, view, point.x, point.y);
      canvas.setPointerCapture(event.pointerId);
      if (node) {
        drag = { kind: 'node', id: node.id, x: point.x, y: point.y, ox: node.x, oy: node.y, moved: false };
        return;
      }
      const link = linkAt(links, bends, view, point.x, point.y);
      if (link) {
        selectedId = link.id;
        setDetail(edgeDetail(link));
        drag = null;
        paint();
        return;
      }
      selectedId = '';
      setDetail(null);
      drag = { kind: 'pan', x: point.x, y: point.y, ox: view.x, oy: view.y, moved: false };
      paint();
    };
    const onPointerMove = (event: PointerEvent) => {
      const point = pointerPoint(event);
      if (drag?.kind === 'node' && drag.id) {
        if (Math.hypot(point.x - drag.x, point.y - drag.y) > 3) drag.moved = true;
        if (drag.moved) {
          const node = byId.get(drag.id);
          if (node) {
            node.x = drag.ox + (point.x - drag.x) / view.k;
            node.y = drag.oy + (point.y - drag.y) / view.k;
          }
          canvas.style.cursor = 'grabbing';
          paint();
        }
        return;
      }
      if (drag?.kind === 'pan') {
        if (Math.hypot(point.x - drag.x, point.y - drag.y) > 3) drag.moved = true;
        view.x = drag.ox + point.x - drag.x;
        view.y = drag.oy + point.y - drag.y;
        canvas.style.cursor = 'grabbing';
        paint();
        return;
      }
      const node = nodeAt(nodes, view, point.x, point.y);
      const link = node ? undefined : linkAt(links, bends, view, point.x, point.y);
      const nextHover = node?.id ?? link?.id ?? '';
      canvas.style.cursor = nextHover ? 'pointer' : 'grab';
      if (nextHover !== hoverId) {
        hoverId = nextHover;
        setTip(node ? { x: point.x + 14, y: point.y + 14, text: node.typeName ? `${node.typeName} · ${node.id}` : node.id } : link ? { x: point.x + 14, y: point.y + 14, text: link.label } : null);
        paint();
      } else if (node || link) {
        setTip({ x: point.x + 14, y: point.y + 14, text: node ? (node.typeName ? `${node.typeName} · ${node.id}` : node.id) : link!.label });
      }
    };
    const onPointerUp = (event: PointerEvent) => {
      const point = pointerPoint(event);
      if (drag?.kind === 'node' && drag.id && !drag.moved) {
        const node = byId.get(drag.id);
        if (node) {
          selectedId = node.id;
          setDetail(nodeDetail(node));
          paint();
        }
      }
      drag = null;
      if (canvas.hasPointerCapture(event.pointerId)) canvas.releasePointerCapture(event.pointerId);
      canvas.style.cursor = nodeAt(nodes, view, point.x, point.y) || linkAt(links, bends, view, point.x, point.y) ? 'pointer' : 'grab';
    };
    const onWheel = (event: WheelEvent) => {
      event.preventDefault();
      const point = pointerPoint(event);
      const factor = Math.exp(-event.deltaY * 0.0012);
      const nextK = Math.min(4, Math.max(0.08, view.k * factor));
      view.x = point.x - (point.x - view.x) * (nextK / view.k);
      view.y = point.y - (point.y - view.y) * (nextK / view.k);
      view.k = nextK;
      paint();
    };
    const onPointerLeave = () => {
      if (drag) return;
      hoverId = '';
      setTip(null);
      paint();
    };
    sceneRef.current = { nodes, paint };
    const observer = new ResizeObserver(resize);
    observer.observe(host);
    canvas.addEventListener('pointerdown', onPointerDown);
    canvas.addEventListener('pointermove', onPointerMove);
    canvas.addEventListener('pointerup', onPointerUp);
    canvas.addEventListener('pointerleave', onPointerLeave);
    canvas.addEventListener('wheel', onWheel, { passive: false });
    resize();
    return () => {
      sceneRef.current = null;
      observer.disconnect();
      canvas.removeEventListener('pointerdown', onPointerDown);
      canvas.removeEventListener('pointermove', onPointerMove);
      canvas.removeEventListener('pointerup', onPointerUp);
      canvas.removeEventListener('pointerleave', onPointerLeave);
      canvas.removeEventListener('wheel', onWheel);
    };
    };
    boot();
    return () => {
      cancelled = true;
      cancelAnimationFrame(frame);
      teardown();
    };
  }, [graph, expanded]);

  useEffect(() => {
    if (!api) return undefined;
    const missing = graph.nodes.filter((node) => !node.typeName).map((node) => node.id);
    if (!missing.length) {
      setResolved(new Map());
      return undefined;
    }
    let live = true;
    const apply = (found: Map<string, { typeName: string; properties: Props }>) => {
      if (!live || !found.size) return;
      const scene = sceneRef.current;
      scene?.nodes.forEach((node) => {
        const match = found.get(node.id);
        if (!match || node.typeName) return;
        node.typeName = match.typeName;
        node.color = typeColor(match.typeName);
        if (!Object.keys(node.properties).length && Object.keys(match.properties).length) node.properties = match.properties;
      });
      scene?.paint();
      if (found.size) setResolved(new Map(found));
      setDetail((current) => {
        if (!current || current.kind !== 'node') return current;
        const match = found.get(current.id);
        if (!match || current.typeName) return current;
        return {
          ...current,
          typeName: match.typeName,
          properties: Object.keys(current.properties).length ? current.properties : match.properties,
        };
      });
    };
    const load = async () => {
      const found = new Map<string, { typeName: string; properties: Props }>();
      const groups: string[][] = [];
      for (let index = 0; index < missing.length; index += 40) groups.push(missing.slice(index, index + 40));
      await Promise.all(groups.map(async (ids) => {
        try {
          const response = await api.query(lookupQuery(ids), 30000);
          nodesFromResponse(response.rows || []).forEach((value, key) => found.set(key, value));
        } catch {
          // 整批失败时下面按点再查一次。
        }
        const absent = ids.filter((id) => !found.has(id));
        if (!absent.length || absent.length !== ids.length) return;
        await Promise.all(absent.map(async (id) => {
          try {
            const response = await api.query(`MATCH (n {id:${gqlString(id)}}) RETURN n`, 15000);
            nodesFromResponse(response.rows || []).forEach((value, key) => found.set(key, value));
          } catch {
            // 单个点查不到时保留灰色，不挡住已经能画出来的边。
          }
        }));
      }));
      apply(found);
    };
    void load();
    return () => { live = false; };
  }, [api, graph]);

  if (!graph.nodes.length) return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="当前结果里没有点或边" />;
  const fields = detail ? Object.entries(detail.properties) : [];
  const stage = (
    <div className="graph-stage" ref={hostRef}>
      <canvas ref={canvasRef} />
      <button type="button" className="graph-expand" aria-label={expanded ? '退出全屏' : '全屏'} onClick={() => setExpanded((open) => !open)}>
        {expanded ? <Minimize2 size={15} /> : <Maximize2 size={15} />}
      </button>
      {types.length > 0 && (
        <div className="graph-legend">
          {types.map(([typeName, color]) => <span key={typeName}><i style={{ background: color }} />{typeName}</span>)}
        </div>
      )}
      {tip && <div className="graph-tip" style={{ left: tip.x, top: tip.y }}>{tip.text}</div>}
      {detail && (
        <aside className="graph-detail">
          <div className="graph-detail-title">{detail.kind === 'node' ? '点' : '边'}</div>
          {detail.kind === 'node' ? (
            <>
              <div><span>编号</span><b>{detail.id}</b></div>
              <div><span>类型</span><b>{detail.typeName || '—'}</b></div>
            </>
          ) : (
            <>
              <div><span>类型</span><b>{detail.label}</b></div>
              <div><span>起点</span><b>{detail.source}</b></div>
              <div><span>终点</span><b>{detail.target}</b></div>
              <div><span>rank</span><b>{detail.rank}</b></div>
            </>
          )}
          <div className="graph-detail-props">{fields.length ? '属性' : '暂无属性'}</div>
          {fields.map(([key, value]) => <div key={key}><span>{key}</span><b>{formatValue(value)}</b></div>)}
        </aside>
      )}
    </div>
  );

  return (
    <div className="graph-result">
      <div className="graph-result-title">
        <Text type="secondary">{graph.links.length ? '拖动圆点分开连线，点击点和边查看属性' : '当前结果没有边。返回路径或边之后，这里会画出关系。'}</Text>
        <Tag color="purple" icon={<Workflow size={13} />}>{graph.nodes.length} 点 · {graph.links.length} 边</Tag>
      </div>
      {expanded ? <div className="graph-stage" /> : stage}
      <Modal
        className="graph-fullscreen"
        title="关系图"
        open={expanded}
        footer={null}
        width="90vw"
        centered
        destroyOnHidden
        transitionName=""
        maskTransitionName=""
        afterOpenChange={(open) => { if (open) sceneRef.current?.paint(); }}
        onCancel={() => setExpanded(false)}
        styles={{
          container: { height: '90vh', display: 'flex', flexDirection: 'column' },
          body: { flex: '1 1 auto', minHeight: 0, display: 'flex', overflow: 'hidden' },
        }}
      >
        {expanded ? stage : null}
      </Modal>
    </div>
  );
}
