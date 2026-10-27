const TYPES = ["BOOL", "INT64", "DOUBLE", "STRING", "DATETIME"];
const state = {
  session: sessionStorage.getItem("cengyan.session") || "",
  user: sessionStorage.getItem("cengyan.user") || "",
  role: sessionStorage.getItem("cengyan.role") || "",
  graph: sessionStorage.getItem("cengyan.graph") || "",
  page: "graphs",
};
const draft = { fields: [{ name: "", type: "STRING", nullable: true }] };

const app = document.getElementById("app");
app.addEventListener("click", onClick);
app.addEventListener("change", onChange);
app.addEventListener("submit", onSubmit);

boot();

async function boot() {
  if (!state.session) {
    renderLogin();
    return;
  }
  try {
    const names = await graphNames();
    if (state.graph && !names.includes(state.graph)) {
      state.graph = "";
      persist();
    }
    if (state.graph) {
      await gql("USE " + ident(state.graph));
    }
    renderShell(names);
  } catch (error) {
    clearSession();
    renderLogin(error.message);
  }
}

function persist() {
  sessionStorage.setItem("cengyan.session", state.session);
  sessionStorage.setItem("cengyan.user", state.user);
  sessionStorage.setItem("cengyan.role", state.role);
  sessionStorage.setItem("cengyan.graph", state.graph);
}

function clearSession() {
  state.session = "";
  state.user = "";
  state.role = "";
  state.graph = "";
  persist();
}

async function call(path, body, timeoutMs) {
  const payload = Object.assign({}, body || {});
  if (timeoutMs) {
    payload.timeoutMs = timeoutMs;
  }
  let response;
  try {
    response = await fetch(path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Cengyan-Session": state.session || "",
      },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    throw new Error("控制台没有响应");
  }
  const data = await response.json();
  if (data.session) {
    state.session = data.session;
    persist();
  }
  if (!response.ok || data.code !== "OK") {
    throw new Error((data.code ? data.code + " " : "") + (data.message || "请求失败"));
  }
  return data;
}

function gql(query, timeoutMs) {
  return call("/api/query", { query }, timeoutMs || 10000);
}

function admin(command, index, timeoutMs) {
  return call("/api/admin", { command, index: index || "" }, timeoutMs);
}

async function graphNames() {
  const data = await gql("SHOW GRAPHS");
  return (data.rows || []).map((row) => String(row[0]));
}

function ident(name) {
  if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(name) || name.length > 128) {
    throw new Error("名字只能用英文字母、数字和下划线，并且要以字母开头");
  }
  return name;
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[ch]));
}

function cellText(value) {
  if (value === null || value === undefined) {
    return "null";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function objects(data) {
  const columns = data.columns || [];
  return (data.rows || []).map((row) => {
    const item = {};
    columns.forEach((column, index) => {
      item[column] = row[index];
    });
    return item;
  });
}

function renderLogin(message) {
  app.innerHTML = `
    <form class="login" data-action="login">
      <h1>层岩控制台</h1>
      <p class="muted">先启动图数据库，再在这里登录。默认用户 root，密码见 cengyan.properties。</p>
      <label>用户</label>
      <input name="user" value="root" autocomplete="username">
      <label>密码</label>
      <input name="password" type="password" value="cengyan" autocomplete="current-password">
      <div class="row">
        <button class="primary" type="submit">登录</button>
        <button class="quiet" type="button" data-action="ping">检查连接</button>
      </div>
      <p class="error" id="error">${esc(message || "")}</p>
    </form>`;
}

function renderShell(names) {
  app.innerHTML = `
    <div class="shell">
      <nav class="nav">
        <strong>层岩控制台</strong>
        <button type="button" data-page="graphs">图</button>
        <button type="button" data-page="nodes">点类型</button>
        <button type="button" data-page="rels">边类型</button>
        <button type="button" data-page="indexes">索引</button>
        <button type="button" data-page="stats">统计</button>
        <button type="button" data-page="console">控制台</button>
      </nav>
      <div>
        <div class="top">
          <span>${esc(state.user)} · ${esc(state.role)}</span>
          <select id="graph-select">${graphOptions(names)}</select>
          <button class="quiet" type="button" data-action="logout">退出</button>
        </div>
        <main class="main" id="main"></main>
      </div>
    </div>`;
  document.getElementById("graph-select").value = state.graph;
  markNav();
  showPage();
}

function graphOptions(names) {
  return `<option value="">未选择图</option>` + names.map((name) => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
}

function markNav() {
  document.querySelectorAll("[data-page]").forEach((button) => {
    button.classList.toggle("active", button.dataset.page === state.page);
  });
}

async function showPage() {
  markNav();
  const main = document.getElementById("main");
  main.innerHTML = `<p class="muted">正在加载…</p>`;
  try {
    if (state.page === "graphs") {
      await renderGraphs(main);
    } else if (state.page === "nodes") {
      await renderTypes(main, true);
    } else if (state.page === "rels") {
      await renderTypes(main, false);
    } else if (state.page === "indexes") {
      await renderIndexes(main);
    } else if (state.page === "stats") {
      await renderStats(main);
    } else {
      renderConsole(main);
    }
  } catch (error) {
    main.innerHTML = `<p class="error">${esc(error.message)}</p>`;
  }
}

async function renderGraphs(main) {
  const names = await graphNames();
  fillGraphSelect(names);
  main.innerHTML = `
    <h1>图</h1>
    <div class="row">
      <input id="graph-name" placeholder="图名，例如 Company">
      <button class="primary" type="button" data-action="create-graph">创建</button>
    </div>
    <p class="error" id="error"></p>
    ${table(["图名", ""], names.map((name) => [
      esc(name),
      `<button class="quiet" type="button" data-action="use-graph" data-name="${esc(name)}">进入</button>
       <button class="quiet" type="button" data-action="drop-graph" data-name="${esc(name)}">删除</button>`,
    ]))}
    <p class="muted">删除图会删掉里面的类型、索引和数据。</p>`;
}

function fillGraphSelect(names) {
  const select = document.getElementById("graph-select");
  if (!select) {
    return;
  }
  const current = select.value || state.graph;
  select.innerHTML = graphOptions(names);
  select.value = names.includes(current) ? current : "";
}

async function renderTypes(main, node) {
  requireGraph();
  const kind = node ? "NODE" : "REL";
  const title = node ? "点类型" : "边类型";
  const data = await gql(node ? "SHOW NODE TYPES" : "SHOW REL TYPES");
  const rows = objects(data);
  main.innerHTML = `
    <h1>${title}</h1>
    <p class="muted">当前图 ${esc(state.graph)}。建好之后只能在末尾追加可空列。</p>
    ${table(["名字", "编号", "版本", ""], rows.map((row) => [
      esc(row.name),
      esc(row.id),
      esc(row.schemaVersion),
      `<button class="quiet" type="button" data-action="describe-type" data-node="${node}" data-name="${esc(row.name)}">查看列</button>`,
    ]))}
    <div id="detail"></div>
    <h2>新建</h2>
    <div class="row"><input id="type-name" placeholder="类型名"></div>
    <div id="fields">${draft.fields.map(fieldRow).join("")}</div>
    <div class="actions row">
      <button class="quiet" type="button" data-action="add-field">添加列</button>
      <button class="primary" type="button" data-action="create-type" data-node="${node}">创建${title}</button>
    </div>
    <h2>追加可空列</h2>
    <div class="row">
      <select id="alter-type">${rows.map((row) => `<option>${esc(row.name)}</option>`).join("")}</select>
      <input id="alter-prop" placeholder="新列名">
      <select id="alter-value-type">${TYPES.map((type) => `<option>${type}</option>`).join("")}</select>
      <button class="primary" type="button" data-action="alter-type" data-node="${node}">追加</button>
    </div>
    <p class="error" id="error"></p>`;
}

function fieldRow(field) {
  return `
    <div class="field-row">
      <input class="field-name" placeholder="列名" value="${esc(field.name)}">
      <select class="field-type">${TYPES.map((type) => `<option ${type === field.type ? "selected" : ""}>${type}</option>`).join("")}</select>
      <label><input class="field-null" type="checkbox" ${field.nullable ? "checked" : ""}>可空</label>
      <button class="quiet" type="button" data-action="remove-field">去掉</button>
    </div>`;
}

function readFields() {
  return [...document.querySelectorAll(".field-row")].map((row) => ({
    name: row.querySelector(".field-name").value.trim(),
    type: row.querySelector(".field-type").value,
    nullable: row.querySelector(".field-null").checked,
  }));
}

async function renderIndexes(main) {
  requireGraph();
  const data = await gql("SHOW INDEXES");
  const rows = objects(data);
  const nodes = objects(await gql("SHOW NODE TYPES")).map((row) => row.name);
  const rels = objects(await gql("SHOW REL TYPES")).map((row) => row.name);
  main.innerHTML = `
    <h1>索引</h1>
    <p class="muted">当前图 ${esc(state.graph)}。新建之后 ready 为 false，查询还不会用它，需要重建。</p>
    ${table(["名字", "目标", "类型", "列", "唯一", "可用", ""], rows.map((row) => [
      esc(row.name),
      esc(row.target),
      esc(row.type),
      esc(row.properties),
      row.unique ? "是" : "否",
      row.ready ? "是" : "否",
      `<button class="quiet" type="button" data-action="rebuild-index" data-name="${esc(row.name)}">重建</button>
       <button class="quiet" type="button" data-action="drop-index" data-name="${esc(row.name)}">删除</button>`,
    ]))}
    <h2>新建索引</h2>
    <div class="row">
      <input id="index-name" placeholder="索引名">
      <select id="index-target">
        <option value="NODE">点类型</option>
        <option value="REL">边类型</option>
      </select>
      <select id="index-type"></select>
      <label><input id="index-unique" type="checkbox">唯一</label>
      <button class="primary" type="button" data-action="create-index">创建</button>
    </div>
    <div id="index-props"></div>
    <p class="error" id="error"></p>`;
  const typeSelect = document.getElementById("index-type");
  typeSelect.dataset.nodes = JSON.stringify(nodes);
  typeSelect.dataset.rels = JSON.stringify(rels);
  fillIndexTypes();
  await loadIndexProps();
}

function fillIndexTypes() {
  const target = document.getElementById("index-target").value;
  const typeSelect = document.getElementById("index-type");
  const names = JSON.parse(target === "NODE" ? typeSelect.dataset.nodes : typeSelect.dataset.rels);
  typeSelect.innerHTML = names.map((name) => `<option>${esc(name)}</option>`).join("");
}

async function loadIndexProps() {
  const box = document.getElementById("index-props");
  const typeSelect = document.getElementById("index-type");
  if (!typeSelect.value) {
    box.innerHTML = `<p class="muted">这个图里还没有对应的类型。</p>`;
    return;
  }
  const node = document.getElementById("index-target").value === "NODE";
  const described = await gql(`DESCRIBE ${node ? "NODE" : "REL"} TYPE ${ident(typeSelect.value)}`);
  const props = objects(described);
  box.innerHTML = props.length
    ? props.map((prop) => `
        <label class="check-row">
          <input type="checkbox" class="index-prop" value="${esc(prop.property)}">
          ${esc(prop.property)} ${esc(prop.type)}${prop.nullable ? " NULL" : ""}
        </label>`).join("")
    : `<p class="muted">这个类型没有列，不能建索引。</p>`;
}

async function renderStats(main) {
  requireGraph();
  const data = await admin("stats", "", 60000);
  const rows = objects(data);
  let nodes = 0;
  let edges = 0;
  for (const row of rows) {
    if (row.target === "NODE") {
      nodes += Number(row.count);
    } else if (row.target === "REL") {
      edges += Number(row.count);
    }
  }
  main.innerHTML = `
    <h1>统计</h1>
    <p class="muted">当前图 ${esc(state.graph)}。数量按类型分开，最大度数是观察值。</p>
    <p>点 ${nodes} · 边 ${edges}</p>
    <button class="quiet" type="button" data-action="reload">刷新</button>
    ${table(["目标", "名字", "数量", "最大度数"], rows.map((row) => [
      esc(row.target), esc(row.name), esc(row.count), esc(row.maxDegree),
    ]))}
    <p class="error" id="error"></p>`;
}

function renderConsole(main) {
  main.innerHTML = `
    <h1>控制台</h1>
    <p class="muted">一次提交一行。边要写类型，起点要写类型或编号。要画出图，RETURN 里需要整点或整边。</p>
    <textarea id="gql" placeholder="MATCH (a:Person)-[e:knows]->(b) RETURN a, e, b"></textarea>
    <div class="row">
      <button class="primary" type="button" data-action="run-query">执行</button>
    </div>
    <p class="error" id="error"></p>
    <div id="result"></div>`;
}

function table(headers, rows) {
  if (!rows.length) {
    return `<p class="muted">没有记录。</p>`;
  }
  return `<table><thead><tr>${headers.map((header) => `<th>${esc(header)}</th>`).join("")}</tr></thead>
    <tbody>${rows.map((row) => `<tr>${row.map((cell) => `<td>${cell}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
}

function requireGraph() {
  if (!state.graph) {
    throw new Error("先在「图」里进入一张图");
  }
}

function showError(error) {
  const node = document.getElementById("error");
  if (node) {
    node.textContent = error && error.message ? error.message : "";
  }
}

async function onSubmit(event) {
  const form = event.target.closest("form[data-action='login']");
  if (!form) {
    return;
  }
  event.preventDefault();
  try {
    await login(form);
  } catch (error) {
    form.querySelector(".error").textContent = error.message;
  }
}

async function onClick(event) {
  const pageButton = event.target.closest("[data-page]");
  if (pageButton) {
    state.page = pageButton.dataset.page;
    await showPage();
    return;
  }
  const target = event.target.closest("[data-action]");
  if (!target) {
    return;
  }
  const action = target.dataset.action;
  try {
    if (action === "ping") {
      await call("/api/ping", {});
      showError({ message: "图数据库已连通" });
      return;
    }
    if (action === "logout") {
      clearSession();
      renderLogin();
      return;
    }
    if (action === "create-graph") {
      await createGraph();
      return;
    }
    if (action === "use-graph") {
      await useGraph(target.dataset.name);
      return;
    }
    if (action === "drop-graph") {
      await dropGraph(target.dataset.name);
      return;
    }
    if (action === "add-field") {
      draft.fields = readFields();
      draft.fields.push({ name: "", type: "STRING", nullable: true });
      await showPage();
      return;
    }
    if (action === "remove-field") {
      const rows = [...document.querySelectorAll(".field-row")];
      const index = rows.indexOf(target.closest(".field-row"));
      draft.fields = readFields();
      if (draft.fields.length > 1) {
        draft.fields.splice(index, 1);
      }
      await showPage();
      return;
    }
    if (action === "create-type") {
      await createType(target.dataset.node === "true");
      return;
    }
    if (action === "alter-type") {
      await alterType(target.dataset.node === "true");
      return;
    }
    if (action === "describe-type") {
      await describeType(target.dataset.node === "true", target.dataset.name);
      return;
    }
    if (action === "create-index") {
      await createIndex();
      return;
    }
    if (action === "rebuild-index") {
      target.disabled = true;
      await admin("rebuildIndex", target.dataset.name, 120000);
      await showPage();
      return;
    }
    if (action === "drop-index") {
      if (!window.confirm("删除索引 " + target.dataset.name + "？")) {
        return;
      }
      await gql("DROP INDEX " + ident(target.dataset.name));
      await showPage();
      return;
    }
    if (action === "reload") {
      await showPage();
      return;
    }
    if (action === "run-query") {
      await runQuery();
    }
  } catch (error) {
    showError(error);
    target.disabled = false;
  }
}

async function onChange(event) {
  if (event.target.id === "graph-select") {
    try {
      await useGraph(event.target.value);
    } catch (error) {
      showError(error);
    }
    return;
  }
  if (event.target.id === "index-target") {
    fillIndexTypes();
    try {
      await loadIndexProps();
    } catch (error) {
      showError(error);
    }
    return;
  }
  if (event.target.id === "index-type") {
    try {
      await loadIndexProps();
    } catch (error) {
      showError(error);
    }
  }
}

async function login(form) {
  const data = await call("/api/login", {
    user: form.user.value.trim(),
    password: form.password.value,
  });
  state.user = data.rows && data.rows[0] ? data.rows[0][0] : form.user.value.trim();
  state.role = data.rows && data.rows[0] ? data.rows[0][1] : "";
  persist();
  const names = await graphNames();
  renderShell(names);
}

async function createGraph() {
  const name = ident(document.getElementById("graph-name").value.trim());
  await gql("CREATE GRAPH " + name);
  await useGraph(name);
}

async function useGraph(name) {
  if (!name) {
    state.graph = "";
    persist();
    const select = document.getElementById("graph-select");
    if (select) {
      select.value = "";
    }
    await showPage();
    return;
  }
  await gql("USE " + ident(name));
  state.graph = name;
  persist();
  const select = document.getElementById("graph-select");
  if (select) {
    select.value = name;
  }
  if (state.page !== "console") {
    await showPage();
  }
}

async function dropGraph(name) {
  if (!window.confirm("删除图 " + name + "？图里的数据会一起删除。")) {
    return;
  }
  await gql("DROP GRAPH " + ident(name));
  if (state.graph === name) {
    state.graph = "";
    persist();
  }
  await showPage();
}

async function createType(node) {
  const name = ident(document.getElementById("type-name").value.trim());
  const fields = readFields().filter((field) => field.name);
  const columns = fields.map((field) => ident(field.name) + " " + field.type + (field.nullable ? " NULL" : ""));
  const keyword = node ? "NODE" : "REL";
  await gql(`CREATE ${keyword} TYPE ${name} (${columns.join(", ")})`);
  draft.fields = [{ name: "", type: "STRING", nullable: true }];
  await showPage();
}

async function alterType(node) {
  const typeName = ident(document.getElementById("alter-type").value);
  const property = ident(document.getElementById("alter-prop").value.trim());
  const valueType = document.getElementById("alter-value-type").value;
  const keyword = node ? "NODE" : "REL";
  await gql(`ALTER ${keyword} TYPE ${typeName} ADD ${property} ${valueType} NULL`);
  await showPage();
}

async function describeType(node, name) {
  const data = await gql(`DESCRIBE ${node ? "NODE" : "REL"} TYPE ${ident(name)}`);
  const detail = document.getElementById("detail");
  detail.innerHTML = `<h2>${esc(name)}</h2>` + table(
    ["列", "类型", "可空"],
    objects(data).map((row) => [esc(row.property), esc(row.type), row.nullable ? "是" : "否"]),
  );
}

async function createIndex() {
  const name = ident(document.getElementById("index-name").value.trim());
  const node = document.getElementById("index-target").value === "NODE";
  const typeName = ident(document.getElementById("index-type").value);
  const props = [...document.querySelectorAll(".index-prop:checked")].map((input) => ident(input.value));
  if (!props.length) {
    throw new Error("至少选一列");
  }
  const unique = document.getElementById("index-unique").checked ? "UNIQUE " : "";
  await gql(`CREATE ${unique}INDEX ${name} ON ${node ? "NODE" : "REL"} TYPE ${typeName} (${props.join(", ")})`);
  await showPage();
}

async function runQuery() {
  const text = document.getElementById("gql").value.trim();
  if (!text) {
    throw new Error("语句不能为空");
  }
  showError({ message: "" });
  const result = document.getElementById("result");
  result.innerHTML = "";
  const data = await gql(text, 30000);
  const columns = data.columns || [];
  const rows = data.rows || [];
  let html = "";
  if (!columns.length) {
    html += `<p>完成，影响 ${esc(data.rowsAffected || 0)} 行。</p>`;
  } else {
    html += table(columns, rows.map((row) => row.map((value) => esc(cellText(value)))));
  }
  const model = graphModel(rows);
  html += model.nodes.length
    ? `<canvas id="graph"></canvas><p class="muted">点 ${model.nodes.length} · 边 ${model.edges.length}。可以拖动圆点。</p>`
    : `<p class="muted">这次结果里没有完整的点或边，所以没有图。</p>`;
  result.innerHTML = html;
  if (model.nodes.length) {
    drawGraph(document.getElementById("graph"), model);
  }
}

function graphModel(rows) {
  const nodes = new Map();
  const edges = [];
  const seen = new Set();
  for (const row of rows) {
    for (const cell of row) {
      if (!cell || typeof cell !== "object" || Array.isArray(cell)) {
        continue;
      }
      if (cell.src != null && cell.dst != null && cell.type != null && cell.properties) {
        const key = [cell.type, cell.rank, cell.src, cell.dst].join("|");
        if (!seen.has(key)) {
          seen.add(key);
          edges.push(cell);
        }
        ensureNode(nodes, cell.src);
        ensureNode(nodes, cell.dst);
      } else if (cell.id != null && cell.type != null && cell.properties) {
        nodes.set(String(cell.id), cell);
      }
    }
  }
  return { nodes: [...nodes.values()], edges };
}

function ensureNode(nodes, id) {
  const key = String(id);
  if (!nodes.has(key)) {
    nodes.set(key, { id, type: "", properties: {} });
  }
}

function drawGraph(canvas, model) {
  const width = Math.max(canvas.clientWidth, 640);
  const height = 420;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  canvas.style.height = height + "px";
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  const nodes = model.nodes.map((node, index) => {
    const angle = (Math.PI * 2 * index) / model.nodes.length;
    return {
      id: String(node.id),
      label: (node.type ? node.type + " " : "") + node.id,
      x: width / 2 + Math.cos(angle) * 160,
      y: height / 2 + Math.sin(angle) * 120,
    };
  });
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const links = model.edges.map((edge) => ({
    source: byId.get(String(edge.src)),
    target: byId.get(String(edge.dst)),
    label: edge.type,
  })).filter((link) => link.source && link.target);
  for (let step = 0; step < 80; step++) {
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        push(nodes[i], nodes[j]);
      }
    }
    for (const link of links) {
      pull(link.source, link.target);
    }
    for (const node of nodes) {
      node.x += (width / 2 - node.x) * 0.02;
      node.y += (height / 2 - node.y) * 0.02;
    }
  }
  paint();
  let drag = null;
  canvas.onpointerdown = (event) => {
    const point = localPoint(event);
    drag = nodes.find((node) => distance(node, point) < 18) || null;
  };
  canvas.onpointermove = (event) => {
    if (!drag) {
      return;
    }
    const point = localPoint(event);
    drag.x = point.x;
    drag.y = point.y;
    paint();
  };
  canvas.onpointerup = () => {
    drag = null;
  };

  function localPoint(event) {
    const rect = canvas.getBoundingClientRect();
    return { x: event.clientX - rect.left, y: event.clientY - rect.top };
  }

  function paint() {
    ctx.clearRect(0, 0, width, height);
    ctx.strokeStyle = "#8aa0c8";
    ctx.fillStyle = "#334155";
    ctx.font = "12px sans-serif";
    for (const link of links) {
      ctx.beginPath();
      ctx.moveTo(link.source.x, link.source.y);
      ctx.lineTo(link.target.x, link.target.y);
      ctx.stroke();
      ctx.fillText(link.label, (link.source.x + link.target.x) / 2, (link.source.y + link.target.y) / 2);
    }
    for (const node of nodes) {
      ctx.beginPath();
      ctx.fillStyle = "#1f4b99";
      ctx.arc(node.x, node.y, 14, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "#1c1c1c";
      ctx.textAlign = "center";
      ctx.fillText(node.label, node.x, node.y + 28);
      ctx.textAlign = "left";
    }
  }
}

function push(a, b) {
  let dx = a.x - b.x;
  let dy = a.y - b.y;
  let dist = Math.hypot(dx, dy);
  if (dist < 1) {
    dx = 1;
    dy = 0;
    dist = 1;
  }
  const min = 110;
  const force = dist < min ? (min - dist) * 0.5 : 600 / (dist * dist);
  dx = (dx / dist) * force;
  dy = (dy / dist) * force;
  a.x += dx;
  a.y += dy;
  b.x -= dx;
  b.y -= dy;
}

function pull(a, b) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  a.x += dx * 0.02;
  a.y += dy * 0.02;
  b.x -= dx * 0.02;
  b.y -= dy * 0.02;
}

function distance(node, point) {
  return Math.hypot(node.x - point.x, node.y - point.y);
}
