from __future__ import annotations

from .exceptions import GraphContextError, SessionClosedError, raise_for_response
from .models import Property
from .result import Result
from .transport import HttpTransport, TcpTransport


class Client:
    def __init__(self, host, port=19669, user=None, password=None, transport="tcp", timeout=30.0, **kwargs):
        self.host, self.port, self.user, self.password = host, port, user, password
        self.transport_name, self.timeout, self.options = transport, timeout, kwargs
        self._sessions = set(); self._closed = False
        self.schema = RootSchemaManager(self)
        self.admin = RootAdminManager(self)

    def __enter__(self): return self
    def __exit__(self, *args): self.close()

    def _new_transport(self):
        cls = HttpTransport if self.transport_name == "http" else TcpTransport
        return cls(self.host, self.port, self.timeout, **self.options)

    def connect(self):
        if self._closed: raise SessionClosedError("client is closed", "SESSION_CLOSED")
        return self

    def login(self):
        transport = self._new_transport()
        try:
            transport.connect()
            response = transport.request({"op": "login", "user": self.user, "password": self.password})
            raise_for_response(response)
            return response
        finally:
            transport.close()

    def graph(self, name):
        if self._closed: raise SessionClosedError("client is closed", "SESSION_CLOSED")
        try:
            session = GraphSession(self, name)
        except Exception:
            raise
        self._sessions.add(session); return session

    def create_graph(self, name, comment=None):
        with self.graph(None) as session:
            query = "CREATE GRAPH " + name
            if comment:
                query += " COMMENT " + repr(comment)
            return session.execute(query)

    def drop_graph(self, name):
        with self.graph(None) as session:
            return session.execute("DROP GRAPH " + name)

    def close(self):
        for session in list(self._sessions):
            session.close()
        self._closed = True


class GraphSession:
    def __init__(self, client, graph_name):
        self.client = client; self.input_name = graph_name; self.graph_name = graph_name; self.canonical_name = graph_name
        self.role = None; self.session_id = ""; self.closed = False
        self.transport = client._new_transport()
        self.schema = SchemaManager(self); self.data = DataManager(self); self.bulk = BulkManager(self)
        self.admin = AdminManager(self); self.queries = QueryManager(self)
        self._enter()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _enter(self):
        try:
            self.transport.connect()
            response = self._raw({"op": "login", "user": self.client.user, "password": self.client.password})
            self.session_id = response.get("session", "")
            rows = response.get("rows") or []
            if rows and len(rows[0]) > 1:
                self.role = rows[0][1]
            if self.graph_name:
                self._raw({"op": "execute", "query": "USE " + self.graph_name, "params": {}, "timeoutMs": 0})
                aliases = self._raw({"op": "execute", "query": "SHOW ALIASES", "params": {}, "timeoutMs": 0})
                for row in aliases.get("rows", []):
                    if row and row[0] == self.input_name and len(row) > 1:
                        self.canonical_name = row[1]
                        self.graph_name = row[1]
                        break
        except Exception:
            self.transport.close()
            self.closed = True
            self.client._sessions.discard(self)
            raise

    def _raw(self, payload, timeout=None):
        if self.closed: raise SessionClosedError("graph session is closed", "SESSION_CLOSED")
        payload.setdefault("session", self.session_id)
        response = self.transport.request(payload, timeout)
        response = raise_for_response(response)
        if response.get("session"):
            self.session_id = response["session"]
        return response

    def execute(self, query, params=None, timeout_ms=None):
        if query.strip().upper().startswith("USE "):
            raise GraphContextError("use graph() to change graph context", "GRAPH_CONTEXT_ERROR")
        return Result(self._raw({"op":"execute", "query":query, "params":params or {}, "timeoutMs":timeout_ms or 0}, timeout_ms / 1000 if timeout_ms else None))

    def close(self):
        if self.closed: return
        try:
            if self.session_id:
                self.transport.request({"op":"logout", "session":self.session_id}, 5)
        except Exception: pass
        self.transport.close(); self.closed = True; self.client._sessions.discard(self)


class SchemaManager:
    def __init__(self, graph): self.graph = graph
    def node_types(self): return self.graph.execute("SHOW NODE TYPES").to_dicts()
    def rel_types(self): return self.graph.execute("SHOW REL TYPES").to_dicts()
    def describe_node_type(self, name): return self.graph.execute(f"DESCRIBE NODE TYPE {name}").to_dicts()
    def describe_rel_type(self, name): return self.graph.execute(f"DESCRIBE REL TYPE {name}").to_dicts()
    def create_graph(self, name, comment=None): return self.graph.execute(f"CREATE GRAPH {name}" + (f" COMMENT '{comment}'" if comment else ""))
    def create_node_type(self, name, id_type="INT", id_length=19, properties=(), comment=None):
        fields = ", ".join(_property_text(p) for p in properties)
        return self.graph.execute(f"CREATE NODE TYPE {name}" + (f" COMMENT '{comment}'" if comment else "") + f" ID {id_type}({id_length}) ({fields})")

    def create_rel_type(self, name, properties=(), comment=None):
        fields = ", ".join(_property_text(p) for p in properties)
        return self.graph.execute(f"CREATE REL TYPE {name}" + (f" COMMENT '{comment}'" if comment else "") + f" ({fields})")

    def create_index(self, name, target, type_name, properties, unique=False):
        prefix = "CREATE UNIQUE INDEX" if unique else "CREATE INDEX"
        cols = ", ".join(properties)
        return self.graph.execute(f"{prefix} {name} ON {target} TYPE {type_name} ({cols})")

    def drop_index(self, name): return self.graph.execute("DROP INDEX " + name)
    def show_indexes(self): return self.graph.execute("SHOW INDEXES").to_dicts()
    def alter_node_type(self, type_name, property):
        return self.graph.execute("ALTER NODE TYPE %s ADD %s %s NULL" % (type_name, property.name, property.type))
    def alter_rel_type(self, type_name, property):
        return self.graph.execute("ALTER REL TYPE %s ADD %s %s NULL" % (type_name, property.name, property.type))


class RootSchemaManager:
    def __init__(self, client): self.client = client
    def create_graph(self, name, comment=None): return self.client.create_graph(name, comment)
    def drop_graph(self, name): return self.client.drop_graph(name)


def _property_text(property):
    if isinstance(property, dict):
        name = property["name"]; type_name = property["type"]
        nullable = property.get("nullable", True); comment = property.get("comment")
    else:
        name = property.name; type_name = property.type
        nullable = property.nullable; comment = property.comment
    text = f"{name} {type_name}{' NULL' if nullable else ''}"
    return text + (" COMMENT " + repr(comment) if comment else "")


class DataManager:
    def __init__(self, graph): self.graph = graph
    def insert_node(self, type_name, node_id, properties=None):
        props = properties or {}; fields = ["id:$id"]; params = {"id": node_id}
        for index, (key, value) in enumerate(props.items()):
            parameter = "p_" + str(index); fields.append(f"{key}:${parameter}"); params[parameter] = value
        return self.graph.execute(f"INSERT (n:{type_name} {{{', '.join(fields)}}})", params=params)
    def insert_edge(self, type_name, src, dst, rank=0, properties=None):
        props = properties or {}; fields = []; params = {"src": src, "dst": dst, "rank": rank}
        for index, (key, value) in enumerate(props.items()):
            parameter = "p_" + str(index); fields.append(f"{key}:${parameter}"); params[parameter] = value
        suffix = " {" + ", ".join(fields) + "}" if fields else ""
        return self.graph.execute(f"INSERT (a {{id:$src}})-[:{type_name}@{int(rank)}{suffix}]->(b {{id:$dst}})", params=params)
    def set_node(self, type_name, node_id, properties):
        assignments = ", ".join(f"n.{key}=${index}" for index, key in enumerate(properties))
        params = {"p_" + str(index): value for index, value in enumerate(properties.values())}
        assignments = ", ".join(f"n.{key}=$p_{index}" for index, key in enumerate(properties))
        return self.graph.execute(f"MATCH (n:{type_name} {{id:$id}}) SET {assignments}", params={"id": node_id, **params})
    def delete_node(self, type_name, node_id):
        return self.graph.execute(f"MATCH (n:{type_name} {{id:$id}}) DELETE n", params={"id": node_id})
    def delete_edge(self, type_name, src, dst, rank=0):
        return self.graph.execute(
            f"MATCH (a {{id:$src}})-[e:{type_name}@{int(rank)}]->(b {{id:$dst}}) DELETE e",
            params={"src": src, "dst": dst},
        )


class BulkManager:
    def __init__(self, graph): self.graph = graph
    def insert_nodes(self, type_name, rows, batch_size=1024, ignore_existing=False):
        total = 0
        for i in range(0, len(rows), batch_size):
            response = self.graph._raw({"op":"bulkInsert", "kind":"node", "type":type_name, "rows":rows[i:i+batch_size], "ignoreExisting":ignore_existing})
            total += response.get("rowsAffected", 0)
        return total
    def insert_edges(self, type_name, rows, batch_size=1024, ignore_existing=False):
        total = 0
        for i in range(0, len(rows), batch_size):
            response = self.graph._raw({"op":"bulkInsert", "kind":"edge", "type":type_name, "rows":rows[i:i+batch_size], "ignoreExisting":ignore_existing})
            total += response.get("rowsAffected", 0)
        return total


class QueryManager:
    def __init__(self, graph): self.graph = graph
    def cancel(self, query_id): return self.graph._raw({"op":"cancel", "queryId":query_id})
    def list(self): return self.graph._raw({"op":"queries"}).get("rows", [])


class AdminManager:
    def __init__(self, graph): self.graph = graph
    def flush(self): return self.graph._raw({"op":"admin", "command":"flush"})
    def compact(self): return self.graph._raw({"op":"admin", "command":"compact"})
    def stats(self): return self.graph._raw({"op":"admin", "command":"stats"})

    def storage(self): return self.graph._raw({"op":"admin", "command":"storage"})
    def rebuild_index(self, name): return self.graph._raw({"op":"admin", "command":"rebuildIndex", "index": name})


class RootAdminManager:
    def __init__(self, client): self.client = client

    def _call(self, command, **values):
        with self.client.graph(None) as session:
            payload = {"op": "admin", "command": command}
            payload.update(values)
            return session._raw(payload)

    def create_user(self, user, password, role="read"):
        return self._call("createUser", user=user, password=password, role=role)
    def set_password(self, user, password):
        return self._call("setPassword", user=user, password=password)
    def set_role(self, user, role):
        return self._call("setRole", user=user, role=role)
    def drop_user(self, user): return self._call("dropUser", user=user)
    def add_replica(self, graph, replicas):
        return self._call("addReplica", graph=graph, replicas=replicas)
    def set_part_count(self, graph, parts):
        return self._call("setPartCount", graph=graph, parts=parts)
    def show_members(self, graph): return self._call("showMembers", graph=graph)
