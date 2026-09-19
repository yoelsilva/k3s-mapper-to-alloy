#!/usr/bin/env python3
"""
dependencias-mapper

Construye el mapa de dependencias declaradas de un clúster Kubernetes y lo
expone como métricas Prometheus.

  1. Lee Deployments y StatefulSets de los namespaces configurados, y los
     ConfigMaps que referencian (envFrom / valueFrom).
  2. Deduce dependencias de las variables de entorno: URLs, host:puerto,
     y hosts sueltos en claves tipo *_HOST / *_URL.
  3. Traduce cada Service interno a su workload dueño (por selector), de modo
     que "gateway -> controlserver-svc-grpc" y "controlserver -> redis" comparten
     el nodo "controlserver" en el grafo.
  4. Sondea por TCP cada destino:puerto distinto (deduplicado) y expone:

       dependencia{src,dst,dst_svc,dst_addr,dst_port,clave,externo}  1 ok | 0 fallo | 2 no sondeado
       dependencia_duracion_segundos{dst,dst_port}
       dependencia_fallo_motivo{dst,dst_port,motivo}  1
       dependencia_mapper_*                            salud del propio mapper

Solo librería estándar. Solo lectura. Nunca imprime valores de variables:
únicamente host, puerto y nombre de la clave.

Endpoints HTTP (puerto 9400):
  /metrics   formato Prometheus
  /healthz   200 si el último ciclo es reciente, 503 si no
  /flechas   JSON con las aristas deducidas (para depurar el parseo)

Configuración: fichero JSON en $CONFIG (por defecto /config/config.json).
Ejecución local: `kubectl proxy` y KUBE_API_URL=http://127.0.0.1:8001.
"""
import json
import os
import re
import socket
import ssl
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "0.1.0"


def log(*a):
    print(time.strftime("%Y-%m-%dT%H:%M:%S"), *a, file=sys.stderr, flush=True)


# ── conexión a la API de Kubernetes ──────────────────────────────────────
SA = "/var/run/secrets/kubernetes.io/serviceaccount"
IN_CLUSTER = bool(os.environ.get("KUBERNETES_SERVICE_HOST"))
if IN_CLUSTER:
    API = "https://%s:%s" % (os.environ["KUBERNETES_SERVICE_HOST"],
                             os.environ["KUBERNETES_SERVICE_PORT"])
    CTX = ssl.create_default_context(cafile=SA + "/ca.crt")
else:
    # Desarrollo local: `kubectl proxy` expone la API sin autenticación en :8001
    API = os.environ.get("KUBE_API_URL", "http://127.0.0.1:8001")
    CTX = None


def token():
    if not IN_CLUSTER:
        return None
    # El token del ServiceAccount rota: se relee en cada llamada.
    with open(SA + "/token") as f:
        return f.read().strip()


def api_get(path):
    req = urllib.request.Request(API + path)
    t = token()
    if t:
        req.add_header("Authorization", "Bearer " + t)
    with urllib.request.urlopen(req, context=CTX, timeout=20) as r:
        return json.load(r).get("items", [])


# ── configuración ────────────────────────────────────────────────────────
def cargar_config():
    ruta = os.environ.get("CONFIG", "/config/config.json")
    cfg = {}
    if os.path.exists(ruta):
        with open(ruta) as f:
            cfg = json.load(f)
    # Variables de entorno que sobrescriben el fichero (útil en Helm/Compose)
    if os.environ.get("NAMESPACES"):
        cfg["namespaces"] = [n.strip() for n in os.environ["NAMESPACES"].split(",") if n.strip()]
    return cfg


CFG = cargar_config()
NAMESPACES = CFG.get("namespaces", ["default"])
INTERVALO = int(CFG.get("intervalo_segundos", 60))
TIMEOUT = float(CFG.get("timeout_sonda_segundos", 3))
PUERTO_HTTP = int(os.environ.get("PORT", CFG.get("puerto_http", 9400)))
ALIAS = CFG.get("alias", {})
NO_SONDEAR = set(CFG.get("no_sondear", []))
HOSTS_IGNORAR = set(CFG.get("hosts_ignorar", ["0.0.0.0", "127.0.0.1", "localhost", "::", "::1"]))
RE_CLAVE = re.compile(CFG.get("claves_regex",
                              "(HOST|HOSTS|URL|URLS|URI|ADDR|ADDRESS|ENDPOINT|BROKER|BROKERS|SERVER)$"), re.I)
RE_EXCLUIR = re.compile(CFG.get("claves_excluir_regex",
                                "(CORS|ORIGIN|ALLOWLIST|WHITELIST|PUBLIC|DOMAIN|THIS_URL|REDIRECT)"), re.I)
PREF_NAVEGADOR = tuple(CFG.get("prefijos_navegador", ["VITE_", "REACT_APP_", "NEXT_PUBLIC_", "VUE_APP_"]))
INCLUIR_NAVEGADOR = bool(CFG.get("incluir_navegador", False))
REDES_PRIVADAS = tuple(CFG.get("redes_privadas", ["10.", "172.16.", "172.17.", "172.18.", "172.19.",
                                                  "172.2", "172.30.", "172.31.", "192.168."]))

RE_IP = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")
RE_URL = re.compile(r"^([a-z][a-z0-9+.-]*)://(?:[^@/\s]*@)?([^/:?#\s]+)(?::(\d{2,5}))?", re.I)
RE_HOST = re.compile(r"^([a-z0-9][a-z0-9.-]*)(?::(\d{2,5}))?$", re.I)
PUERTO_ESQUEMA = {"http": "80", "https": "443", "postgresql": "5432", "postgres": "5432",
                  "redis": "6379", "rediss": "6380", "mqtt": "1883", "mqtts": "8883",
                  "amqp": "5672", "amqps": "5671", "mongodb": "27017", "mysql": "3306",
                  "smtp": "25", "smtps": "465", "grpc": ""}
PUERTO_CLAVE = [("REDIS", "6379"), ("KAFKA", "9092"), ("BROKER", "9092"), ("MINIO", "9000"),
                ("MQTT", "1883"), ("MONGO", "27017"), ("MYSQL", "3306"), ("SMTP", "587"),
                ("MAIL", "587"), ("DATABASE", "5432"), ("POSTGRES", "5432"), ("PG", "5432"),
                ("DB", "5432")]


# ── extracción de dependencias ───────────────────────────────────────────
def normalizar(host, ns):
    h = host.strip().lower().rstrip(".")
    for suf in (".%s.svc.cluster.local" % ns, ".%s.svc" % ns, ".svc.cluster.local"):
        if h.endswith(suf):
            h = h[:-len(suf)]
    return h


def puerto_defecto(clave, esquema):
    if esquema and PUERTO_ESQUEMA.get(esquema.lower()):
        return PUERTO_ESQUEMA[esquema.lower()]
    cu = clave.upper()
    for pref, p in PUERTO_CLAVE:
        if pref in cu:
            return p
    return None


def cadenas(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from cadenas(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from cadenas(v)


def candidatos(clave, valor, servicios, ns, solo_explicitos=False):
    """yield (host, puerto) por cada dependencia hallada en el valor."""
    v = (valor or "").strip()
    if not v:
        return
    if v[0] in "[{":                       # JSON: solo host:puerto o URL explícitos
        try:
            for s in cadenas(json.loads(v)):
                yield from candidatos(clave, s, servicios, ns, True)
        except ValueError:
            pass
        return
    for parte in re.split(r"[,\s;]+", v):
        m = RE_URL.match(parte)
        if m:
            esquema, host, puerto = m.group(1), m.group(2), m.group(3)
        else:
            m = RE_HOST.match(parte)
            if not m:
                continue
            esquema, host, puerto = None, m.group(1), m.group(2)
        host = normalizar(host, ns)
        if host in HOSTS_IGNORAR:
            continue
        es_dep = host in servicios or RE_IP.match(host) or "." in host
        if not es_dep:
            continue
        explicito = bool(puerto or esquema)
        if solo_explicitos and not explicito:
            continue
        # host suelto sin puerto ni esquema: solo si la clave lo justifica
        if not explicito and not RE_CLAVE.search(clave):
            continue
        yield host, puerto or puerto_defecto(clave, esquema)


def extraer(data, servicios, ns):
    """{(host, puerto): clave} a partir de un dict de variables."""
    deps = {}
    for clave, valor in data.items():
        if RE_EXCLUIR.search(clave):
            continue
        if clave.upper().startswith(PREF_NAVEGADOR) and not INCLUIR_NAVEGADOR:
            continue
        for host, puerto in candidatos(clave, valor, servicios, ns):
            if not puerto:                 # ¿existe <PREFIJO>_PORT?
                pref = re.sub(r"_(HOST|HOSTS|ADDR|ADDRESS|URL|URI|ENDPOINT|SERVER)$", "", clave.upper())
                for k2, v2 in data.items():
                    if k2.upper() in (pref + "_PORT", "PORT") and str(v2).isdigit():
                        puerto = str(v2)
            if puerto:
                deps.setdefault((host, puerto), clave)
    return deps


def leer_topologia():
    aristas = []
    fuentes = {"deployment": 0, "statefulset": 0, "configmap": 0, "service": 0}
    for ns in NAMESPACES:
        servicios_raw = api_get("/api/v1/namespaces/%s/services" % ns)
        servicios = {s["metadata"]["name"] for s in servicios_raw}
        selectores = {s["metadata"]["name"]: (s.get("spec", {}).get("selector") or {})
                      for s in servicios_raw}
        fuentes["service"] += len(servicios)
        cms = {c["metadata"]["name"]: c.get("data", {}) or {}
               for c in api_get("/api/v1/namespaces/%s/configmaps" % ns)}
        fuentes["configmap"] += len(cms)
        cargas = [("deployment", w) for w in api_get("/apis/apps/v1/namespaces/%s/deployments" % ns)] + \
                 [("statefulset", w) for w in api_get("/apis/apps/v1/namespaces/%s/statefulsets" % ns)]

        # Service -> workload dueño: el selector del Service coincide con las labels del pod
        dueno = {}
        for tipo, w in cargas:
            etiquetas = w["spec"]["template"].get("metadata", {}).get("labels", {}) or {}
            for svc, sel in selectores.items():
                if sel and all(etiquetas.get(k) == v for k, v in sel.items()):
                    dueno.setdefault(svc, w["metadata"]["name"])

        for tipo, w in cargas:
            fuentes[tipo] += 1
            nombre = w["metadata"]["name"]
            datos = {}
            for c in w["spec"]["template"]["spec"].get("containers", []):
                for e in c.get("envFrom", []):
                    ref = e.get("configMapRef", {}).get("name")
                    if ref:
                        datos.update(cms.get(ref, {}))
                for e in c.get("env", []):
                    if "value" in e:
                        datos[e["name"]] = e["value"]
                    ref = e.get("valueFrom", {}).get("configMapKeyRef")
                    if ref:
                        datos[e["name"]] = cms.get(ref["name"], {}).get(ref["key"], "")
            for (host, puerto), clave in extraer(datos, servicios, ns).items():
                if host.startswith(nombre) or dueno.get(host) == nombre:
                    continue                # no me apunto a mí mismo
                aristas.append({"ns": ns, "src": nombre, "tipo": tipo,
                                "host": host, "puerto": puerto, "clave": clave,
                                "interno": host in servicios, "dueno": dueno.get(host)})
    return aristas, fuentes


# ── sondas ───────────────────────────────────────────────────────────────
def direccion(a):
    """Dirección que resuelve desde el namespace del mapper."""
    return "%s.%s.svc.cluster.local" % (a["host"], a["ns"]) if a["interno"] else a["host"]


def alias(a):
    """Nombre del nodo destino: alias explícito > workload dueño > host."""
    expl = ALIAS.get("%s:%s" % (a["host"], a["puerto"]), ALIAS.get(a["host"]))
    if expl:
        return expl
    return a.get("dueno") or a["host"]


def sondear(addr, puerto):
    t0 = time.monotonic()
    try:
        with socket.create_connection((addr, int(puerto)), timeout=TIMEOUT):
            pass
        return 1, time.monotonic() - t0, ""
    except socket.timeout:
        return 0, TIMEOUT, "timeout"
    except ConnectionRefusedError:
        return 0, time.monotonic() - t0, "refused"
    except socket.gaierror:
        return 0, time.monotonic() - t0, "dns"
    except OSError:
        return 0, time.monotonic() - t0, "error"


# ── ciclo principal ──────────────────────────────────────────────────────
ESTADO = {"aristas": [], "sondas": {}, "fuentes": {}, "ts": 0, "errores": 0, "duracion_ciclo": 0}
LOCK = threading.Lock()


def ciclo():
    t0 = time.monotonic()
    aristas, fuentes = leer_topologia()
    destinos = {}
    for a in aristas:
        destinos[(direccion(a), a["puerto"])] = a
    pendientes = [k for k, a in destinos.items()
                  if "%s:%s" % (a["host"], k[1]) not in NO_SONDEAR and a["host"] not in NO_SONDEAR]
    sondas = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for k, r in zip(pendientes, ex.map(lambda k: sondear(*k), pendientes)):
            sondas[k] = r
    with LOCK:
        ESTADO.update(aristas=aristas, sondas=sondas, fuentes=fuentes,
                      ts=time.time(), duracion_ciclo=time.monotonic() - t0)
    rojos = sum(1 for r in sondas.values() if r[0] == 0)
    log("ciclo ok: %d flechas, %d destinos, %d en rojo, %.1fs" % (len(aristas), len(destinos), rojos,
                                                                  time.monotonic() - t0))


def bucle():
    while True:
        try:
            ciclo()
        except Exception as e:
            with LOCK:
                ESTADO["errores"] += 1
            log("ERROR en ciclo:", repr(e))
        time.sleep(INTERVALO)


# ── exposición ───────────────────────────────────────────────────────────
def esc(s):
    return str(s).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def metricas():
    with LOCK:
        aristas = list(ESTADO["aristas"])
        sondas = dict(ESTADO["sondas"])
        fuentes = dict(ESTADO["fuentes"])
        ts, errores, dur = ESTADO["ts"], ESTADO["errores"], ESTADO["duracion_ciclo"]
    out = [
        "# HELP dependencia Flecha declarada. 1 destino alcanzable, 0 no alcanzable, 2 no sondeado.",
        "# TYPE dependencia gauge",
    ]
    for a in sorted(aristas, key=lambda a: (a["src"], a["host"], a["puerto"])):
        k = (direccion(a), a["puerto"])
        valor = sondas[k][0] if k in sondas else 2
        out.append('dependencia{namespace="%s",src="%s",src_tipo="%s",dst="%s",dst_svc="%s",'
                   'dst_addr="%s",dst_port="%s",clave="%s",externo="%s"} %d' % (
                       esc(a["ns"]), esc(a["src"]), esc(a["tipo"]), esc(alias(a)),
                       esc(a["host"] if a["interno"] else ""), esc(a["host"]), esc(a["puerto"]),
                       esc(a["clave"]), "false" if a["interno"] else "true", valor))
    out.append("# HELP dependencia_duracion_segundos Tiempo de la sonda TCP al destino.")
    out.append("# TYPE dependencia_duracion_segundos gauge")
    out.append("# HELP dependencia_fallo_motivo Motivo del fallo de sonda: timeout, refused, dns, error.")
    out.append("# TYPE dependencia_fallo_motivo gauge")
    vistos = set()
    for a in aristas:
        k = (direccion(a), a["puerto"])
        if k in vistos or k not in sondas:
            continue
        vistos.add(k)
        ok, dur_sonda, motivo = sondas[k]
        dst = esc(alias(a))
        out.append('dependencia_duracion_segundos{dst="%s",dst_port="%s"} %.4f' % (dst, esc(a["puerto"]), dur_sonda))
        if not ok:
            out.append('dependencia_fallo_motivo{dst="%s",dst_port="%s",motivo="%s"} 1' % (dst, esc(a["puerto"]), motivo))
    out.append("# TYPE dependencia_mapper_info gauge")
    out.append('dependencia_mapper_info{version="%s"} 1' % VERSION)
    out.append("# TYPE dependencia_mapper_ultima_lectura_timestamp_seconds gauge")
    out.append("dependencia_mapper_ultima_lectura_timestamp_seconds %.0f" % ts)
    out.append("# TYPE dependencia_mapper_duracion_ciclo_segundos gauge")
    out.append("dependencia_mapper_duracion_ciclo_segundos %.3f" % dur)
    out.append("# TYPE dependencia_mapper_fuentes gauge")
    for tipo, n in sorted(fuentes.items()):
        out.append('dependencia_mapper_fuentes{tipo="%s"} %d' % (tipo, n))
    out.append("# TYPE dependencia_mapper_errores_total counter")
    out.append("dependencia_mapper_errores_total %d" % errores)
    return "\n".join(out) + "\n"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype):
        b = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith("/metrics"):
            self._send(200, metricas(), "text/plain; version=0.0.4")
        elif self.path.startswith("/healthz"):
            fresco = time.time() - ESTADO["ts"] < INTERVALO * 3
            self._send(200 if fresco else 503, "ok\n" if fresco else "stale\n", "text/plain")
        elif self.path.startswith("/flechas"):
            with LOCK:
                cuerpo = json.dumps(ESTADO["aristas"], indent=1, ensure_ascii=False)
            self._send(200, cuerpo, "application/json")
        else:
            self._send(404, "not found\n", "text/plain")


if __name__ == "__main__":
    threading.Thread(target=bucle, daemon=True).start()
    log("dependencias-mapper %s arrancado; namespaces=%s intervalo=%ss api=%s" % (
        VERSION, NAMESPACES, INTERVALO, API))
    ThreadingHTTPServer(("", PUERTO_HTTP), Handler).serve_forever()
