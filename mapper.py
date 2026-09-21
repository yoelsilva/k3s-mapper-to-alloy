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
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "0.5.0"


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
PROTOCOLOS = CFG.get("protocolos", {})
PROTOCOLO_PUERTO = CFG.get("protocolo_por_puerto", {})
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

# Clase del destino para el icono del panel (etiqueta dst_kind). El esquema de
# la URL es la fuente fiable: quien escribió postgresql:// lo sabia.
ESQUEMA_CLASE = {"http": "http", "https": "http", "grpc": "grpc",
                 "postgresql": "postgres", "postgres": "postgres",
                 "redis": "redis", "rediss": "redis",
                 "mqtt": "mqtt", "mqtts": "mqtt",
                 "amqp": "amqp", "amqps": "amqp",
                 "mongodb": "mongo", "mysql": "mysql",
                 "smtp": "smtp", "smtps": "smtp",
                 "kafka": "kafka", "s3": "storage", "minio": "storage"}

# Prefijos de clave que no dejan lugar a duda. Deliberadamente corta: DATABASE,
# DB, PG y BROKER se quedan fuera porque no dicen de qué motor hablan, y una
# clase equivocada es peor que ninguna (ver clase()).
CLASE_CLAVE = [("POSTGRES", "postgres"), ("MYSQL", "mysql"), ("MONGO", "mongo"),
               ("REDIS", "redis"), ("KAFKA", "kafka"), ("MQTT", "mqtt"),
               ("MINIO", "storage"), ("ELASTIC", "search"), ("OPENSEARCH", "search"),
               ("SMTP", "smtp"), ("MAIL", "smtp")]


# ── extracción de dependencias ───────────────────────────────────────────
def normalizar(host, ns):
    """Reduce el host a `servicio` o a `servicio.namespace`.

    Las formas largas del DNS de Kubernetes se recortan: la del propio
    namespace queda en `servicio`, la de otro queda en `servicio.namespace`.
    Quién es quién lo decide después clasificar(), con el índice del clúster.
    """
    h = host.strip().lower().rstrip(".")
    for suf in (".%s.svc.cluster.local" % ns, ".%s.svc" % ns,
                ".svc.cluster.local", ".svc"):
        if h.endswith(suf):
            return h[:-len(suf)]
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
        yield host, puerto or puerto_defecto(clave, esquema), esquema


def extraer(data, servicios, ns):
    """{(host, puerto): (clave, esquema)} a partir de un dict de variables."""
    deps = {}
    for clave, valor in data.items():
        if RE_EXCLUIR.search(clave):
            continue
        if clave.upper().startswith(PREF_NAVEGADOR) and not INCLUIR_NAVEGADOR:
            continue
        for host, puerto, esquema in candidatos(clave, valor, servicios, ns):
            if not puerto:                 # ¿existe <PREFIJO>_PORT?
                pref = re.sub(r"_(HOST|HOSTS|ADDR|ADDRESS|URL|URI|ENDPOINT|SERVER)$", "", clave.upper())
                for k2, v2 in data.items():
                    if k2.upper() in (pref + "_PORT", "PORT") and str(v2).isdigit():
                        puerto = str(v2)
            if puerto:
                deps.setdefault((host, puerto), (clave, esquema))
    return deps


def leer(path, ns, recurso, errores):
    """api_get que no tumba el ciclo: anota el fallo y devuelve None.

    Antes, un 403 en un solo namespace abortaba el ciclo entero y dejaba el
    mapa vacío, incluidos los namespaces que sí se podían leer.
    """
    try:
        return api_get(path)
    except urllib.error.HTTPError as e:
        errores.append((ns, recurso, str(e.code)))
    except Exception as e:                            # red, DNS, JSON roto
        errores.append((ns, recurso, type(e).__name__))
    return None


def indice_servicios(errores):
    """{servicio: {namespace, ...}} de TODOS los Services del clúster.

    Es lo que permite saber que `emqx-svc.brokers` es interno aunque `brokers`
    no esté en la lista de namespaces. Necesita el ClusterRole de solo lectura
    sobre Services (deploy/00-rbac.yaml); sin él se cae a los namespaces
    escaneados, y entonces `externo` vuelve a depender de que la lista esté al
    día, que es justo la fragilidad que este índice viene a quitar.
    """
    indice = {}
    todos = leer("/api/v1/services", "*", "services", errores)
    if todos is None:
        return indice, False
    for s in todos:
        indice.setdefault(s["metadata"]["name"], set()).add(s["metadata"]["namespace"])
    return indice, True


def clasificar(host, ns, indice):
    """(interno, namespace destino, servicio) del destino.

    `interno` significa "está dentro del clúster", no "está en mi namespace":
    el namespace es una división administrativa, no una frontera de confianza.
    Un Service de otro namespace es tan interno como el de al lado.
    """
    if ns in indice.get(host, ()):
        return True, ns, host
    if "." in host:
        svc, _, resto = host.partition(".")
        if resto in indice.get(svc, ()):              # forma servicio.namespace
            return True, resto, svc
    return False, None, None


def leer_topologia():
    aristas = []
    fuentes = {"deployment": 0, "statefulset": 0, "configmap": 0, "service": 0}
    errores = []
    indice, global_ok = indice_servicios(errores)

    dueno = {}          # (namespace, servicio) -> workload dueño
    pendientes = []     # (ns, servicios del ns, cms, tipo, workload)

    for ns in NAMESPACES:
        servicios_raw = leer("/api/v1/namespaces/%s/services" % ns, ns, "services", errores)
        if servicios_raw is None:
            continue                                  # este namespace se cae solo
        servicios = {s["metadata"]["name"] for s in servicios_raw}
        selectores = {s["metadata"]["name"]: (s.get("spec", {}).get("selector") or {})
                      for s in servicios_raw}
        fuentes["service"] += len(servicios)
        if not global_ok:                             # sin ClusterRole, al menos lo escaneado
            for nombre_svc in servicios:
                indice.setdefault(nombre_svc, set()).add(ns)

        cms_raw = leer("/api/v1/namespaces/%s/configmaps" % ns, ns, "configmaps", errores) or []
        cms = {c["metadata"]["name"]: c.get("data", {}) or {} for c in cms_raw}
        fuentes["configmap"] += len(cms)

        cargas = []
        for tipo, ruta in (("deployment", "/apis/apps/v1/namespaces/%s/deployments"),
                           ("statefulset", "/apis/apps/v1/namespaces/%s/statefulsets")):
            for w in (leer(ruta % ns, ns, tipo + "s", errores) or []):
                cargas.append((tipo, w))

        # Service -> workload dueño: el selector del Service coincide con las labels del pod
        for tipo, w in cargas:
            etiquetas = w["spec"]["template"].get("metadata", {}).get("labels", {}) or {}
            for svc, sel in selectores.items():
                if sel and all(etiquetas.get(k) == v for k, v in sel.items()):
                    dueno.setdefault((ns, svc), w["metadata"]["name"])

        for tipo, w in cargas:
            fuentes[tipo] += 1
            pendientes.append((ns, servicios, cms, tipo, w))

    # Segunda pasada: los dueños de todos los namespaces ya están resueltos, así
    # que una flecha a otro namespace puede nombrar su workload destino.
    for ns, servicios, cms, tipo, w in pendientes:
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
        for (host, puerto), (clave, esquema) in extraer(datos, servicios, ns).items():
            interno, ns_dst, svc = clasificar(host, ns, indice)
            dueno_dst = dueno.get((ns_dst, svc)) if interno else None
            if host.startswith(nombre) or (dueno_dst == nombre and ns_dst == ns):
                continue                              # no me apunto a mí mismo
            aristas.append({"ns": ns, "src": nombre, "tipo": tipo,
                            "host": host, "puerto": puerto, "clave": clave, "esquema": esquema,
                            "interno": interno, "ns_dst": ns_dst, "svc": svc,
                            "dueno": dueno_dst})
    return aristas, fuentes, errores


# ── sondas ───────────────────────────────────────────────────────────────
def direccion(a):
    """Dirección que resuelve desde el namespace del mapper.

    Para lo interno se usa siempre la forma larga con su propio namespace, que
    resuelve desde cualquier sitio del clúster.
    """
    if a["interno"]:
        return "%s.%s.svc.cluster.local" % (a["svc"], a["ns_dst"])
    return a["host"]


def alias(a):
    """Nombre del nodo destino: alias explícito > dueño > Service > host.

    El Service entra en la cadena porque un destino interno de un namespace que
    no escaneamos se conoce por su nombre de Service, pero no su dueño.
    """
    expl = ALIAS.get("%s:%s" % (a["host"], a["puerto"]), ALIAS.get(a["host"]))
    if expl:
        return expl
    return a.get("dueno") or a.get("svc") or a["host"]


def clase(a):
    """Clase del destino, para el icono del panel: etiqueta `dst_kind`.

    Solo se emite cuando se sabe de verdad. Si no, cadena vacía, NUNCA "other":
    el panel da prioridad a lo que diga el mapper, así que un "other" nuestro
    apagaría su deducción por puerto, que acierta más que una suposición.
    Prometheus además descarta las etiquetas vacías, asi que no ensucia nada.
    """
    esq = (a.get("esquema") or "").lower()
    if esq in ESQUEMA_CLASE:
        return ESQUEMA_CLASE[esq]
    cu = a["clave"].upper()
    for pref, c in CLASE_CLAVE:
        if pref in cu:
            return c
    return ""


def nodo_id(nombre):
    """Id seguro y estable del nodo: "n_" + todo lo que no sea [A-Za-z0-9_] a "_".

    Se deriva del nombre, así que es estable entre ciclos sin guardar estado.
    Dos nombres que solo difieran en signos de puntuación colisionan en un único
    nodo del grafo; es aceptable porque el nombre ya viene deduplicado por alias.
    """
    return "n_" + re.sub(r"[^A-Za-z0-9_]", "_", str(nombre))


# ── sondas de protocolo ──────────────────────────────────
# Hay tres niveles, y solo los dos primeros le tocan al mapper:
#
#   1 · red        ¿el puerto acepta conexiones?     responde el kernel
#   2 · protocolo  ¿hay un programa que hable?       responde el programa
#   3 · aplicación ¿ese programa está sano?          readinessProbe del servicio
#
# El nivel 1 miente. Cuando un programa abre un puerto, es el kernel quien
# completa el saludo TCP y encola la conexión; si el programa está colgado, el
# kernel sigue aceptando y desde fuera el puerto responde igual de bien. Por eso
# estas sondas dicen algo en el idioma del servicio y esperan contestación.
#
# Ninguna se autentica ni ejecuta nada: son el saludo más barato de cada
# protocolo. MQTT se queda a propósito en "tcp": un CONNECT sin credenciales
# cada ciclo aparecería en el log del broker como intento rechazado.

PREFACIO_H2 = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
SETTINGS_H2 = b"\x00\x00\x00\x04\x00\x00\x00\x00\x00"      # SETTINGS vacío


def sonda_grpc(s, host):
    """Saludo HTTP/2. Un gRPC vivo responde con un frame; uno colgado calla."""
    s.sendall(PREFACIO_H2 + SETTINGS_H2)
    r = s.recv(9)
    return len(r) >= 9 and r[3] <= 0x09          # cabecera de frame: tipo válido


def sonda_http(s, host):
    s.sendall(b"HEAD / HTTP/1.1\r\nHost: " + host.encode() +
              b"\r\nConnection: close\r\nUser-Agent: dependencias-mapper\r\n\r\n")
    return s.recv(16).startswith(b"HTTP/")


def sonda_redis(s, host):
    s.sendall(b"PING\r\n")
    r = s.recv(64)
    return r.startswith((b"+PONG", b"-NOAUTH", b"-ERR"))   # vivo aunque pida clave


def sonda_postgres(s, host):
    """Negociación SSL, no un login: un login falso llenaría el log de la base."""
    s.sendall(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")        # SSLRequest, código 80877103
    return s.recv(1) in (b"S", b"N")


def sonda_kafka(s, host):
    cid = b"dependencias-mapper"
    corr = 0x4D415045
    cuerpo = struct.pack(">hhi", 18, 0, corr) + struct.pack(">h", len(cid)) + cid
    s.sendall(struct.pack(">i", len(cuerpo)) + cuerpo)
    r = s.recv(8)
    return len(r) >= 8 and struct.unpack(">i", r[4:8])[0] == corr


SONDAS = {"grpc": sonda_grpc, "http": sonda_http, "redis": sonda_redis,
          "postgres": sonda_postgres, "kafka": sonda_kafka}

# La clase que ya deducimos para el icono sirve también para elegir sonda.
CLASE_SONDA = {"postgres": "postgres", "redis": "redis", "kafka": "kafka",
               "http": "http", "grpc": "grpc"}

# Último recurso, solo si no sabemos la clase. 1883/8883 explícitos para que no
# se los coma el rango 8xxx de HTTP.
PUERTO_SONDA = {"6379": "redis", "5432": "postgres", "9092": "kafka", "9093": "kafka",
                "80": "http", "443": "http", "3000": "http", "3100": "http",
                "5000": "http", "1883": "tcp", "8883": "tcp"}

# Verificar el certificado convertiría uno caducado en un falso rojo, y aquí
# solo preguntamos si hay alguien vivo al otro lado.
CTX_SONDA = ssl.create_default_context()
CTX_SONDA.check_hostname = False
CTX_SONDA.verify_mode = ssl.CERT_NONE


def protocolo(a):
    """Qué sonda usar con este destino: configuración > clase > puerto."""
    for k in ("%s:%s" % (a["host"], a["puerto"]), a["host"]):
        if k in PROTOCOLOS:
            return PROTOCOLOS[k]
    if a["puerto"] in PROTOCOLO_PUERTO:
        return PROTOCOLO_PUERTO[a["puerto"]]
    c = clase(a)
    if c:                                # sabemos qué es: o hay sonda, o TCP
        return CLASE_SONDA.get(c, "tcp")
    if a["puerto"] in PUERTO_SONDA:
        return PUERTO_SONDA[a["puerto"]]
    n = int(a["puerto"]) if str(a["puerto"]).isdigit() else 0
    if 50051 <= n <= 50099:
        return "grpc"
    if 8000 <= n <= 8999:
        return "http"
    return "tcp"


def sondear(addr, puerto, proto="tcp", host=None):
    """Devuelve (valor, duración, motivo, protocolo usado).

    "sin_respuesta" es el caso que el nivel de red no ve: la conexión se
    estableció, pero el programa no contestó a su propio protocolo.
    """
    t0 = time.monotonic()
    s = None
    try:
        s = socket.create_connection((addr, int(puerto)), timeout=TIMEOUT)
        if proto == "tcp":
            return 1, time.monotonic() - t0, "", proto
        s.settimeout(TIMEOUT)
        if proto == "http" and str(puerto) in ("443", "8443"):
            s = CTX_SONDA.wrap_socket(s, server_hostname=host or addr)
        if SONDAS[proto](s, host or addr):
            return 1, time.monotonic() - t0, "", proto
        return 0, time.monotonic() - t0, "sin_respuesta", proto
    except socket.timeout:
        # si ya había conexión, el que calla es el programa, no la red
        return 0, time.monotonic() - t0, "sin_respuesta" if s else "timeout", proto
    except ConnectionRefusedError:
        return 0, time.monotonic() - t0, "refused", proto
    except ConnectionResetError:
        return 0, time.monotonic() - t0, "sin_respuesta" if s else "error", proto
    except socket.gaierror:
        return 0, time.monotonic() - t0, "dns", proto
    except ssl.SSLError:
        return 0, time.monotonic() - t0, "sin_respuesta", proto
    except OSError:
        return 0, time.monotonic() - t0, "error", proto
    finally:
        if s is not None:
            try:
                s.close()
            except OSError:
                pass


# ── ciclo principal ──────────────────────────────────────────────────────
ESTADO = {"aristas": [], "sondas": {}, "fuentes": {}, "ts": 0, "errores": 0,
          "duracion_ciclo": 0, "errores_ns": []}
LOCK = threading.Lock()


def ciclo():
    t0 = time.monotonic()
    aristas, fuentes, errores_ns = leer_topologia()
    destinos = {}
    for a in aristas:
        destinos[(direccion(a), a["puerto"])] = a
    pendientes = [k for k, a in destinos.items()
                  if "%s:%s" % (a["host"], k[1]) not in NO_SONDEAR and a["host"] not in NO_SONDEAR]
    sondas = {}
    argumentos = [(k[0], k[1], protocolo(destinos[k]), destinos[k]["host"]) for k in pendientes]
    with ThreadPoolExecutor(max_workers=16) as ex:
        for k, r in zip(pendientes, ex.map(lambda x: sondear(*x), argumentos)):
            sondas[k] = r
    with LOCK:
        ESTADO.update(aristas=aristas, sondas=sondas, fuentes=fuentes, errores_ns=errores_ns,
                      ts=time.time(), duracion_ciclo=time.monotonic() - t0)
    rojos = sum(1 for r in sondas.values() if r[0] == 0)
    log("ciclo ok: %d flechas, %d destinos, %d en rojo, %.1fs" % (len(aristas), len(destinos), rojos,
                                                                  time.monotonic() - t0))
    for ns, recurso, motivo in errores_ns:
        log("  sin acceso: namespace=%s recurso=%s motivo=%s" % (ns, recurso, motivo))


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
        errores_ns = list(ESTADO["errores_ns"])
        ts, errores, dur = ESTADO["ts"], ESTADO["errores"], ESTADO["duracion_ciclo"]
    out = [
        "# HELP dependencia Flecha declarada. 1 destino alcanzable, 0 no alcanzable, 2 no sondeado.",
        "# TYPE dependencia gauge",
    ]
    for a in sorted(aristas, key=lambda a: (a["src"], a["host"], a["puerto"])):
        k = (direccion(a), a["puerto"])
        valor = sondas[k][0] if k in sondas else 2
        usada = sondas[k][3] if k in sondas else ""
        dst = alias(a)
        out.append('dependencia{namespace="%s",src="%s",src_id="%s",src_tipo="%s",'
                   'dst="%s",dst_id="%s",dst_svc="%s",dst_ns="%s",dst_addr="%s",dst_port="%s",'
                   'dst_kind="%s",clave="%s",externo="%s",sonda="%s"} %d' % (
                       esc(a["ns"]), esc(a["src"]), nodo_id(a["src"]), esc(a["tipo"]),
                       esc(dst), nodo_id(dst),
                       esc(a.get("svc") or ""), esc(a.get("ns_dst") or ""),
                       esc(a["host"]), esc(a["puerto"]),
                       esc(clase(a)), esc(a["clave"]), "false" if a["interno"] else "true", esc(usada), valor))
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
        ok, dur_sonda, motivo, proto_usado = sondas[k]
        dst = esc(alias(a))
        out.append('dependencia_duracion_segundos{dst="%s",dst_port="%s"} %.4f' % (dst, esc(a["puerto"]), dur_sonda))
        if not ok:
            out.append('dependencia_fallo_motivo{dst="%s",dst_port="%s",motivo="%s",sonda="%s"} 1' % (
                dst, esc(a["puerto"]), motivo, proto_usado))
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
    out.append("# HELP dependencia_mapper_namespace_error Recurso que el mapper no pudo leer. "
               "motivo: codigo HTTP (403 = falta el RoleBinding) o tipo de excepcion.")
    out.append("# TYPE dependencia_mapper_namespace_error gauge")
    for ns, recurso, motivo in errores_ns:
        out.append('dependencia_mapper_namespace_error{namespace="%s",recurso="%s",motivo="%s"} 1'
                   % (esc(ns), esc(recurso), esc(motivo)))
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
