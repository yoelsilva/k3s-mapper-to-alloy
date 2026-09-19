# Elementos del mapper en el clúster

Inventario de todo lo que hay que crear para que el mapper corra, qué hace cada
pieza y qué hay que adaptar. Los tres ficheros de esta carpeta crean **seis
objetos** en Kubernetes; nada más. No hay Helm chart, ni operador, ni base de datos.

## Antes de empezar

| Requisito | Por qué |
|---|---|
| Namespace `monitoring` **ya existente** | Ningún manifiesto lo crea. Normalmente lo trae el kube-prometheus-stack; si no, `kubectl create namespace monitoring` |
| Los namespaces a mapear ya existentes | El mapper solo lee, no crea nada en ellos |
| Alloy corriendo en el clúster | Es quien recoge las métricas y las manda al Prometheus central |
| Salida TCP desde `monitoring` | La sonda sale del pod del mapper; ver *Red* abajo |

La imagen es pública, así que **no hace falta `imagePullSecret`**.

## Los seis objetos

### 1. ServiceAccount `dependencias-mapper` · ns `monitoring`
La identidad del pod. Es a quien se le conceden los permisos de lectura. Sin
esto el pod usaría la `default` del namespace, que no puede leer nada.

### 2. ClusterRole `dependencias-mapper-lectura`
Define **qué** se puede leer, y solo eso:

```
apps/  deployments, statefulsets   get, list
""/    configmaps, services        get, list
```

No incluye Secrets, y es deliberado: si una dependencia vive en un Secret, la
solución correcta es sacar el host a una variable aparte, no dar acceso a Secrets.
Ser ClusterRole no concede nada por sí mismo — es solo la definición.

### 3. RoleBinding `dependencias-mapper-lectura` · **uno por namespace mapeado**
Lo que de verdad concede el permiso, y solo dentro de su namespace. Es la pieza
que hay que duplicar: un RoleBinding por cada namespace que quieras ver en el mapa.
Así el mapper nunca ve más de lo que le diste explícitamente.

```yaml
metadata:
  namespace: tecopos        # <- cambia esto y duplica el bloque
subjects:
  - kind: ServiceAccount
    name: dependencias-mapper
    namespace: monitoring   # <- la SA siempre vive en monitoring
```

Cada namespace que añadas aquí tiene que estar también en `namespaces` del
ConfigMap. Si falta el RoleBinding, el ciclo falla con 403; si falta en el
ConfigMap, simplemente no se lee.

### 4. ConfigMap `dependencias-mapper-config` · ns `monitoring`
La única pieza que cambia entre clústeres. Una clave, `config.json`:

| Campo | Para qué |
|---|---|
| `namespaces` | Qué namespaces leer. Cada uno necesita su RoleBinding |
| `alias` | Nombre legible para IPs y hosts externos. `"10.0.0.10:5432"` → `postgres-principal`. Con puerto tiene prioridad sobre sin puerto |
| `no_sondear` | Destinos que se dibujan pero no se tocan. Salen con valor `2` |
| `hosts_ignorar` | Direcciones de escucha (`0.0.0.0`, `localhost`), no dependencias |
| `claves_regex` | Qué claves admiten un host suelto sin puerto (`*_HOST`, `*_URL`…) |
| `claves_excluir_regex` | Qué claves descartar siempre (CORS, orígenes, URLs públicas de sí mismo) |
| `prefijos_navegador` | `VITE_`, `REACT_APP_`…: describen a quién llama el navegador, no el pod |
| `intervalo_segundos` | Cada cuánto se rehace el mapa. 60 por defecto |
| `timeout_sonda_segundos` | Cuánto espera cada sonda TCP. 3 por defecto |

Los alias del repo son **de ejemplo** (`10.0.0.x`). El mapa real de tu red va aquí
y solo aquí — no en el repo, que es público.

Tras editarlo:

```bash
kubectl apply -f deploy/10-config.yaml
kubectl -n monitoring rollout restart deploy/dependencias-mapper
```

### 5. Deployment `dependencias-mapper` · ns `monitoring`
Una réplica. No tiene sentido escalarlo: cada réplica haría el mismo trabajo y
duplicaría las sondas.

| | |
|---|---|
| Imagen | `ghcr.io/yoelsilva/k3s-mapper-to-alloy:0.2.0` — amd64 y arm64 |
| Recursos | pide 20m CPU / 48Mi, tope 200m / 128Mi |
| Seguridad | usuario 65534, `runAsNonRoot`, raíz de solo lectura, todas las capabilities soltadas, `seccompProfile: RuntimeDefault` |
| Config | el ConfigMap montado en `/config`, de solo lectura |
| Sondas | `readinessProbe` y `livenessProbe` contra `/healthz`, que devuelve 503 si el último ciclo tiene más de 3 intervalos |

Fija **siempre una versión concreta**. Nunca `:latest` en producción: el pod se
reiniciaría con otra versión sin que nadie lo haya decidido.

### 6. Service `dependencias-mapper` · ns `monitoring`
Puerto 9400, nombre `metrics`. Opcional en sentido estricto — Alloy descubre el
pod por etiqueta, no por Service — pero sin él no hay `port-forward` cómodo ni
nombre estable al que apuntar.

## Fuera de estos ficheros: Alloy

El pod expone métricas, pero alguien tiene que recogerlas. Eso lo hace Alloy, y su
configuración **no está en estos manifiestos**: el snippet de
[`alloy-snippet.river`](alloy-snippet.river) va pegado dentro de
`alloy.configMap.content` en el values del chart `grafana/alloy`, y luego
`helm upgrade`.

Hace tres cosas: descubre el pod por la etiqueta
`app.kubernetes.io/name=dependencias-mapper` en `monitoring`, lo scrapea cada 30s,
y filtra para mandar al Prometheus central solo las métricas del mapper.

La etiqueta `cluster` no la pone el mapper: la pone el `external_labels` de cada
Alloy. Por eso cada clúster llega al Prometheus central ya identificado, y por eso
el mismo mapper sirve para todos sin configuración distinta.

## Red

La sonda TCP **sale desde el namespace del mapper**, no desde el pod origen. Esto
importa si tienes NetworkPolicies: que `tecopos → brokers` esté permitido no
implica que `monitoring → brokers` lo esté, y entonces la flecha sale roja aunque
el servicio real llegue perfectamente.

Dos salidas: permitir el namespace del mapper en la política, o meter ese destino
en `no_sondear` para que se dibuje sin tocarlo.

El mapper también necesita llegar a la API de Kubernetes, que en k3s es lo normal
dentro del clúster y no requiere nada especial.

## Orden de aplicación y comprobación

```bash
kubectl apply -f deploy/00-rbac.yaml     # identidad y permisos
kubectl apply -f deploy/10-config.yaml   # configuración
kubectl apply -f deploy/20-deployment.yaml

kubectl -n monitoring rollout status deploy/dependencias-mapper
kubectl -n monitoring logs deploy/dependencias-mapper --tail=3
#  → ciclo ok: 82 flechas, 41 destinos, 4 en rojo, 1.2s
```

Si el log dice `ciclo ok` con un número razonable de flechas, está funcionando.
Si dice `ERROR en ciclo: HTTPError 403`, falta un RoleBinding.

Para revisar qué dedujo, antes de mirar Grafana:

```bash
kubectl -n monitoring port-forward svc/dependencias-mapper 9400:9400 &
curl -s localhost:9400/flechas | python3 -c "
import sys,json
for a in sorted(json.load(sys.stdin), key=lambda a:(a['src'],a['host'])):
    print(f\"{a['src']:32} → {a['host']}:{a['puerto']:<6} ({a['clave']})\")"
```

Tres endpoints en el puerto 9400: `/metrics` (Prometheus), `/healthz` (las sondas
del kubelet) y `/flechas` (JSON con lo deducido, para depurar el parseo).

## Añadir otro clúster

Los mismos tres `kubectl apply` con su propia `10-config.yaml` —sus namespaces y
sus alias— y su Alloy con `cluster = "<nombre>"`. La imagen es la misma. En
Grafana, el selector `$cluster` lo recoge solo.
