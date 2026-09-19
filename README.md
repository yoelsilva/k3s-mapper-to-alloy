# dependencias-mapper

Mapa de dependencias declaradas de un clúster Kubernetes, expuesto como métricas
Prometheus. Un pod por clúster; sin sidecars, sin tocar los manifiestos de los
servicios, sin dependencias externas (solo librería estándar de Python).

**Qué responde:** *¿qué servicio está configurado para hablar con qué otro, por
qué puerto, y esa puerta responde ahora mismo?*

**Qué no responde:** cuánto tráfico pasa por cada flecha ni si las peticiones
reales fallan. Eso solo lo da observar el tráfico (Beyla, una malla de servicios).
Este mapper dibuja lo *declarado*, y por eso una dependencia sin uso sigue gris
en lugar de desaparecer.

## Cómo funciona

Cada `intervalo_segundos`:

1. Lee de la API de Kubernetes los Deployments y StatefulSets de los namespaces
   configurados, y los ConfigMaps y Services de esos namespaces.
2. Reconstruye las variables de entorno efectivas de cada workload
   (`env`, `envFrom.configMapRef`, `env.valueFrom.configMapKeyRef`). **No lee Secrets.**
3. Saca las dependencias de esas variables (ver *Reglas de detección*).
4. Traduce cada Service interno a su workload dueño comparando el `selector` del
   Service con las labels del pod. Así `gateway → controlserver-svc-grpc` y
   `controlserver → redis` comparten el nodo `controlserver`.
5. Abre una conexión TCP a cada `destino:puerto` distinto (deduplicado) y cierra.
6. Expone todo en `/metrics`.

Nunca imprime ni exporta valores de variables: solo host, puerto y nombre de la clave.

### Reglas de detección

Un valor cuenta como dependencia si, tras trocearlo por comas/espacios:

| Forma del valor | Ejemplo | Aceptado |
|---|---|---|
| URL con esquema | `postgresql://u:p@172.16.0.9:5432/db` | siempre |
| `host:puerto` | `controlserver-svc-grpc:50053` | siempre |
| host suelto | `redis-hermes-server-svc` | solo si la clave casa con `claves_regex` (`*_HOST`, `*_URL`, …) |
| JSON | `[{"grpcUrl":"172.16.0.13:50056"}]` | solo los `host:puerto` o URLs explícitos que contenga |

Y el host tiene que ser un Service del namespace, una IP, o un nombre con punto.
`DB_DIALECT=postgres` no es una dependencia porque `postgres` no es nada de eso.

Se descartan:

- claves que casan con `claves_excluir_regex` (CORS, orígenes permitidos, URLs
  públicas de sí mismo…);
- claves con `prefijos_navegador` (`VITE_*`, `REACT_APP_*`…): describen a quién
  llama el **navegador**, no el pod. Activables con `incluir_navegador: true`;
- hosts en `hosts_ignorar` (`0.0.0.0`, `localhost`…): son direcciones de escucha.

Si falta el puerto, se busca `<PREFIJO>_PORT` en las mismas variables; si no,
se deduce del esquema (`https` → 443) o de la clave (`REDIS_*` → 6379, `KAFKA_*` → 9092…).

### Métricas

```
dependencia{namespace,src,src_tipo,dst,dst_svc,dst_addr,dst_port,clave,externo}
    1 = destino alcanzable · 0 = no alcanzable · 2 = en no_sondear
dependencia_duracion_segundos{dst,dst_port}
dependencia_fallo_motivo{dst,dst_port,motivo}      motivo: timeout | refused | dns | error
dependencia_mapper_info{version}
dependencia_mapper_ultima_lectura_timestamp_seconds
dependencia_mapper_duracion_ciclo_segundos
dependencia_mapper_fuentes{tipo}                    deployment | statefulset | configmap | service
dependencia_mapper_errores_total
```

`dst` es el nombre del nodo destino: alias explícito > workload dueño > host.
`dst_svc` conserva el nombre del Service; `dst_addr` el host tal cual estaba
escrito.

## Desplegar en un clúster

```bash
# 1. permisos (edita el namespace del RoleBinding; uno por namespace a mapear)
kubectl apply -f deploy/00-rbac.yaml
# 2. configuración (namespaces, alias de IPs, exclusiones)
kubectl apply -f deploy/10-config.yaml
# 3. el pod (fija la imagen y versión)
kubectl apply -f deploy/20-deployment.yaml

kubectl -n monitoring rollout status deploy/dependencias-mapper
kubectl -n monitoring logs deploy/dependencias-mapper --tail=3
#  → ciclo ok: 82 flechas, 41 destinos, 4 en rojo, 1.2s
```

Validar el parseo antes de conectarlo a Prometheus:

```bash
kubectl -n monitoring port-forward svc/dependencias-mapper 9400:9400 &
curl -s localhost:9400/flechas | python3 -c "
import sys,json
for a in sorted(json.load(sys.stdin), key=lambda a:(a['src'],a['host'])):
    print(f\"{a['src']:32} → {a['host']}:{a['puerto']:<6} ({a['clave']})\")"
```

Conectarlo a Alloy: pegar `deploy/alloy-snippet.river` en el values del chart
`grafana/alloy` (dentro de `alloy.configMap.content`) y `helm upgrade`.
La etiqueta `cluster` la pone el `external_labels` de cada Alloy, así que cada
clúster llega al Prometheus central ya identificado.

## Añadir otro clúster al mapa

En el clúster nuevo, los mismos tres `kubectl apply` con su propia
`10-config.yaml` (namespaces y alias suyos) y su Alloy con `cluster = "<nombre>"`.
En Grafana, el selector `$cluster` lo recoge solo.

## Grafana

Panel **Node graph**, datasource Prometheus, dos consultas con *Format = Table*
y *Type = Instant*.

**A · nodos**

```promql
count by (id) (
    label_replace(dependencia{cluster="$cluster"}, "id", "$1", "src", "(.*)")
  or
    label_replace(dependencia{cluster="$cluster"}, "id", "$1", "dst", "(.*)")
)
```

**B · flechas**

```promql
label_join(
    label_replace(dependencia{cluster="$cluster"} == 0, "color", "#F2495C", "src", ".*")
  or
    label_replace(dependencia{cluster="$cluster"} == 1, "color", "#8E8E9E", "src", ".*")
  or
    label_replace(dependencia{cluster="$cluster"} == 2, "color", "#5794F2", "src", ".*"),
  "id", "→", "src", "dst", "dst_port"
)
```

Transformación *Organize fields* sobre el frame B: `src → source`, `dst → target`,
`dst_port → mainstat`, `clave → detail__clave`, `dst_svc → detail__service`;
ocultar el resto. Sobre el frame A: `Value → mainstat`, ocultar `Time`.

Alerta básica:

```promql
dependencia == 0
```

## Desarrollo local

Sin construir imagen, contra un clúster real y sin credenciales en el código:

```bash
kubectl proxy &                       # expone la API en :8001 con tu kubeconfig
CONFIG=./config.example.json python3 mapper.py
curl -s localhost:9400/flechas | head
```

Prueba del parseo sin clúster:

```bash
python3 -c "
import importlib.util
s=importlib.util.spec_from_file_location('m','mapper.py'); m=importlib.util.module_from_spec(s); s.loader.exec_module(m)
print(m.extraer({'REDIS_HOST':'redis-x-svc','KAFKA_BROKERS':'10.0.0.1:9092','CORS_ORIGIN':'https://a.com'}, {'redis-x-svc'}, 'ns'))"
```

## Publicar una versión

El workflow construye para `amd64` y `arm64` y publica en Docker Hub. Requiere
dos secrets en el repo: `DOCKERHUB_USERNAME` y `DOCKERHUB_TOKEN` (token de acceso,
no la contraseña).

```bash
# actualizar VERSION en mapper.py, y luego:
git tag v0.1.0 && git push --tags
#  → threeface/dependencias-mapper:0.1.0, :0.1 y :latest
```

Los push a `main` publican `:main` y `:sha-xxxxxxx` para probar sin etiquetar.

## Limitaciones conocidas

- La sonda sale desde el namespace del mapper, no desde el pod origen. Si una
  NetworkPolicy permite `tecopos → brokers` pero no `monitoring → brokers`, la
  flecha sale roja aunque el servicio real llegue. Solución: permitir el
  namespace del mapper en la política, o meter el destino en `no_sondear`.
- Solo TCP: "el puerto abre". Un servicio HTTP que responde 500 sale gris.
- Solo lee ConfigMaps. Si una dependencia vive en un Secret (p. ej. una
  `DATABASE_URL` completa), no se ve. Es deliberado: mover el host a una variable
  aparte del Secret es la solución correcta, no que el mapper lea Secrets.
- Services por pod de StatefulSet (`x-0`, `x-1`) no encuentran dueño por selector
  y se muestran con su propio nombre.
