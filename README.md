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
| URL con esquema | `postgresql://u:p@10.0.0.10:5432/db` | siempre |
| `host:puerto` | `controlserver-svc-grpc:50053` | siempre |
| host suelto | `redis-hermes-server-svc` | solo si la clave casa con `claves_regex` (`*_HOST`, `*_URL`, …) |
| JSON | `[{"grpcUrl":"10.0.0.12:50056"}]` | solo los `host:puerto` o URLs explícitos que contenga |

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
dependencia{namespace,src,src_id,src_tipo,dst,dst_id,dst_svc,dst_addr,dst_port,clave,externo}
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

`src_id` y `dst_id` son el mismo nombre pasado por `[^A-Za-z0-9_] -> _` con prefijo
`n_`: identificadores seguros para usar como id de nodo en un grafo, estables entre
ciclos porque se derivan del nombre y no de ningún estado guardado. Existen desde
0.2.0; el panel `tecopos-mapa-panel` los exige.

## Desplegar en un clúster

Inventario detallado de cada objeto, qué hace y qué hay que adaptar:
[`deploy/README.md`](deploy/README.md).

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

Dashboard listo para importar: [`dashboards/mapa-dependencias-nodegraph.json`](dashboards/mapa-dependencias-nodegraph.json).
*Dashboards → New → Import → Upload JSON file*, y elige tu datasource de Prometheus.

Usa el **Node graph** nativo. Es provisional: el panel propio
(`tecopos-mapa-panel`) lo sustituirá con layout por capas, que es su razón de ser.

Trae variables de `cluster` y de servicio origen, ambas con *All*, y colorea las
flechas por estado: rojo `#F2495C` si la sonda falla, gris `#8E8E9E` si responde,
azul `#5794F2` si está en `no_sondear`.

### Por qué las etiquetas se construyen en PromQL

El Node graph identifica el frame de aristas por tener un campo llamado
`source`. La forma "natural" —sacar los campos tal cual y renombrarlos con la
transformación *Organize fields*— **no funciona**: probado contra Grafana 13.1.1,
el panel ignora el frame y dibuja los nodos sueltos en rejilla, sin una sola flecha.

Por eso las consultas crean los nombres que el panel espera como etiquetas, con
`label_replace`, y el panel no lleva ninguna transformación:

```promql
label_replace(<expr>, "source", "$1", "src_id", "(.*)")
label_replace(<expr>, "target", "$1", "dst_id", "(.*)")
label_replace(<expr>, "mainstat", "$1", "dst_port", "(.*)")
```

Y aquí es donde se paga lo de `src_id`/`dst_id`: como id de nodo hace falta algo
seguro, y los nombres legibles llevan espacios y paréntesis
(`controlserver (NodePort)`).

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

El workflow construye para `amd64` y `arm64` y publica en el registro del propio
repo, `ghcr.io`. No hace falta ningún secret: Actions inyecta `GITHUB_TOKEN` y el
workflow pide `packages: write`.

```bash
# actualizar VERSION en mapper.py, y luego:
git tag v0.2.0 && git push --tags
#  → ghcr.io/yoelsilva/k3s-mapper-to-alloy:0.2.0, :0.2 y :latest
```

Los push a `main` publican `:main` y `:sha-xxxxxxx` para probar sin etiquetar.

La primera publicación crea el paquete en GitHub. Comprueba su visibilidad en
`https://github.com/users/yoelsilva/packages` → el paquete → *Package settings*:
si queda **privado**, el clúster necesita un `imagePullSecret` para bajarlo; si lo
pones **público**, k3s lo descarga sin credenciales.

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
