#!/usr/bin/env bash
# deploy-petclinic-k8s.sh
#
# Deploys PetClinic + OTel into a k3d cluster, with otel-beacon as the backend.
#
# Prerequisites:
#   - k3d cluster already running
#   - kubectl context pointing at the cluster
#   - OTEL_BACKEND_ENDPOINT set to the OTLP endpoint reachable from pods
#     e.g. http://host.k3d.internal:4318  (otel-beacon running on host)
#
# Usage:
#   ./deploy/deploy-petclinic-k8s.sh [namespace]
#
# Key env vars:
#   OTEL_BACKEND_ENDPOINT  OTLP HTTP endpoint for exporters
#                          (default: http://host.k3d.internal:4318)
#   SKIP_COLLECTOR         Skip OTel Collector DaemonSet; Java apps export
#                          directly to OTEL_BACKEND_ENDPOINT.  Set to "true"
#                          when otel-beacon is already on the host network.
#                          (default: false)
#   OTEL_AGENT_VERSION     OTel Java agent version to auto-download via init
#                          container if /otel/opentelemetry-javaagent.jar is
#                          not already present on k3d nodes. (default: 2.14.0)
#
# To teardown:
#   kubectl delete namespace <namespace>

set -euo pipefail

NAMESPACE="${1:-petclinic}"
OTEL_BACKEND="${OTEL_BACKEND_ENDPOINT:-http://host.k3d.internal:4318}"
SKIP_COLLECTOR="${SKIP_COLLECTOR:-false}"
OTEL_AGENT_VERSION="${OTEL_AGENT_VERSION:-2.14.0}"
OTEL_AGENT_JAR="/otel/opentelemetry-javaagent.jar"
OTEL_AGENT_URL="https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar"

echo "==> Deploying to namespace:   $NAMESPACE"
echo "==> OTLP backend endpoint:    $OTEL_BACKEND"
echo "==> Skip OTel Collector:      $SKIP_COLLECTOR"
echo "==> OTel Java agent version:  $OTEL_AGENT_VERSION"

# ---------------------------------------------------------------------------
# OTLP env blocks injected into each petclinic pod
# ---------------------------------------------------------------------------
if [ "$SKIP_COLLECTOR" = "true" ]; then
  # Direct export to otel-beacon; no per-node collector hop needed
  OTLP_ENDPOINT="$OTEL_BACKEND"
  NODE_IP_YAML=""
else
  # Via the OTel Collector DaemonSet hostPort on each node
  OTLP_ENDPOINT='http://$(NODE_IP):4318'
  NODE_IP_YAML='            - name: NODE_IP
              valueFrom: {fieldRef: {fieldPath: status.hostIP}}'
fi

# ---------------------------------------------------------------------------
# Step 1: Namespace
# ---------------------------------------------------------------------------
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# ---------------------------------------------------------------------------
# Steps 2-5: OTel Collector (skipped when SKIP_COLLECTOR=true)
# ---------------------------------------------------------------------------
if [ "$SKIP_COLLECTOR" != "true" ]; then

# Step 2: RBAC
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: otel-collector
  namespace: $NAMESPACE
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: otel-collector-$NAMESPACE
rules:
- apiGroups: [""]
  resources: [nodes, nodes/stats, nodes/proxy, pods, namespaces, endpoints]
  verbs: [get, list, watch]
- apiGroups: [apps]
  resources: [replicasets, deployments, daemonsets, statefulsets]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: otel-collector-$NAMESPACE
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: otel-collector-$NAMESPACE
subjects:
- kind: ServiceAccount
  name: otel-collector
  namespace: $NAMESPACE
EOF

# Step 3: Classic SA token
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: otel-collector-token
  namespace: $NAMESPACE
  annotations:
    kubernetes.io/service-account.name: otel-collector
type: kubernetes.io/service-account-token
EOF

echo "--- Waiting for SA token ---"
for i in $(seq 1 15); do
  TOKEN=$(kubectl get secret otel-collector-token -n "$NAMESPACE" -o jsonpath='{.data.token}' 2>/dev/null | base64 -d 2>/dev/null || true)
  [ -n "$TOKEN" ] && break
  sleep 2
done
if [ -z "$TOKEN" ]; then echo "ERROR: SA token not populated after 30s"; exit 1; fi

CA_CERT=$(kubectl get secret otel-collector-token -n "$NAMESPACE" -o jsonpath='{.data.ca\.crt}')

# Step 4: kubeconfig Secret
kubectl create secret generic otel-collector-kubeconfig -n "$NAMESPACE" \
  --from-literal=kubeconfig="apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${CA_CERT}
    server: https://kubernetes.default.svc.cluster.local
  name: local
contexts:
- context:
    cluster: local
    user: otel-collector
  name: local
current-context: local
users:
- name: otel-collector
  user:
    token: ${TOKEN}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Step 5: OTel Collector ConfigMap + DaemonSet
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector-config
  namespace: $NAMESPACE
data:
  config.yaml: |
    receivers:
      otlp:
        protocols:
          http:
            endpoint: 0.0.0.0:4318
          grpc:
            endpoint: 0.0.0.0:4317

      kubeletstats:
        collection_interval: 30s
        auth_type: kubeConfig
        endpoint: \${env:K8S_NODE_NAME}
        insecure_skip_verify: true
        metric_groups: [node, pod, container]

    processors:
      batch:
        timeout: 10s

      k8sattributes:
        auth_type: serviceAccount
        passthrough: false
        extract:
          metadata:
            - k8s.pod.name
            - k8s.pod.uid
            - k8s.deployment.name
            - k8s.namespace.name
            - k8s.node.name
            - k8s.replicaset.name
            - k8s.pod.start_time
        pod_association:
          - sources:
              - from: resource_attribute
                name: k8s.pod.ip
          - sources:
              - from: resource_attribute
                name: k8s.pod.uid
          - sources:
              - from: connection

    exporters:
      otlp_http:
        endpoint: ${OTEL_BACKEND}
        tls:
          insecure: true

    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch, k8sattributes]
          exporters: [otlp_http]
        metrics:
          receivers: [otlp, kubeletstats]
          processors: [batch, k8sattributes]
          exporters: [otlp_http]
        logs:
          receivers: [otlp]
          processors: [batch, k8sattributes]
          exporters: [otlp_http]
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: otel-collector
  namespace: $NAMESPACE
spec:
  selector:
    matchLabels:
      app: otel-collector
  template:
    metadata:
      labels:
        app: otel-collector
    spec:
      serviceAccountName: otel-collector
      hostNetwork: true
      hostPID: true
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
        - operator: Exists
      containers:
        - name: otel-collector
          image: otel/opentelemetry-collector-contrib:latest
          args: [--config=/conf/config.yaml]
          env:
            - name: K8S_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: KUBECONFIG
              value: /var/kubecfg/kubeconfig
          ports:
            - containerPort: 4317
              hostPort: 4317
            - containerPort: 4318
              hostPort: 4318
          resources:
            requests: {cpu: 100m, memory: 128Mi}
            limits: {cpu: 500m, memory: 512Mi}
          volumeMounts:
            - name: config
              mountPath: /conf
            - name: kubeconfig
              mountPath: /var/kubecfg/kubeconfig
              subPath: kubeconfig
      volumes:
        - name: config
          configMap:
            name: otel-collector-config
        - name: kubeconfig
          secret:
            secretName: otel-collector-kubeconfig
EOF

echo ""
echo "==> Waiting for collector DaemonSet rollout..."
kubectl rollout status daemonset/otel-collector -n "$NAMESPACE" --timeout=120s

fi  # end SKIP_COLLECTOR

# ---------------------------------------------------------------------------
# Step 6: MySQL
# ---------------------------------------------------------------------------
kubectl apply -f - <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: mysql-secret
  namespace: $NAMESPACE
type: Opaque
stringData:
  mysql-root-password: "petclinic"
  mysql-password: "petclinic"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql
  namespace: $NAMESPACE
spec:
  serviceName: mysql
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
      containers:
        - name: mysql
          image: mysql:8.0
          ports:
            - containerPort: 3306
          env:
            - name: MYSQL_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: mysql-secret
                  key: mysql-root-password
            - name: MYSQL_DATABASE
              value: petclinic
            - name: MYSQL_USER
              value: petclinic
            - name: MYSQL_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: mysql-secret
                  key: mysql-password
          resources:
            requests:
              memory: 512Mi
            limits:
              memory: 1Gi
          readinessProbe:
            exec:
              command: ["mysqladmin", "ping", "-h", "localhost", "-upetclinic", "-ppetclinic"]
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 5
          volumeMounts:
            - name: mysql-data
              mountPath: /var/lib/mysql
  volumeClaimTemplates:
    - metadata:
        name: mysql-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 2Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: $NAMESPACE
spec:
  selector:
    app: mysql
  ports:
    - port: 3306
      targetPort: 3306
EOF

echo "--- Waiting for MySQL to be ready ---"
kubectl rollout status statefulset/mysql -n "$NAMESPACE" --timeout=180s

# ---------------------------------------------------------------------------
# Step 7: PetClinic services
#
# All pods include an otel-agent-init init container that downloads the OTel
# Java agent to /otel on the k3d node (hostPath DirectoryOrCreate) if it is
# not already present.  Subsequent pod restarts skip the download entirely.
# ---------------------------------------------------------------------------

# Common otel env block (bash-expanded into the heredoc below)
COMMON_OTEL_ENV=$(cat <<ENVEOF
            - {name: JAVA_TOOL_OPTIONS, value: "-javaagent:${OTEL_AGENT_JAR}"}
            - {name: OTEL_EXPORTER_OTLP_PROTOCOL, value: http/protobuf}
            - {name: OTEL_METRICS_EXPORTER, value: otlp}
            - {name: OTEL_LOGS_EXPORTER, value: otlp}
            - name: POD_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: POD_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
            - name: POD_UID
              valueFrom: {fieldRef: {fieldPath: metadata.uid}}
            - name: OTEL_RESOURCE_ATTRIBUTES
              value: "deployment.environment=kubernetes,k8s.pod.name=\$(POD_NAME),k8s.namespace.name=\$(POD_NAMESPACE),k8s.pod.uid=\$(POD_UID)"
${NODE_IP_YAML}
            - {name: OTEL_EXPORTER_OTLP_ENDPOINT, value: "${OTLP_ENDPOINT}"}
ENVEOF
)

AGENT_INIT_CONTAINER=$(cat <<INITEOF
      initContainers:
        - name: otel-agent-init
          image: alpine:3.20
          command:
            - sh
            - -c
            - "[ -f ${OTEL_AGENT_JAR} ] || wget -qO ${OTEL_AGENT_JAR} ${OTEL_AGENT_URL}"
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
INITEOF
)

AGENT_VOLUME=$(cat <<VOLEOF
        - name: otel-agent
          hostPath: {path: /otel, type: DirectoryOrCreate}
VOLEOF
)

kubectl apply -f - <<EOF
---
# Config Server
apiVersion: apps/v1
kind: Deployment
metadata:
  name: config-server
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: config-server}
  template:
    metadata:
      labels: {app: config-server}
    spec:
${AGENT_INIT_CONTAINER}
      containers:
        - name: config-server
          image: springcommunity/spring-petclinic-config-server:latest
          ports: [{containerPort: 8888}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: docker}
            - {name: OTEL_SERVICE_NAME, value: config-server}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: config-server
  namespace: $NAMESPACE
spec:
  selector: {app: config-server}
  ports: [{port: 8888}]
---
# Discovery Server
apiVersion: apps/v1
kind: Deployment
metadata:
  name: discovery-server
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: discovery-server}
  template:
    metadata:
      labels: {app: discovery-server}
    spec:
${AGENT_INIT_CONTAINER}
        - name: wait-config
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z config-server 8888; do sleep 2; done']
      containers:
        - name: discovery-server
          image: springcommunity/spring-petclinic-discovery-server:latest
          ports: [{containerPort: 8761}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: docker}
            - {name: OTEL_SERVICE_NAME, value: discovery-server}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: discovery-server
  namespace: $NAMESPACE
spec:
  selector: {app: discovery-server}
  ports: [{port: 8761}]
---
# Customers Service
apiVersion: apps/v1
kind: Deployment
metadata:
  name: customers-service
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: customers-service}
  template:
    metadata:
      labels: {app: customers-service}
    spec:
${AGENT_INIT_CONTAINER}
        - name: wait-discovery
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z discovery-server 8761; do sleep 2; done']
        - name: wait-mysql
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z mysql 3306; do sleep 3; done']
      containers:
        - name: customers-service
          image: springcommunity/spring-petclinic-customers-service:latest
          ports: [{containerPort: 8081}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: "docker,mysql"}
            - {name: SPRING_DATASOURCE_URL, value: "jdbc:mysql://mysql:3306/petclinic?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"}
            - {name: SPRING_DATASOURCE_USERNAME, value: petclinic}
            - name: SPRING_DATASOURCE_PASSWORD
              valueFrom:
                secretKeyRef: {name: mysql-secret, key: mysql-password}
            - {name: SPRING_SQL_INIT_MODE, value: always}
            - {name: OTEL_SERVICE_NAME, value: customers-service}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: customers-service
  namespace: $NAMESPACE
spec:
  selector: {app: customers-service}
  ports: [{port: 8081}]
---
# Vets Service
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vets-service
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: vets-service}
  template:
    metadata:
      labels: {app: vets-service}
    spec:
${AGENT_INIT_CONTAINER}
        - name: wait-discovery
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z discovery-server 8761; do sleep 2; done']
        - name: wait-mysql
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z mysql 3306; do sleep 3; done']
      containers:
        - name: vets-service
          image: springcommunity/spring-petclinic-vets-service:latest
          ports: [{containerPort: 8083}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: "docker,mysql"}
            - {name: SPRING_DATASOURCE_URL, value: "jdbc:mysql://mysql:3306/petclinic?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"}
            - {name: SPRING_DATASOURCE_USERNAME, value: petclinic}
            - name: SPRING_DATASOURCE_PASSWORD
              valueFrom:
                secretKeyRef: {name: mysql-secret, key: mysql-password}
            - {name: SPRING_SQL_INIT_MODE, value: always}
            - {name: OTEL_SERVICE_NAME, value: vets-service}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: vets-service
  namespace: $NAMESPACE
spec:
  selector: {app: vets-service}
  ports: [{port: 8083}]
---
# Visits Service
apiVersion: apps/v1
kind: Deployment
metadata:
  name: visits-service
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: visits-service}
  template:
    metadata:
      labels: {app: visits-service}
    spec:
${AGENT_INIT_CONTAINER}
        - name: wait-discovery
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z discovery-server 8761; do sleep 2; done']
        - name: wait-mysql
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z mysql 3306; do sleep 3; done']
        - name: wait-customers
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z customers-service 8081; do sleep 3; done']
      containers:
        - name: visits-service
          image: springcommunity/spring-petclinic-visits-service:latest
          ports: [{containerPort: 8082}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: "docker,mysql"}
            - {name: SPRING_DATASOURCE_URL, value: "jdbc:mysql://mysql:3306/petclinic?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"}
            - {name: SPRING_DATASOURCE_USERNAME, value: petclinic}
            - name: SPRING_DATASOURCE_PASSWORD
              valueFrom:
                secretKeyRef: {name: mysql-secret, key: mysql-password}
            - {name: SPRING_SQL_INIT_MODE, value: always}
            - {name: OTEL_SERVICE_NAME, value: visits-service}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: visits-service
  namespace: $NAMESPACE
spec:
  selector: {app: visits-service}
  ports: [{port: 8082}]
---
# API Gateway
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-gateway
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: api-gateway}
  template:
    metadata:
      labels: {app: api-gateway}
    spec:
${AGENT_INIT_CONTAINER}
        - name: wait-discovery
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z discovery-server 8761; do sleep 2; done']
      containers:
        - name: api-gateway
          image: springcommunity/spring-petclinic-api-gateway:latest
          ports: [{containerPort: 8080}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: docker}
            - {name: OTEL_SERVICE_NAME, value: api-gateway}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: api-gateway
  namespace: $NAMESPACE
spec:
  type: NodePort
  selector: {app: api-gateway}
  ports:
    - port: 8080
      nodePort: 30081
---
# Admin Server
apiVersion: apps/v1
kind: Deployment
metadata:
  name: admin-server
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels: {app: admin-server}
  template:
    metadata:
      labels: {app: admin-server}
    spec:
${AGENT_INIT_CONTAINER}
        - name: wait-discovery
          image: busybox:1.28
          command: ['sh', '-c', 'until nc -z discovery-server 8761; do sleep 2; done']
      containers:
        - name: admin-server
          image: springcommunity/spring-petclinic-admin-server:latest
          ports: [{containerPort: 9090}]
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: docker}
            - {name: OTEL_SERVICE_NAME, value: admin-server}
${COMMON_OTEL_ENV}
          volumeMounts:
            - {name: otel-agent, mountPath: /otel}
      volumes:
${AGENT_VOLUME}
---
apiVersion: v1
kind: Service
metadata:
  name: admin-server
  namespace: $NAMESPACE
spec:
  selector: {app: admin-server}
  ports: [{port: 9090}]
EOF

echo ""
echo "==> All done. Pod status:"
kubectl get pods -n "$NAMESPACE"
echo ""
echo "Access otel-beacon UI:  http://<EC2-PUBLIC-IP>:8080"
echo "OTel ingest endpoint:   $OTEL_BACKEND"
echo ""
