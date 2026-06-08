# Build from the REPO ROOT so the blueprint manifest (which lives in
# ../manifests/) is inside the build context:
#
#   docker build -f controller/Dockerfile -t jp36/emulator-controller:latest .
#
# (The old `cd controller && docker build .` form will fail because the
# manifests/ directory isn't visible from controller/'s build context.)
FROM python:3.12-slim
WORKDIR /app

# FastAPI + uvicorn serve the HTTP API; PyYAML is used by materialiser.py to
# parse the worker-template blueprint.
COPY controller/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY controller/app.py          /app/app.py
COPY controller/api.py          /app/api.py
COPY controller/graph.py        /app/graph.py
COPY controller/k8s.py          /app/k8s.py
COPY controller/materialiser.py /app/materialiser.py
COPY controller/prom.py         /app/prom.py
COPY controller/watcher.py      /app/watcher.py

# The blueprint stencil. materialiser.py reads this at runtime and
# substitutes per-role placeholders before POSTing to the k8s API.
COPY manifests/worker-template.yaml /etc/emulator/worker-template.yaml

EXPOSE 8081
ENV BLUEPRINT_PATH=/etc/emulator/worker-template.yaml

# One worker process: the watcher reconciler runs in-process as a daemon
# thread (see app.py lifespan), so multiple processes would each run their own
# reconciler. Concurrency for blocking handlers comes from uvicorn's threadpool
# for sync `def` endpoints, which is what keeps /health responsive during a
# long materialise. --port honours CONTROLLER_PORT if set, else 8081.
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${CONTROLLER_PORT:-8081}"]
