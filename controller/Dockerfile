# Build from the REPO ROOT so the blueprint manifest (which lives in
# ../manifests/) is inside the build context:
#
#   docker build -f controller/Dockerfile -t jp36/emulator-controller:latest .
#
# (The old `cd controller && docker build .` form will fail because the
# manifests/ directory isn't visible from controller/'s build context.)
FROM python:3.12-slim
WORKDIR /app

# PyYAML is used by materializer.py to parse the worker-template blueprint.
RUN pip install --no-cache-dir pyyaml

COPY controller/controller.py   /app/controller.py
COPY controller/graph.py        /app/graph.py
COPY controller/k8s.py          /app/k8s.py
COPY controller/materializer.py /app/materializer.py
COPY controller/watcher.py      /app/watcher.py

# The blueprint stencil. materializer.py reads this at runtime and
# substitutes per-role placeholders before POSTing to the k8s API.
COPY manifests/worker-template.yaml /etc/emulator/worker-template.yaml

EXPOSE 8081
ENV CONTROLLER_PORT=8081
ENV BLUEPRINT_PATH=/etc/emulator/worker-template.yaml

CMD ["python", "-u", "controller.py"]
