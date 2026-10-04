"""Run a command inside a running Compose service container through the Docker Engine API."""
#? ==========================================================================================
#? Permite que Kestra ejecute dbt y el job de Spark en SUS contenedores (dbt-ui-backend y
#? snowpark-connect), que ya tienen instaladas sus dependencias y montado el codigo.
#? Equivale a "docker compose exec <servicio> <comando>", pero sin depender del CLI de Docker:
#? habla directamente con la API de Docker por el socket /var/run/docker.sock que docker-compose
#? monta en el contenedor de Kestra. Solo usa la libreria estandar de Python.
#?
#? Uso:
#?   python3 docker_exec.py <servicio> <workdir> <comando> [args...]
#? Ej.:
#?   python3 docker_exec.py dbt-ui-backend /workspace/dbt-projects/ssd_failure_prediction \
#?       /opt/dbt-ui/backend/.venv/bin/dbt build --profiles-dir /home/dbtui/.dbt
#?
#? La salida del comando se reenvia al log de Kestra y el codigo de salida se propaga: si dbt o
#? el job fallan, la tarea de Kestra falla (y aplica su retry / errors).
#? ==========================================================================================
from __future__ import annotations

import http.client
import json
import os
import socket
import struct
import sys
import urllib.parse

DOCKER_SOCKET = os.environ.get('DOCKER_SOCKET', '/var/run/docker.sock')
#? Nombre del proyecto compose (clave "name:" de docker-compose.yml). Sirve para no confundir
#? contenedores de otro proyecto que tengan un servicio con el mismo nombre.
COMPOSE_PROJECT = os.environ.get('COMPOSE_PROJECT', 'ciencia-de-datos-ssd-failure-pipeline-pset-2')


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP sobre el socket UNIX de Docker (en vez de TCP)."""

    def __init__(self, path: str, timeout: float | None = None):
        super().__init__('localhost', timeout=timeout)
        self.unix_path = path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.unix_path)
        self.sock = sock


def docker_request(method: str, path: str, body: dict | None = None, timeout: float | None = 60):
    #? Una peticion a la API de Docker; devuelve (status, respuesta abierta).
    connection = UnixHTTPConnection(DOCKER_SOCKET, timeout=timeout)
    payload = json.dumps(body).encode('utf-8') if body is not None else None
    headers = {'Content-Type': 'application/json'} if payload is not None else {}
    connection.request(method, path, body=payload, headers=headers)
    return connection.getresponse()


def docker_json(method: str, path: str, body: dict | None = None):
    response = docker_request(method, path, body)
    data = response.read()
    if response.status >= 300:
        raise RuntimeError(f'Docker API {method} {path} -> {response.status}: {data[:500]!r}')
    return json.loads(data) if data else None


def find_service_container(service: str) -> str:
    #? Busca el contenedor EN EJECUCION del servicio por las etiquetas que pone docker compose.
    filters = json.dumps({
        'label': [
            f'com.docker.compose.service={service}',
            f'com.docker.compose.project={COMPOSE_PROJECT}',
        ],
        'status': ['running'],
    })
    containers = docker_json('GET', '/containers/json?filters=' + urllib.parse.quote(filters))
    if not containers:
        raise RuntimeError(
            f'No running container for service {service!r} in project {COMPOSE_PROJECT!r}. '
            'Start it with: docker compose --env-file src/res/env/.env up -d ' + service
        )
    return containers[0]['Id']


def stream_exec_output(response) -> None:
    #? Sin TTY, Docker multiplexa stdout/stderr en tramas: cabecera de 8 bytes
    #? (tipo de flujo, 3 bytes de relleno, largo de 4 bytes big-endian) + contenido.
    while True:
        header = response.read(8)
        if len(header) < 8:
            return
        stream_type, length = struct.unpack('>BxxxI', header)
        chunk = response.read(length)
        target = sys.stderr if stream_type == 2 else sys.stdout
        target.write(chunk.decode('utf-8', errors='replace'))
        target.flush()


def run(service: str, workdir: str, command: list[str]) -> int:
    container_id = find_service_container(service)
    print(f'[docker_exec] {service} ({container_id[:12]}) $ {" ".join(command)}', flush=True)
    #? 1) Crear el exec dentro del contenedor (como "docker exec", hereda su entorno y volumenes).
    exec_id = docker_json('POST', f'/containers/{container_id}/exec', {
        'AttachStdout': True,
        'AttachStderr': True,
        'Tty': False,
        'WorkingDir': workdir,
        'Cmd': command,
    })['Id']
    #? 2) Iniciarlo y transmitir la salida en vivo (timeout None: dbt o la OBT pueden tardar).
    response = docker_request('POST', f'/exec/{exec_id}/start', {'Detach': False, 'Tty': False}, timeout=None)
    if response.status >= 300:
        raise RuntimeError(f'Docker exec start failed: {response.status} {response.read()[:500]!r}')
    stream_exec_output(response)
    #? 3) Leer el codigo de salida del proceso.
    exit_code = docker_json('GET', f'/exec/{exec_id}/json')['ExitCode']
    print(f'[docker_exec] exit code {exit_code}', flush=True)
    return int(exit_code if exit_code is not None else 1)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise SystemExit('usage: docker_exec.py SERVICE WORKDIR COMMAND [ARGS...]')
    sys.exit(run(sys.argv[1], sys.argv[2], sys.argv[3:]))
