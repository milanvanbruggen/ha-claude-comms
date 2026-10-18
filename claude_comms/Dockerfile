# signal-cli + REST API (multi-arch, incl. aarch64). Pinned on purpose: bump deliberately.
FROM bbernhard/signal-cli-rest-api:0.101

# Home Assistant mounts /data as root, so run as root inside this container.
USER root

# normal mode: messages stay queued on Signal's servers until we fetch them,
# so nothing is lost while the add-on restarts.
ENV MODE=normal \
    SIGNAL_CLI_CONFIG_DIR=/data/signal-cli \
    PORT=8080

COPY app/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
    && mkdir -p /data

COPY rootfs/ /
RUN chmod 755 /etc/s6-overlay/s6-rc.d/comms-mcp/run
COPY app/ /opt/app/

WORKDIR /opt/app
