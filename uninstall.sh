#!/usr/bin/env bash
set -e

SERVICE_NAME="ha-longterm-stats"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "============================================================"
echo "  Home Assistant Long-Term Stats - Systemd Service Removal"
echo "============================================================"

if [ "$EUID" -ne 0 ]; then
    SUDO="sudo"
else
    SUDO=""
fi

if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
    echo "🛑 Stopping ${SERVICE_NAME} service..."
    $SUDO systemctl stop "${SERVICE_NAME}"
fi

if systemctl is-enabled --quiet "${SERVICE_NAME}" 2>/dev/null; then
    echo "🚫 Disabling ${SERVICE_NAME} service on boot..."
    $SUDO systemctl disable "${SERVICE_NAME}"
fi

if [ -f "${SERVICE_FILE}" ]; then
    echo "🗑️ Removing ${SERVICE_FILE}..."
    $SUDO rm -f "${SERVICE_FILE}"
    $SUDO systemctl daemon-reload
    $SUDO systemctl reset-failed
fi

echo "✨ Service successfully uninstalled!"
echo "============================================================"
