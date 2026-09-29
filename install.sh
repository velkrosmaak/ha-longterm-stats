#!/usr/bin/env bash
set -e

# Home Assistant Long-Term Stats Service Installer for Linux (systemd)

SERVICE_NAME="ha-longterm-stats"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# Determine script directory & execution user
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_USER="${SUDO_USER:-$USER}"

if [ "$TARGET_USER" = "root" ]; then
    REAL_USER_HOME="/root"
else
    REAL_USER_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6)"
fi

echo "============================================================"
echo "  Home Assistant Long-Term Stats - Systemd Service Setup"
echo "============================================================"
echo "Installation Directory: ${SCRIPT_DIR}"
echo "Target Service User:    ${TARGET_USER}"
echo "------------------------------------------------------------"

# Check systemd availability
if ! command -v systemctl >/dev/null 2>&1; command -v systemd >/dev/null 2>&1; then
    echo "❌ Error: systemd is required to install as a service on boot."
    exit 1
fi

# 1. Ensure Python 3 and venv are installed
if ! command -v python3 >/dev/null 2>&1; then
    echo "❌ Error: python3 is not installed. Please install python3 first."
    exit 1
fi

# 2. Setup Python virtual environment
VENV_DIR="${SCRIPT_DIR}/venv"
if [ ! -d "${VENV_DIR}" ]; then
    echo "📦 Creating Python virtual environment in ${VENV_DIR}..."
    python3 -m venv "${VENV_DIR}"
fi

echo "📦 Installing/Updating Python dependencies..."
"${VENV_DIR}/bin/pip" install --upgrade pip >/dev/null 2>&1 || true
"${VENV_DIR}/bin/pip" install -r "${SCRIPT_DIR}/requirements.txt"

# 3. Create .env configuration file if missing
ENV_FILE="${SCRIPT_DIR}/.env"
if [ ! -f "${ENV_FILE}" ]; then
    echo "📝 Creating .env configuration file from template..."
    cp "${SCRIPT_DIR}/.env.example" "${ENV_FILE}"
    echo "⚠️  PLEASE NOTE: Edit ${ENV_FILE} to set your Home Assistant URL and HA_TOKEN."
fi

# 4. Generate systemd service unit file
echo "⚙️  Generating systemd unit file at ${SERVICE_FILE}..."

# Must run with sudo / root to write to /etc/systemd/system
if [ "$EUID" -ne 0 ]; then
    echo "🔐 Elevating permissions to write ${SERVICE_FILE} and enable service..."
    SUDO="sudo"
else
    SUDO=""
fi

$SUDO bash -c "cat <<EOF > ${SERVICE_FILE}
[Unit]
Description=Home Assistant Long-Term Stats & Analytics Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${TARGET_USER}
WorkingDirectory=${SCRIPT_DIR}
Environment=\"PYTHONPATH=${SCRIPT_DIR}\"
ExecStart=${VENV_DIR}/bin/python ${SCRIPT_DIR}/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF"

# 5. Reload systemd daemon & enable service on boot
echo "🚀 Reloading systemd daemon and enabling ${SERVICE_NAME} on boot..."
$SUDO systemctl daemon-reload
$SUDO systemctl enable "${SERVICE_NAME}.service"
$SUDO systemctl restart "${SERVICE_NAME}.service"

echo "------------------------------------------------------------"
echo "✨ Installation complete! Service is running and enabled on boot."
echo ""
echo "Commands to manage the service:"
echo "  • View status:  sudo systemctl status ${SERVICE_NAME}"
echo "  • View logs:    sudo journalctl -u ${SERVICE_NAME} -f"
echo "  • Restart:      sudo systemctl restart ${SERVICE_NAME}"
echo "  • Stop:         sudo systemctl stop ${SERVICE_NAME}"
echo "============================================================"
