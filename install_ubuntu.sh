#!/usr/bin/env bash

set -e

echo "Updating system..."
sudo apt update
sudo apt upgrade -y

echo "Installing base packages..."
sudo apt install -y \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  software-properties-common \
  unzip

echo "Installing Git..."
sudo apt install -y git

echo "Installing Python 3, venv, and build headers..."
sudo apt install -y \
  python3 \
  python3-pip \
  python3-venv \
  python3-dev

echo "Installing PostgreSQL development libraries (for psycopg2)..."
sudo apt install -y libpq-dev

echo "Installing ODBC libraries (for pyodbc / SQL Server)..."
sudo apt install -y unixodbc unixodbc-dev

echo "Python version:"
python3 --version

if ! command -v docker >/dev/null 2>&1; then
  echo "Installing Docker..."

  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
    https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

  sudo apt update
  sudo apt install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin
else
  echo "Docker already installed."
fi

echo "Enabling Docker service..."
sudo systemctl enable docker
sudo systemctl start docker

echo "Adding current user to docker group..."
sudo usermod -aG docker "$USER"

echo ""
echo "IMPORTANT: Log out and back in for Docker group changes to take effect."
echo "Alternatively, run: newgrp docker (temporary for this shell)"
echo ""

echo "Versions summary:"
git --version
docker --version
python3 --version
pip3 --version
