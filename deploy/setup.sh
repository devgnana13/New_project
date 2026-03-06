#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  Options Analytics Platform — COMPLETE ONE-SHOT SETUP
#  
#  Run on a FRESH Ubuntu VPS as root:
#    sudo bash setup.sh
# ─────────────────────────────────────────────────────────────

set -e

APP_DIR="/home/optionkings/app"
DOMAIN="optionkings.in"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  🚀 Options Analytics Platform — Complete Server Setup"
echo "══════════════════════════════════════════════════════════════"
echo ""

# ── 1. System Updates ──
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [1/9] Updating system packages..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
apt update && apt upgrade -y
echo "  ✅ System updated"

# ── 2. Install Dependencies ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [2/9] Installing system dependencies..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
apt install -y python3 python3-pip python3-venv python3-dev \
    nginx certbot python3-certbot-nginx \
    git curl ufw build-essential libffi-dev libssl-dev
echo "  ✅ System dependencies installed"

# ── 3. Create App User ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [3/9] Creating application user..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if ! id "optionkings" &>/dev/null; then
    useradd -m -s /bin/bash optionkings
    echo "  ✅ User 'optionkings' created"
else
    echo "  ℹ️  User 'optionkings' already exists"
fi

# ── 4. Clone/Update Code from GitHub ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [4/9] Setting up application code..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -d "$APP_DIR/.git" ]; then
    echo "  ℹ️  Project already cloned. Pulling latest code..."
    cd "$APP_DIR"
    git pull origin main
else
    echo "  📦 Cloning project from GitHub..."
    echo ""
    read -p "  Enter your GitHub repo URL (e.g. https://github.com/username/repo.git): " REPO_URL
    
    if [ -z "$REPO_URL" ]; then
        echo "  ❌ No URL provided. Exiting."
        exit 1
    fi
    
    mkdir -p "$APP_DIR"
    git clone "$REPO_URL" "$APP_DIR"
fi
chown -R optionkings:optionkings "$APP_DIR"
echo "  ✅ Code ready at $APP_DIR"

# ── 5. Python Virtual Environment ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [5/9] Setting up Python virtual environment..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
sudo -u optionkings bash -c "
    cd $APP_DIR
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
"
echo "  ✅ Python environment ready"

# ── 6. Create .env file ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [6/9] Configuring environment variables..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -f "$APP_DIR/.env" ]; then
    echo "  ℹ️  .env file already exists. Skipping."
else
    echo ""
    read -p "  Enter KITE_API_KEY: " KITE_KEY
    read -p "  Enter KITE_API_SECRET: " KITE_SECRET
    read -p "  Enter MONGO_URI: " MONGO_URI
    read -p "  Enter MONGO_DB_NAME [options_analytics]: " MONGO_DB
    MONGO_DB=${MONGO_DB:-options_analytics}

    cat > "$APP_DIR/.env" << EOF
KITE_API_KEY=$KITE_KEY
KITE_API_SECRET=$KITE_SECRET

MONGO_URI=$MONGO_URI
MONGO_DB_NAME=$MONGO_DB

DEBUG=False
LOG_LEVEL=INFO
FLASK_PORT=5000
EOF

    chown optionkings:optionkings "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    echo "  ✅ .env file created (permissions: owner-only read)"
fi

# ── 7. Firewall ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [7/9] Configuring firewall..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw --force enable
echo "  ✅ Firewall: SSH + HTTP/HTTPS allowed"

# ── 8. Nginx Configuration ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [8/9] Configuring Nginx (HTTP only for now)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Start with HTTP-only config (certbot will add SSL later)
cat > /etc/nginx/sites-available/optionkings << 'NGINX'
server {
    listen 80;
    server_name optionkings.in www.optionkings.in;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 86400;
        proxy_connect_timeout 60;
    }
}
NGINX

ln -sf /etc/nginx/sites-available/optionkings /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx
echo "  ✅ Nginx configured"

# ── 9. Systemd Service ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  [9/9] Setting up systemd service..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cp "$APP_DIR/deploy/optionkings.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable optionkings
echo "  ✅ Service configured (auto-starts on boot)"

# ── DONE ──
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  ✅ SETUP COMPLETE!"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "  👉 REMAINING STEPS (run these manually):"
echo ""
echo "  1. Start the app:"
echo "     sudo systemctl start optionkings"
echo ""
echo "  2. Check it's running:"
echo "     sudo systemctl status optionkings"
echo ""
echo "  3. Visit http://$DOMAIN and check it loads"
echo ""
echo "  4. Add SSL certificate:"
echo "     sudo certbot --nginx -d $DOMAIN -d www.$DOMAIN"
echo ""
echo "  5. View logs:"
echo "     tail -f $APP_DIR/logs.txt"
echo ""
echo "══════════════════════════════════════════════════════════════"
