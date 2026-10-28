#!/usr/bin/env bash
# Hetzner Cloud Firewall for the staging server (docs/staging-deployment-plan.md).
# Runs on a workstation with an hcloud context, not on the server.
#
#   deploy/hetzner_firewall.sh            # create or refresh, then attach
#
# Web traffic must come through Cloudflare: 80/443 are open only to
# Cloudflare's published ranges, fetched each run, so rerun this when they
# change. SSH (22) and ICMP stay open to all; fail2ban and key-only SSH
# guard 22. Outbound is not filtered (no outbound rules).
#
# UFW on the server can't do this: Docker's published ports bypass it. This
# firewall sits in Hetzner's network, in front of the server.
set -euo pipefail

NAME=ballistics-web
SERVER=ballistics-v2

ranges() {
  curl -fsS "https://www.cloudflare.com/ips-$1" | grep -E '^[0-9a-f:.]+/[0-9]+$'
}
v4="$(ranges v4)"
v6="$(ranges v6)"
[[ -n "$v4" && -n "$v6" ]] || { echo "could not fetch Cloudflare ranges" >&2; exit 1; }

rules="$(mktemp)"
trap 'rm -f "$rules"' EXIT
python3 - "$v4" "$v6" > "$rules" <<'EOF'
import json, sys
cloudflare = sys.argv[1].split() + sys.argv[2].split()
anywhere = ["0.0.0.0/0", "::/0"]
rule = lambda port, ips, what: {"direction": "in", "protocol": "tcp", "port": port,
                                "source_ips": ips, "description": what}
print(json.dumps([
    rule("22", anywhere, "SSH"),
    rule("80", cloudflare, "HTTP from Cloudflare"),
    rule("443", cloudflare, "HTTPS from Cloudflare"),
    {"direction": "in", "protocol": "icmp", "source_ips": anywhere, "description": "ping"},
], indent=2))
EOF

if hcloud firewall describe "$NAME" > /dev/null 2>&1; then
  hcloud firewall replace-rules "$NAME" --rules-file "$rules"
else
  hcloud firewall create --name "$NAME" --rules-file "$rules"
fi
server_id="$(hcloud server describe "$SERVER" -o format='{{.ID}}')"
applied="$(hcloud firewall describe "$NAME" -o format='{{range .AppliedTo}}{{if .Server}}{{.Server.ID}} {{end}}{{end}}')"
if [[ " $applied " != *" $server_id "* ]]; then
  hcloud firewall apply-to-resource "$NAME" --type server --server "$SERVER"
fi
hcloud firewall describe "$NAME"
