# Run the indexer with systemd

This system service starts at boot, survives SSH logout, and restarts after a
failure. It runs as an unprivileged dynamic user, listens on UDP port **11223**,
and advertises itself using the protocol's public Mainline rendezvous hash.
No user account or writable state directory needs to be created.

Run these commands from the repository root on the Linux server:

```sh
cargo build --release --locked -p udp-addr-index --features cli --bin udp-addr-index
sudo install -m 0755 target/release/udp-addr-index /usr/local/bin/udp-addr-index
sudo install -m 0644 udp-addr-index/examples/systemd/udp-addr-index.service /etc/systemd/system/udp-addr-index.service
sudo systemd-analyze verify /etc/systemd/system/udp-addr-index.service
sudo systemctl daemon-reload
sudo systemctl enable --now udp-addr-index.service
```

Allow inbound UDP **11223** in the server firewall and any hosting-provider
firewall. Mainline also needs outbound UDP to arbitrary peer ports and working
DNS. The service binds all IPv4 interfaces. If behind NAT, forward UDP 11223 to
the server. A running service alone does not establish public reachability.

## Status and logs

```sh
sudo systemctl status udp-addr-index.service
sudo journalctl -u udp-addr-index.service -f
sudo journalctl -u udp-addr-index.service --since '10 minutes ago'
```

The example enables debug logging for writes, read hits/misses, invalid tokens,
and rate-limited requests. To reduce logging, run
`sudo systemctl edit udp-addr-index.service` and add:

```ini
[Service]
Environment=RUST_LOG=info
```

Then run `sudo systemctl restart udp-addr-index.service`. To change arguments
through an override, clear `ExecStart=` before supplying its replacement.
Keep `--rendezvous-hash` for public discovery; omitting it serves requests without
advertising the indexer. Prometheus metrics can be enabled with
`--metrics-listen 127.0.0.1:9090`.

## Upgrade or stop

Build the new binary, then stop the service before replacing the executable:

```sh
sudo systemctl stop udp-addr-index.service
sudo install -m 0755 target/release/udp-addr-index /usr/local/bin/udp-addr-index
sudo systemctl start udp-addr-index.service
```

To stop it and disable startup at boot:

```sh
sudo systemctl disable --now udp-addr-index.service
```

The index stores mappings in memory. Restarting it empties those mappings;
providers repopulate them when they republish. The service keeps the process
running independently of an SSH session; it does not add persistent storage.
