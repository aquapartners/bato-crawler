# Bato Finance Crawler Repository

This repository is part of the **Bato Finance** ecosystem. It serves as a **fallback and documentation hub** for the decentralized scraping network. The actual scraping of bank bonuses is performed by user devices running JavaScript in the Bato Finance web app, leveraging their own residential IPs to avoid blocks.

## 📦 What's in this repo?

- **`version.txt`** – A simple version file updated every 15 minutes by GitHub Actions. Client devices can use this as a fallback if the DHT is unreachable.
- **`bonuses.json`** – (optional) A static mirror of the latest bonus data, updated when a successful scrape occurs (can be used as an additional fallback).
- **Documentation** – This README and future developer guides.

## 🚀 How the scraping actually works

- Every 15 minutes, all online Bato Finance user devices attempt to become the **scraper leader** for that round.
- The first device to successfully acquire a distributed lock (via the BitTorrent DHT) wins and scrapes all bank sites using its own residential IP.
- If that device fails (e.g., blocked), the lock expires and another device takes over.
- The successful scraper produces a new `bonuses.json`, seeds it as a torrent, and announces the new version via the DHT.
- All other devices download the torrent from the swarm and update their local encrypted storage.
- User‑specific data (tracked bonuses, settings) is synchronized between a user's own devices via direct WebRTC handshakes, using the **Simple Sync** protocol.

## 🔧 For Developers

If you wish to contribute to the JavaScript crawler code, please see the `src/` directory in the main Bato Finance web app repository. This repo is only for fallback infrastructure.

## 📄 License

MIT
