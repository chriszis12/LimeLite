# LimeLite

3rd-person build-fight game. Static client + tiny Node websocket switchboard (`server.js`) that lets players join lobbies by code from anywhere.

## run locally
    npm install
    python3 tools/split.py path/to/yardNN.html   # builds public/index.html + public/game-data.json
    npm start                                     # http://localhost:8080

Open it in two browsers (or two machines on your network), copy the lobby code from one and join with it in the other.

## deploy on fly.io
    fly launch --no-deploy --copy-config   # pick your app name, keep fly.toml
    fly deploy --ha=false
For auto-deploy from GitHub: `fly tokens create deploy`, add it as repo secret `FLY_API_TOKEN`, push to `main`.

**Keep ONE machine.** Lobbies live in server memory, so two machines = players on different ones can't find each other (`--ha=false`, already in the workflow).

## updating the game
Drop in the new monolithic html, rerun `tools/split.py`, commit. `game-data.json` is fingerprinted and cached forever by browsers, so the big file only downloads when it changes.

## mobile
Touch controls turn on automatically on phones/tablets (add `?touch=1` to force them on). Left thumb = move (drag far = sprint), right side = look (tap = shoot), plus fire/jump/crouch/aim/reload/build buttons. Shadows start off and pixel ratio is capped on touch for speed. Landscape only.

## keybind presets
Add presets in the `PRESETS` object at the top of the settings section (search `KEYBIND PRESETS` in `public/index.html`) — or send them to me and I'll add them. They show up in Settings → Keybinds.

## notes
- Server only relays messages by lobby code. Damage/hits are client-side (no anti-cheat).
- If the server is unreachable the game falls back to same-browser local play.
