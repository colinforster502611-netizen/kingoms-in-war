import argparse
import asyncio
import json
import os
import secrets
import string
import sys
import threading
import webbrowser
from http import HTTPStatus
from pathlib import Path
from urllib.parse import unquote, urlsplit

from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Response


rooms = {}
ROOM_CODE_ALPHABET = string.ascii_uppercase + string.digits
ROOM_CODE_LENGTH = 6


def asset_path(name):
    bundle_dir = getattr(sys, "_MEIPASS", None)
    base_dir = Path(bundle_dir) if bundle_dir else Path(__file__).resolve().parent
    return base_dir / name


def http_response(status, content_type, body):
    return Response(
        status.value,
        status.phrase,
        Headers([
            ("Content-Type", content_type),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-cache"),
            ("X-Content-Type-Options", "nosniff"),
        ]),
        body,
    )


async def serve_http_or_websocket(connection, request):
    path = unquote(urlsplit(request.path).path)
    if path == "/ws":
        return None
    if path not in ("/", "/spiel.html"):
        return http_response(HTTPStatus.NOT_FOUND, "text/plain; charset=utf-8", b"Not found")

    try:
        body = await asyncio.to_thread(asset_path("spiel.html").read_bytes)
    except OSError as error:
        print(f"Could not read game page: {error}", file=sys.stderr, flush=True)
        return http_response(HTTPStatus.INTERNAL_SERVER_ERROR, "text/plain; charset=utf-8", b"Game page unavailable")
    return http_response(HTTPStatus.OK, "text/html; charset=utf-8", body)


async def send_json(connection, message):
    await connection.send(json.dumps(message, separators=(",", ":")))


def new_room_code():
    for _ in range(20):
        code = "".join(secrets.choice(ROOM_CODE_ALPHABET) for _ in range(ROOM_CODE_LENGTH))
        if code not in rooms:
            return code
    raise RuntimeError("Unable to allocate a room code")


async def handle_client(connection):
    role = None
    room_code = None

    try:
        async for raw_message in connection:
            try:
                message = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                await send_json(connection, {"type": "error", "message": "Invalid message format."})
                continue

            if not isinstance(message, dict) or not isinstance(message.get("type"), str):
                await send_json(connection, {"type": "error", "message": "Invalid message."})
                continue

            message_type = message["type"]
            if message_type == "create_room":
                if room_code:
                    await send_json(connection, {"type": "error", "message": "You are already in a room."})
                    continue
                mode = message.get("mode")
                if mode not in ("coop", "pvp"):
                    await send_json(connection, {"type": "error", "message": "Choose co-op or 1v1."})
                    continue
                room_code = new_room_code()
                role = "host"
                rooms[room_code] = {"host": connection, "guest": None, "mode": mode}
                await send_json(connection, {"type": "room_created", "room": room_code, "mode": mode})
                continue

            if message_type == "join_room":
                if room_code:
                    await send_json(connection, {"type": "error", "message": "You are already in a room."})
                    continue
                requested_code = message.get("room")
                if not isinstance(requested_code, str):
                    await send_json(connection, {"type": "error", "message": "Enter a valid room code."})
                    continue
                requested_code = requested_code.strip().upper()
                room = rooms.get(requested_code)
                if not room:
                    await send_json(connection, {"type": "error", "message": "Room not found. Check the code and try again."})
                    continue
                if room["guest"] is not None:
                    await send_json(connection, {"type": "error", "message": "That room already has two players."})
                    continue
                room["guest"] = connection
                room_code = requested_code
                role = "guest"
                await send_json(connection, {"type": "room_joined", "room": room_code, "mode": room["mode"]})
                await send_json(room["host"], {"type": "guest_joined", "room": room_code})
                continue

            if not room_code or role is None:
                await send_json(connection, {"type": "error", "message": "Create or join a room first."})
                continue

            room = rooms.get(room_code)
            if not room or room.get(role) is not connection:
                await send_json(connection, {"type": "error", "message": "The room is no longer available."})
                continue

            allowed_messages = {
                "host": ("game_start", "game_state", "command_result", "round_finished"),
                "guest": ("command",),
            }
            if message_type not in allowed_messages[role]:
                await send_json(connection, {"type": "error", "message": "Unsupported multiplayer message."})
                continue

            recipient_role = "guest" if role == "host" else "host"
            recipient = room.get(recipient_role)
            if recipient is not None:
                await send_json(recipient, message)
    except ConnectionClosed:
        pass
    finally:
        if room_code:
            room = rooms.get(room_code)
            if room and room.get(role) is connection:
                del rooms[room_code]
                peer_role = "guest" if role == "host" else "host"
                peer = room.get(peer_role)
                if peer:
                    try:
                        await send_json(peer, {"type": "peer_disconnected"})
                    except ConnectionClosed:
                        pass
                    except OSError as error:
                        print(f"Could not notify disconnected room peer: {error}", file=sys.stderr, flush=True)


async def run_server(host, port, open_browser):
    async with serve(
        handle_client,
        host,
        port,
        process_request=serve_http_or_websocket,
        max_size=262144,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"Kingdoms in war server ready at http://{host}:{port}", flush=True)
        if open_browser:
            threading.Timer(1, lambda: webbrowser.open(f"http://127.0.0.1:{port}/")).start()
        await asyncio.Future()


def main():
    parser = argparse.ArgumentParser(description="Kingdoms in war web and multiplayer server")
    parser.add_argument("--desktop", action="store_true", help="serve locally and open the game in your browser")
    args = parser.parse_args()

    if getattr(sys, "frozen", False):
        args.desktop = True
    host = "127.0.0.1" if args.desktop else "0.0.0.0"
    port = 8765 if args.desktop else int(os.environ.get("PORT", "8765"))
    try:
        asyncio.run(run_server(host, port, args.desktop))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
