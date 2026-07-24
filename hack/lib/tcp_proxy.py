#!/usr/bin/env python3

import argparse
import asyncio
import contextlib
import signal


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError):
            await writer.wait_closed()


async def proxy_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
) -> None:
    try:
        server_reader, server_writer = await asyncio.open_connection(
            target_host, target_port
        )
    except OSError:
        client_writer.close()
        with contextlib.suppress(ConnectionError):
            await client_writer.wait_closed()
        return

    tasks = {
        asyncio.create_task(relay(client_reader, server_writer)),
        asyncio.create_task(relay(server_reader, client_writer)),
    }
    _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal TCP forwarding proxy")
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", required=True, type=int)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", required=True, type=int)
    args = parser.parse_args()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_name, stop.set)

    server = await asyncio.start_server(
        lambda reader, writer: proxy_connection(
            reader, writer, args.target_host, args.target_port
        ),
        args.listen_host,
        args.listen_port,
    )
    print(
        f"ready={args.listen_host}:{args.listen_port}"
        f" target={args.target_host}:{args.target_port}",
        flush=True,
    )
    async with server:
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
