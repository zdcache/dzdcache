import asyncio
import signal
import sys

listen_port, target_port = int(sys.argv[1]), int(sys.argv[2])
state = {"blocked": False, "connections": set()}


def toggle():
    state["blocked"] = not state["blocked"]
    if state["blocked"]:
        for writer in list(state["connections"]):
            writer.close()


async def pipe(reader, writer):
    try:
        while not state["blocked"]:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def handle(client_reader, client_writer):
    if state["blocked"]:
        client_writer.close()
        return
    try:
        server_reader, server_writer = await asyncio.open_connection("127.0.0.1", target_port)
    except OSError:
        client_writer.close()
        return
    state["connections"].update({client_writer, server_writer})
    await asyncio.gather(pipe(client_reader, server_writer), pipe(server_reader, client_writer))
    state["connections"].difference_update({client_writer, server_writer})


async def main():
    asyncio.get_running_loop().add_signal_handler(signal.SIGUSR1, toggle)
    server = await asyncio.start_server(handle, "127.0.0.1", listen_port)
    async with server:
        await server.serve_forever()


asyncio.run(main())
