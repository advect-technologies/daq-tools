#!/usr/bin/env python3
"""
Test HTTP server for daq-tools HttpPostSink.
Supports both JSON batches and Line Protocol format.
"""

import json
from aiohttp import web
from rich import print_json, print
import asyncio

async def handle_post(request: web.Request):
    """Handle POST requests - auto-detects JSON or Line Protocol."""
    try:
        content_type = request.headers.get("Content-Type", "").lower()
        body = await request.read()
        
        print("\n" + "="*90)
        print(f"Received POST to {request.path} | Content-Type: {content_type}")
        print("="*90)

        if "application/json" in content_type or body.strip().startswith(b'['):
            # JSON format (list of DataPoints)
            data = await request.json()
            print(f"✅ JSON Mode: Received batch of {len(data)} DataPoints\n")
            
            for i, item in enumerate(data):
                print(f"--- DataPoint {i+1} ---")
                print_json(json.dumps(item, indent=2))
                print()

        else:
            # Line Protocol format (text/plain, one line per point)
            text = body.decode("utf-8").strip()
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            
            print(f"✅ Line Protocol Mode: Received {len(lines)} measurements\n")
            
            for i, line in enumerate(lines):
                print(f"LP {i+1}: {line}")

        print(f"✅ Successfully processed {len(lines) if 'lines' in locals() else len(data)} items\n")
        
        return web.json_response({"status": "ok", "received": len(data) if 'data' in locals() else len(lines)})

    except Exception as e:
        print(f"❌ Error processing request: {e}")
        import traceback
        traceback.print_exc()
        return web.json_response({"status": "error", "message": str(e)}, status=400)


async def main():
    app = web.Application()
    app.router.add_post("/{path:.*}", handle_post)  # Accept any path (flexible for /write, etc.)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()
    
    print("🚀 Test HTTP Server running on http://localhost:8080")
    print("   Supports both JSON (default) and Line Protocol")
    print("   Ready for HttpPostSink testing...\n")
    
    try:
        await asyncio.Future()  # Run forever
    except asyncio.CancelledError:
        await runner.cleanup()
        print("Server stopped.")


if __name__ == "__main__":
    asyncio.run(main())