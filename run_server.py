#!/usr/bin/env python3
import asyncio
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Import the main app creator
from main import create_app, bot, manager

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("🤖 Adjarabet Bot Server Starting...")
    print("=" * 60)
    print("📍 Server will run on: http://localhost:8000")
    print("📍 Press CTRL+C to stop")
    print("=" * 60)
    
    # Create app
    app = create_app()
    
    # Run server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
