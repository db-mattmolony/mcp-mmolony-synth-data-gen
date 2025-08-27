import uvicorn
from custom_server.app import app


def main():
    uvicorn.run(
        "custom_server.app:app",  # import path to your `app`
        host="0.0.0.0",
        port=8002,
        reload=True,  # optional
    )
