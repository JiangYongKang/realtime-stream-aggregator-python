"""Local entrypoint: ``uv run python main.py`` then see /docs."""

import os

import uvicorn

from streamagg.api import create_app
from streamagg.config import Config

app = create_app(Config(
    data_dir=os.environ.get("STREAMAGG_DATA_DIR", "./data"),
    queue_capacity=int(os.environ.get("STREAMAGG_QUEUE_CAPACITY", "1000")),
))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
