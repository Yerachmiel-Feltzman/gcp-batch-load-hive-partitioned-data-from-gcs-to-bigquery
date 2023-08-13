import os

from ingestion import config
from ingestion.app import app


def main():
    import uvicorn

    server_port = int(os.environ.get("PORT", 8000))
    print(__name__)
    # NOTE: Note that the application instance itself can be passed instead of the app import string.
    # However, this style only works if you are not using multiprocessing (workers=NUM)
    # or reloading (reload=True), so we recommend using the import string style.
    uvicorn.run(app, host="0.0.0.0", port=server_port, log_level="info")


if __name__ == "__main__":
    config.set_log_level_from_env(force=False)
    main()
