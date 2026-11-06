"""Page-button entrypoint. Refreshes the series in REFRESH_REQUEST_ID's request.
Started on its own short-lived machine by web/server.py."""
from econ_data.refresh import main


if __name__ == "__main__":
    main()
