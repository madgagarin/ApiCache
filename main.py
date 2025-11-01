from asyncio import (
    new_event_loop,
    set_event_loop,
    all_tasks,
    AbstractEventLoop,
)
from logging import warning
from signal import SIGHUP, SIGTERM
from aiohttp.web import get, post, Application, run_app


from routers import (
    health_router,
    get_update_router,
    post_update_router,
    post_form_data_router,
    get_data_router,
)


class AioHttpAppException(BaseException):
    """A base exception for application-specific errors."""


class GracefulExitException(AioHttpAppException):
    """Exception raised to request a graceful application exit."""


class ResetException(AioHttpAppException):
    """Exception raised to request an application reset (e.g., on SIGHUP)."""


def handle_sighup() -> None:
    """Signal handler for SIGHUP to trigger a configuration reload."""
    warning("Received SIGHUP")
    raise ResetException("Application reset requested via SIGHUP")


def handle_sigterm() -> None:
    """Signal handler for SIGTERM to trigger a graceful shutdown."""
    warning("Received SIGTERM")
    raise GracefulExitException("Application exit requested via SIGTERM")


def cancel_tasks(loop: AbstractEventLoop) -> None:
    """Cancel all running tasks in the event loop."""
    for task in all_tasks(loop=loop):
        task.cancel()


def new_loop() -> AbstractEventLoop:
    """Create a new event loop with signal handlers configured."""
    loop = new_event_loop()
    loop.add_signal_handler(SIGHUP, handle_sighup)
    loop.add_signal_handler(SIGTERM, handle_sigterm)
    return loop


def run() -> bool:
    """
    Set up and run the aiohttp application.

    This function initializes the event loop, configures the application routes,
    and handles graceful shutdown and restart signals.

    Returns:
        bool: True if the application should be restarted, False otherwise.
    """
    loop = new_loop()
    set_event_loop(loop)

    # Initialize the aiohttp application
    app = Application()

    # Register application routes
    app.add_routes(
        [
            get("/health", health_router),
            get("/", get_data_router),
            get("/{search_text}", get_data_router),
            post("/", post_form_data_router),
            get("/update", get_update_router),
            post("/update", post_update_router),
        ]
    )

    try:
        # Run the application
        run_app(app)
    except ResetException:
        warning("Reloading...")
        cancel_tasks(loop)
        loop = new_loop()
        set_event_loop(loop)
        return True  # Signal that the app should restart
    except GracefulExitException:
        warning("Exiting...")
        cancel_tasks(loop)
        loop.close()
    return False  # Signal that the app should not restart


def main() -> None:
    """
    Main entry point for the application.
    Runs the application in a loop to allow for restarts.
    """
    while run():
        pass


if __name__ == "__main__":
    main()
