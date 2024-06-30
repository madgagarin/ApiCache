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
    """An exception specific to the AioHttp application."""


class GracefulExitException(AioHttpAppException):
    """Exception raised when an application exit is requested."""


class ResetException(AioHttpAppException):
    """Exception raised when an application reset is requested."""


def handle_sighup() -> None:
    warning("Received SIGHUP")
    raise ResetException("Application reset requested via SIGHUP")


def handle_sigterm() -> None:
    warning("Received SIGTERM")
    raise GracefulExitException("Application exit requested via SIGTERM")


def cancel_tasks(loop: AbstractEventLoop) -> None:
    for task in all_tasks(loop=loop):
        task.cancel()


def new_loop() -> AbstractEventLoop:
    loop = new_event_loop()
    loop.add_signal_handler(SIGHUP, handle_sighup)
    loop.add_signal_handler(SIGTERM, handle_sigterm)
    return loop


def run() -> bool:
    """
    Launch the application.
    Returns whether the application should be restarted or not.
    """
    loop = new_loop()
    set_event_loop(loop)
    app = Application()
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
        run_app(app)
    except ResetException:
        warning("Reloading...")
        cancel_tasks(loop)
        loop = new_loop()
        set_event_loop(loop)
        return True
    except GracefulExitException:
        warning("Exiting...")
        cancel_tasks(loop)
        loop.close()
    return False


def main() -> None:
    while run():
        pass


if __name__ == "__main__":
    main()
