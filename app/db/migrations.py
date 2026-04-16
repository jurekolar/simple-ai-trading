from app.config import get_settings
from app.db.models import create_session_factory


def run() -> None:
    settings = get_settings()
    create_session_factory(settings.database_url)


if __name__ == "__main__":
    run()
