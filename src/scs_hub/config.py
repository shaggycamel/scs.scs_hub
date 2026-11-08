import os
from pathlib import Path

DEFAULT_CREDENTIALS_INI = Path.home() / ".config" / "scs_hub_credentials.ini"
ENV_CREDENTIALS_INI = "SCS_HUB_CREDENTIALS"


def credentials_path(explicit: str | None = None) -> str:
    """
    Resolve the credentials file, in order:
    explicit arg > SCS_HUB_CREDENTIALS env var > ~/.config default.

    The env var is what lets a container or CI point at a mounted secret
    (e.g. /run/secrets/credentials.ini) without changing caller code.
    """
    return (
        explicit or os.environ.get(ENV_CREDENTIALS_INI) or str(DEFAULT_CREDENTIALS_INI)
    )


def ensure_credentials_file(path: str) -> None:
    """Fail with a clear, actionable message instead of a silent empty parser."""
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Credentials file not found at {path}.\n"
            "Expected an INI file with sections like [postgres], [statyx], "
            "[espn_api], [yahoo_api]. Create it there, set "
            "SCS_HUB_CREDENTIALS to its path, or pass an explicit path "
            "when constructing SportsHub / Database / StatyxPipeline."
        )
