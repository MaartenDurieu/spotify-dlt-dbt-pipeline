import tomli


def load_secrets() -> dict[str, str]:
    with open(".dlt/secrets.toml", "rb") as f:
        return tomli.load(f)
