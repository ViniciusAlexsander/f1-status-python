from pathlib import Path

JWKS_URL = "https://api.formula1.com/static/jwks.json"


class LivetimingAuthError(RuntimeError):
    pass


class LivetimingAuthProvider:
    def __init__(
        self,
        subscription_token: str | None = None,
        auth_file: Path | None = None,
    ) -> None:
        self._subscription_token = (
            subscription_token.strip() if subscription_token else None
        )
        self._auth_file = auth_file

    def get_auth_token(self) -> str | None:
        if self._subscription_token:
            self._verify_or_raise(self._subscription_token)
            return self._subscription_token

        token = self._read_token_file()

        if not token:
            return None

        self._verify_or_raise(token)
        self._subscription_token = token

        return token

    def _read_token_file(self) -> str | None:
        auth_file = self._auth_file or _default_auth_data_file()

        if not auth_file.exists():
            return None

        token = auth_file.read_text(encoding="utf-8").strip()

        return token or None

    def _verify_or_raise(self, token: str) -> None:
        try:
            _verify(token)
        except Exception as exc:
            raise LivetimingAuthError("Invalid Formula 1 subscription token.") from exc


def _default_auth_data_file() -> Path:
    try:
        import platformdirs
    except ModuleNotFoundError:
        return Path.home() / ".f1-status-python" / "formula1-token.txt"

    return Path(platformdirs.user_data_dir("f1-status-python")) / "formula1-token.txt"


def _get_jwk(jwks_uri, kid):
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise LivetimingAuthError(
            "requests is required for live timing auth. "
            "Install project requirements before using live timing endpoints."
        ) from exc

    keys = requests.get(jwks_uri, timeout=10).json()["keys"]

    for key in keys:
        if key["kid"] == kid:
            return key

    raise RuntimeError("Key not found")


def _verify(token):
    try:
        import jwt
        from jwt.algorithms import RSAAlgorithm
    except ModuleNotFoundError as exc:
        raise LivetimingAuthError(
            "PyJWT is required for live timing auth. "
            "Install project requirements before using live timing endpoints."
        ) from exc

    header = jwt.get_unverified_header(token)

    jwk = _get_jwk(JWKS_URL, header["kid"])

    public_key = RSAAlgorithm.from_jwk(jwk)

    return jwt.decode(
        token,
        key=public_key,
        algorithms=["RS256"],
        options={"verify_aud": False},
    )
