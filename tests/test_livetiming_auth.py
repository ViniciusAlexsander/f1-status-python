import tempfile
import unittest
from pathlib import Path

from api.domain.livetiming_auth import LivetimingAuthProvider


class LivetimingAuthProviderTest(unittest.TestCase):
    def test_missing_token_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            auth_file = Path(temp_dir) / "missing-token.txt"
            provider = LivetimingAuthProvider(auth_file=auth_file)

            self.assertIsNone(provider.get_auth_token())
