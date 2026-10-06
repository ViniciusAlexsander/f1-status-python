import unittest
from types import SimpleNamespace

from fastapi import HTTPException

from api.routers.worker import require_worker_admin_token


class WorkerRouterTest(unittest.TestCase):
    def test_rejects_when_admin_token_is_not_configured(self) -> None:
        with self.assertRaises(HTTPException) as context:
            require_worker_admin_token(
                x_worker_admin_token="secret",
                settings=SimpleNamespace(worker_admin_token=None),
            )

        self.assertEqual(context.exception.status_code, 503)

    def test_rejects_invalid_admin_token(self) -> None:
        with self.assertRaises(HTTPException) as context:
            require_worker_admin_token(
                x_worker_admin_token="wrong",
                settings=SimpleNamespace(worker_admin_token="secret"),
            )

        self.assertEqual(context.exception.status_code, 403)

    def test_accepts_valid_admin_token(self) -> None:
        result = require_worker_admin_token(
            x_worker_admin_token="secret",
            settings=SimpleNamespace(worker_admin_token="secret"),
        )

        self.assertIsNone(result)
