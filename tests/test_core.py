import unittest

from pysandbox import _core


class CoreTests(unittest.IsolatedAsyncioTestCase):
    def test_protocol_version(self) -> None:
        self.assertEqual(_core.protocol_version(), 1)

    async def test_tokio_awaitable(self) -> None:
        self.assertIsNone(await _core.sleep(0))
