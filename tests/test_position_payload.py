import base64
import json
import unittest
import zlib

from api.domain.position_payload import compact_position_payload, decode_z_payload


def _raw_deflate(payload: dict) -> str:
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    raw = compressor.compress(json.dumps(payload).encode("utf-8"))
    raw += compressor.flush()
    return base64.b64encode(raw).decode("ascii")


POSITION = {
    "Position": [
        {
            "Timestamp": "2026-11-08T18:00:00.000Z",
            "Entries": {
                "16": {"Status": "OnTrack", "X": 10, "Y": 20, "Z": 1},
            },
        },
        {
            "Timestamp": "2026-11-08T18:00:00.220Z",
            "Entries": {
                "16": {"Status": "OnTrack", "X": 11, "Y": 21, "Z": 1},
                "1": {"Status": "OffTrack", "X": 5, "Y": 6, "Z": 0},
            },
        },
    ]
}


class PositionPayloadTest(unittest.TestCase):
    def test_decodes_raw_deflate_base64_string(self) -> None:
        self.assertEqual(decode_z_payload(_raw_deflate(POSITION)), POSITION)

    def test_decodes_json_encoded_compressed_string(self) -> None:
        encoded = json.dumps(_raw_deflate(POSITION))
        self.assertEqual(decode_z_payload(encoded), POSITION)

    def test_passes_through_already_decoded_object(self) -> None:
        self.assertEqual(decode_z_payload(POSITION), POSITION)

    def test_compacts_last_sample_per_batch(self) -> None:
        compact = compact_position_payload(_raw_deflate(POSITION))
        self.assertEqual(
            compact,
            {
                "timestamp": "2026-11-08T18:00:00.220Z",
                "cars": {
                    "16": {"x": 11, "y": 21, "status": "OnTrack"},
                    "1": {"x": 5, "y": 6, "status": "OffTrack"},
                },
            },
        )

    def test_compact_returns_none_for_invalid_payload(self) -> None:
        self.assertIsNone(compact_position_payload("%%%"))
        self.assertIsNone(compact_position_payload({"Position": []}))


if __name__ == "__main__":
    unittest.main()
