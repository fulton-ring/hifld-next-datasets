import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from dagster_hifld.conversion import (
    PMTilesGenerationError,
    _create_and_upload_pmtiles,
)

ZOOM_GUESS_ERROR = (
    "Can't guess maxzoom (-zg) without at least two distinct feature locations"
)


class PMTilesZoomFallbackTests(unittest.TestCase):
    def test_zoom_guess_error_retries_without_zg_and_uploads(self):
        storage = Mock(upload_file=AsyncMock(return_value="uploaded"))
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fgb_path = root / "chunk.fgb"
            fgb_path.write_bytes(b"fgb")
            output = root / "layer.pmtiles"
            commands = []

            def run(command, cwd, env):
                commands.append(command)
                if len(commands) == 1:
                    output.write_bytes(b"partial")
                    return 110, f"tippecanoe progress\n{ZOOM_GUESS_ERROR}\n"
                self.assertFalse(output.exists())
                output.write_bytes(b"pmtiles")
                return 0, ""

            with (
                patch(
                    "dagster_hifld.conversion._run_streaming_command",
                    side_effect=run,
                ),
                patch("dagster_hifld.conversion.logger.warning") as warning,
            ):
                result = asyncio.run(
                    _create_and_upload_pmtiles(
                        storage, [fgb_path], output, "dest/", "layer"
                    )
                )

        self.assertEqual(result, "uploaded")
        self.assertEqual(len(commands), 2)
        self.assertIn("-zg", commands[0])
        self.assertNotIn("-zg", commands[1])
        self.assertIn("--maximum-zoom=14", commands[1])
        warning.assert_called_once()
        storage.upload_file.assert_awaited_once()

    def test_zoom_fallback_failure_preserves_context_and_does_not_upload(self):
        storage = Mock(upload_file=AsyncMock())
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fgb_path = root / "chunk.fgb"
            fgb_path.write_bytes(b"1234")
            output = root / "layer.pmtiles"
            with (
                patch(
                    "dagster_hifld.conversion._run_streaming_command",
                    side_effect=[
                        (110, f"tippecanoe progress\n{ZOOM_GUESS_ERROR}\n"),
                        (1, "fallback detail"),
                    ],
                ) as run,
                self.assertRaisesRegex(
                    PMTilesGenerationError,
                    r"command=.*--maximum-zoom=14.*1 input.*4 bytes.*elapsed=.*"
                    r"exit code 1.*fallback detail",
                ),
            ):
                asyncio.run(
                    _create_and_upload_pmtiles(
                        storage, [fgb_path], output, "dest/", "layer"
                    )
                )

        self.assertEqual(run.call_count, 2)
        self.assertNotIn("-zg", run.call_args.args[0])
        storage.upload_file.assert_not_awaited()

    def test_unrelated_exit_110_is_not_retried(self):
        storage = Mock(upload_file=AsyncMock())
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fgb_path = root / "chunk.fgb"
            fgb_path.write_bytes(b"fgb")
            with (
                patch(
                    "dagster_hifld.conversion._run_streaming_command",
                    return_value=(110, f"prefix {ZOOM_GUESS_ERROR}"),
                ) as run,
                self.assertRaises(PMTilesGenerationError),
            ):
                asyncio.run(
                    _create_and_upload_pmtiles(
                        storage,
                        [fgb_path],
                        root / "layer.pmtiles",
                        "dest/",
                        "layer",
                    )
                )

        run.assert_called_once()
        storage.upload_file.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
