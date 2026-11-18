"""Loopback integration tests for the Steel client. Python standard library only."""

import json
import socket
import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("test-lab-client.scm").resolve()
TIMEOUT = 10


def frames(*messages):
    return "".join(json.dumps(message) + "\n" for message in messages).encode()


def measurement(value):
    return {"event": "measurement", "status": "ok", "key": "meter/power", "value": value}


class SteelClientTests(unittest.TestCase):
    def run_client(self, scenario, serve, *args):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(TIMEOUT)
            address = f"127.0.0.1:{listener.getsockname()[1]}"
            with subprocess.Popen(
                ["steel", str(SCRIPT), address, scenario, *args],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            ) as client:
                try:
                    connection, _ = listener.accept()
                    with connection, connection.makefile("rb") as requests:
                        connection.settimeout(TIMEOUT)
                        serve(connection, requests)
                        self.assertEqual(requests.readline(), b"", "client sent an extra request")
                    output, _ = client.communicate(timeout=TIMEOUT)
                except Exception as exc:
                    if client.poll() is None:
                        client.kill()
                    output, _ = client.communicate()
                    raise AssertionError(f"Steel client did not finish the scenario:\n{output}") from exc
                self.assertEqual(client.returncode, 0, output)
                self.assertIn("Steel TCP scenario passed", output)

    def test_interleaves_get_set_and_measurements_in_fifo_order(self):
        def serve(connection, requests):
            self.assertEqual(json.loads(requests.readline()), {
                "id": 1, "op": "subscribe", "key": "meter/температура",
            })
            connection.sendall(frames({"id": 1, "status": "ok"}, measurement(11)))
            self.assertEqual(json.loads(requests.readline()), {
                "id": 2, "op": "get", "key": "meter/power",
            })
            connection.sendall(frames(measurement(12), measurement(13), {
                "id": 2, "status": "ok", "value": 99,
            }))
            self.assertEqual(json.loads(requests.readline()), {
                "id": 2**64 - 1, "op": "set", "key": "meter/power", "value": 7,
            })
            connection.sendall(frames({
                "event": "measurement", "status": "error", "key": "meter/power",
                "error": "sensor error",
            }, {
                "data": {"id": 4, "text": '\\"[id]'}, "id": 2**64 - 1, "status": "ok",
            }, measurement(15)))

        self.run_client("interleaved", serve)

    def test_fatal_frames_close_connection_without_resending_set(self):
        for payload, error in [
            (frames({"id": 9007199254740993, "status": "ok"}), "unexpected response id"),
            (b'{"id":9007199254740992,"id":9007199254740993,"status":"ok"}\n',
             "unexpected response id"),
            (frames({"status": "error", "error": "duplicate_id"}), "duplicate_id"),
            (frames({"status": "error", "error": "observation_data_lost"}), "observation_data_lost"),
            (b"", "unexpected EOF"),
            (b'{"id":9007199254740992,"status":"ok"}', "EOF before"),
            (b"{broken JSON\n", "invalid JSON frame"),
        ]:
            with self.subTest(error=error):
                def serve(connection, requests):
                    self.assertEqual(json.loads(requests.readline()), {
                        "id": 9007199254740992, "op": "set", "key": "meter/power", "value": 1,
                    })
                    if payload:
                        connection.sendall(payload)
                    if error in ("unexpected EOF", "EOF before"):
                        connection.shutdown(socket.SHUT_WR)

                self.run_client("failure", serve, error)

    def test_overflow_is_explicit_and_preserves_queued_prefix(self):
        def serve(connection, requests):
            self.assertEqual(json.loads(requests.readline())["op"], "set")
            connection.sendall(frames(*(measurement(value) for value in range(257))))

        self.run_client("overflow", serve)

    def test_receive_reports_lost_stream(self):
        def serve(connection, requests):
            self.assertEqual(json.loads(requests.readline())["op"], "subscribe")
            connection.sendall(frames(
                {"id": 1, "status": "ok"},
                {"status": "error", "error": "observation_data_lost"},
            ))

        self.run_client("subscription-error", serve)

    def test_matched_set_error_is_returned_without_retry(self):
        def serve(connection, requests):
            self.assertEqual(json.loads(requests.readline()), {
                "id": 1, "op": "set", "key": "meter/power", "value": 7,
            })
            connection.sendall(frames({"id": 1, "status": "error", "error": "outcome_unknown"}))
            self.assertEqual(json.loads(requests.readline()), {
                "id": 2, "op": "get", "key": "meter/power",
            })
            connection.sendall(frames({"id": 2, "status": "ok", "value": 99}))

        self.run_client("matched-error", serve)


if __name__ == "__main__":
    unittest.main()
