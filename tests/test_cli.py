import logging
import os
import sys
from argparse import ArgumentError
from unittest import TestCase, skipUnless
from unittest.mock import mock_open, patch

from daphne.access import AccessLogGenerator
from daphne.cli import CommandLineInterface
from daphne.endpoints import build_endpoint_description_strings as build


class TestEndpointDescriptions(TestCase):
    """
    Tests that the endpoint parsing/generation works as intended.
    """

    def testBasics(self):
        self.assertEqual(build(), [], msg="Empty list returned when no kwargs given")

    def testTcpPortBindings(self):
        self.assertEqual(
            build(port=1234, host="example.com"),
            ["tcp:port=1234:interface=example.com"],
        )

        self.assertEqual(
            build(port=8000, host="127.0.0.1"), ["tcp:port=8000:interface=127.0.0.1"]
        )

        self.assertEqual(
            build(port=8000, host="[200a::1]"), [r"tcp:port=8000:interface=200a\:\:1"]
        )

        self.assertEqual(
            build(port=8000, host="200a::1"), [r"tcp:port=8000:interface=200a\:\:1"]
        )

        # incomplete port/host kwargs raise errors
        self.assertRaises(ValueError, build, port=123)
        self.assertRaises(ValueError, build, host="example.com")

    def testUnixSocketBinding(self):
        self.assertEqual(
            build(unix_socket="/tmp/daphne.sock"), ["unix:/tmp/daphne.sock"]
        )

    def testFileDescriptorBinding(self):
        self.assertEqual(build(file_descriptor=5), ["fd:fileno=5"])

    def testMultipleEnpoints(self):
        self.assertEqual(
            sorted(
                build(
                    file_descriptor=123,
                    unix_socket="/tmp/daphne.sock",
                    port=8080,
                    host="10.0.0.1",
                )
            ),
            sorted(
                [
                    "tcp:port=8080:interface=10.0.0.1",
                    "unix:/tmp/daphne.sock",
                    "fd:fileno=123",
                ]
            ),
        )


class TestCLIInterface(TestCase):
    """
    Tests the overall CLI class.
    """

    class TestedCLI(CommandLineInterface):
        """
        CommandLineInterface subclass that we used for testing (has a fake
        server subclass).
        """

        class TestedServer:
            """
            Mock server object for testing.
            """

            abort_start = False

            def __init__(self, **kwargs):
                self.init_kwargs = kwargs

            def run(self):
                pass

        server_class = TestedServer

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def assertCLI(self, args, server_kwargs):
        """
        Asserts that the CLI class passes the right args to the server class.
        Passes in a fake application automatically.
        """
        cli = self.TestedCLI()
        cli.run(
            args + ["daphne:__version__"]
        )  # We just pass something importable as app
        # Check the server got all arguments as intended
        for key, value in server_kwargs.items():
            # Get the value and sort it if it's a list (for endpoint checking)
            actual_value = cli.server.init_kwargs.get(key)
            if isinstance(actual_value, list):
                actual_value.sort()
            # Check values
            self.assertEqual(
                value,
                actual_value,
                "Wrong value for server kwarg %s: %r != %r"
                % (key, value, actual_value),
            )

    def testCLIBasics(self):
        """
        Tests basic endpoint generation.
        """
        self.assertCLI([], {"endpoints": ["tcp:port=8000:interface=127.0.0.1"]})
        self.assertCLI(
            ["-p", "123"], {"endpoints": ["tcp:port=123:interface=127.0.0.1"]}
        )
        self.assertCLI(
            ["-b", "10.0.0.1"], {"endpoints": ["tcp:port=8000:interface=10.0.0.1"]}
        )
        self.assertCLI(
            ["-b", "200a::1"], {"endpoints": [r"tcp:port=8000:interface=200a\:\:1"]}
        )
        self.assertCLI(
            ["-b", "[200a::1]"], {"endpoints": [r"tcp:port=8000:interface=200a\:\:1"]}
        )
        self.assertCLI(
            ["-p", "8080", "-b", "example.com"],
            {"endpoints": ["tcp:port=8080:interface=example.com"]},
        )

    def testUnixSockets(self):
        self.assertCLI(
            ["-p", "8080", "-u", "/tmp/daphne.sock"],
            {
                "endpoints": [
                    "tcp:port=8080:interface=127.0.0.1",
                    "unix:/tmp/daphne.sock",
                ]
            },
        )
        self.assertCLI(
            ["-b", "example.com", "-u", "/tmp/daphne.sock"],
            {
                "endpoints": [
                    "tcp:port=8000:interface=example.com",
                    "unix:/tmp/daphne.sock",
                ]
            },
        )
        self.assertCLI(
            ["-u", "/tmp/daphne.sock", "--fd", "5"],
            {"endpoints": ["fd:fileno=5", "unix:/tmp/daphne.sock"]},
        )

    def testMixedCLIEndpointCreation(self):
        """
        Tests mixing the shortcut options with the endpoint string options.
        """
        self.assertCLI(
            ["-p", "8080", "-e", "unix:/tmp/daphne.sock"],
            {
                "endpoints": [
                    "tcp:port=8080:interface=127.0.0.1",
                    "unix:/tmp/daphne.sock",
                ]
            },
        )
        self.assertCLI(
            ["-p", "8080", "-e", "tcp:port=8080:interface=127.0.0.1"],
            {
                "endpoints": [
                    "tcp:port=8080:interface=127.0.0.1",
                    "tcp:port=8080:interface=127.0.0.1",
                ]
            },
        )

    def testCustomEndpoints(self):
        """
        Tests entirely custom endpoints
        """
        self.assertCLI(["-e", "imap:"], {"endpoints": ["imap:"]})

    def test_default_proxyheaders(self):
        """
        Passing `--proxy-headers` without a parameter will use the
        `X-Forwarded-For` header.
        """
        self.assertCLI(
            ["--proxy-headers"], {"proxy_forwarded_address_header": "X-Forwarded-For"}
        )

    def test_custom_proxyhost(self):
        """
        Passing `--proxy-headers-host` will set the used host header to
        the passed one, and `--proxy-headers` is mandatory.
        """
        self.assertCLI(
            ["--proxy-headers", "--proxy-headers-host", "blah"],
            {"proxy_forwarded_address_header": "blah"},
        )
        with self.assertRaises(expected_exception=ArgumentError) as exc:
            self.assertCLI(
                ["--proxy-headers-host", "blah"],
                {"proxy_forwarded_address_header": "blah"},
            )
        self.assertEqual(exc.exception.argument_name, "--proxy-headers-host")
        self.assertEqual(
            exc.exception.message,
            "--proxy-headers has to be passed for this parameter.",
        )

    def test_custom_proxyport(self):
        """
        Passing `--proxy-headers-port` will set the used port header to
        the passed one, and `--proxy-headers` is mandatory.
        """
        self.assertCLI(
            ["--proxy-headers", "--proxy-headers-port", "blah2"],
            {"proxy_forwarded_port_header": "blah2"},
        )
        with self.assertRaises(expected_exception=ArgumentError) as exc:
            self.assertCLI(
                ["--proxy-headers-port", "blah2"],
                {"proxy_forwarded_address_header": "blah2"},
            )
        self.assertEqual(exc.exception.argument_name, "--proxy-headers-port")
        self.assertEqual(
            exc.exception.message,
            "--proxy-headers has to be passed for this parameter.",
        )

    def test_custom_servername(self):
        """
        Passing `--server-name` will set the default server header
        from 'daphne' to the passed one.
        """
        self.assertCLI([], {"server_name": "daphne"})
        self.assertCLI(["--server-name", ""], {"server_name": ""})
        self.assertCLI(["--server-name", "python"], {"server_name": "python"})

    def test_no_servername(self):
        """
        Passing `--no-server-name` will set server name to '' (empty string)
        """
        self.assertCLI(["--no-server-name"], {"server_name": ""})

    def test_lifespan_shutdown_timeout(self):
        """
        Tests that --lifespan-shutdown-timeout is passed through to the server.
        The default is 30 seconds; a custom value overrides it.
        """
        self.assertCLI([], {"lifespan_shutdown_timeout": 30})
        self.assertCLI(
            ["--lifespan-shutdown-timeout", "60"],
            {"lifespan_shutdown_timeout": 60},
        )

    def test_entrypoint(self):
        """
        Tests that entrypoint() creates an instance and calls run() with
        sys.argv[1:], i.e. strips the program name before dispatching.
        """
        argv = ["daphne", "daphne:__version__"]
        with patch("sys.argv", argv):
            with patch.object(self.TestedCLI, "run") as mock_run:
                self.TestedCLI.entrypoint()
                mock_run.assert_called_once_with(["daphne:__version__"])

    def test_access_log_stdout(self):
        """
        Passing --access-log - routes the access log to stdout.
        """
        cli = self.TestedCLI()
        cli.run(["--access-log", "-", "daphne:__version__"])
        action_logger = cli.server.init_kwargs.get("action_logger")
        self.assertIsInstance(action_logger, AccessLogGenerator)
        self.assertIs(action_logger.stream, sys.stdout)

    def test_access_log_file(self):
        """
        Passing --access-log with a file path opens that file for appending.
        """
        with patch("builtins.open", mock_open()) as mocked_open:
            cli = self.TestedCLI()
            cli.run(["--access-log", "/tmp/access.log", "daphne:__version__"])
            mocked_open.assert_called_once_with("/tmp/access.log", "a", 1)
            action_logger = cli.server.init_kwargs.get("action_logger")
            self.assertIsInstance(action_logger, AccessLogGenerator)

    def test_abort_start_exits(self):
        """
        If the server sets abort_start after run(), the CLI exits with code 1.
        """
        with patch.object(self.TestedCLI.TestedServer, "abort_start", new=True):
            with self.assertRaises(SystemExit) as cm:
                self.TestedCLI().run(["daphne:__version__"])
            self.assertEqual(cm.exception.code, 1)


@skipUnless(os.getenv("ASGI_THREADS"), "ASGI_THREADS environment variable not set.")
class TestASGIThreads(TestCase):
    def test_default_executor(self):
        from daphne.server import twisted_loop

        executor = twisted_loop._default_executor
        self.assertEqual(executor._max_workers, int(os.getenv("ASGI_THREADS")))
