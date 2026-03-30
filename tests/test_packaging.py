from pathlib import Path

import daphne


def test_fd_endpoint_plugin_installed():
    plugin_path = (
        Path(daphne.__file__).parent / "twisted" / "plugins" / "fd_endpoint.py"
    )
    assert plugin_path.exists(), f"fd_endpoint.py not found at {plugin_path}"
