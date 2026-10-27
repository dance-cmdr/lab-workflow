"""A real driver must echo the command attempt on success and failure."""

import asyncio
import json

import pytest

from services.worker.main import Driver


class CapturingConnection:
    def __init__(self):
        self.results = []
        self.published = asyncio.Event()

    async def publish(self, subject, payload):
        self.results.append(json.loads(payload))
        self.published.set()


class CommandMessage:
    def __init__(self, data):
        self.data = json.dumps(data).encode()
        self.responses = []

    async def respond(self, payload):
        self.responses.append(json.loads(payload))


@pytest.mark.parametrize("fail_pct", [0, 100])
async def test_driver_echoes_attempt_identifier(fail_pct):
    connection = CapturingConnection()
    driver = Driver("incubator-1", connection, 0, 0, fail_pct, True)
    command = CommandMessage(
        {
            "run_id": "run-contract",
            "step_id": "step-contract",
            "step_name": "incubate",
            "device_id": "incubator-1",
            "attempt_id": "attempt-second",
        }
    )
    await driver.handle_command(command)
    await asyncio.wait_for(connection.published.wait(), timeout=1)
    assert command.responses[0]["accepted"] is True
    assert connection.results[0]["attempt_id"] == "attempt-second"
    assert bool(connection.results[0]["error"]) == bool(fail_pct)
    assert driver.state.busy is False
