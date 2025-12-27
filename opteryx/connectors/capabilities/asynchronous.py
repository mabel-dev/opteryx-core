# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


class Asynchronous:
    def __init__(self, **kwargs):
        pass

    async def async_read_blob(self, *, blob_name, pool, telemetry, **kwargs):
        """Require connectors to implement their own async_read_blob.

        Connectors should commit bytes into the provided AsyncMemoryPool and return
        a pool reference. If a connector cannot support async reads it should not
        advertise the Asynchronous capability.
        """
        raise NotImplementedError(
            "Connector must implement async_read_blob(blob_name, pool, telemetry, **kwargs)"
        )
