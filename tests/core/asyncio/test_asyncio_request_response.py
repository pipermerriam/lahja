import asyncio

import pytest

from lahja import BaseEvent, BaseRequestResponseEvent


class Response(BaseEvent):
    def __init__(self, value):
        self.value = value


class Request(BaseRequestResponseEvent[Response]):
    def __init__(self, value):
        self.value = value

    @staticmethod
    def expected_response_type():
        return Response


@pytest.mark.asyncio
async def test_request(endpoint_pair):
    requester, responder = endpoint_pair

    async def do_responder():
        logging.info("IN do_responder")
        request = await responder.wait_for(Request)
        await responder.broadcast(Response(request.value), request.response_config())

    asyncio.ensure_future(do_responder())

    import logging

    logging.info("HERE")
    await requester.wait_until_all_connections_subscribed_to(Request)
    logging.info("THEN HERE")
    response = await requester.request(Request("test"))
    logging.info("AFTER")

    assert isinstance(response, Response)
    assert response.value == "test"
