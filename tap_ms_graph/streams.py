"""Stream type classes for tap-ms-graph."""

from tap_ms_graph.client import MSGraphStream
import hashlib


def md5(input: str) -> str:
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class SubscribedSkusStream(MSGraphStream):
    name = "subscribedSkus"
    path = "/subscribedSkus"
    primary_keys = ["id"]
    replication_key = None
    schema_filename = "subscribedSkus.json"


class UsersStream(MSGraphStream):
    name = "users"
    path = "/users"
    primary_keys = ["id"]
    replication_key = None
    schema_filename = "users.json"

    def get_child_context(self, record: dict, context) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "user_id": record["id"],
        }


class UserMessagesStream(MSGraphStream):
    name = "user_messages"
    parent_stream_type = UsersStream
    path = "/users/{user_id}/messages"
    primary_keys = ["id"]
    replication_key = None
    schema_filename = "user_messages.json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_notfound = False

    def get_records(self, context=None):
        if self.is_notfound:
            self.is_notfound = False
            return []
        return super().get_records(context)

    def validate_response(self, response) -> None:
        if response.status_code == 404:
            self.is_notfound = True
            return
        return super().validate_response(response)


class UserEventsStream(MSGraphStream):
    name = "user_events"
    parent_stream_type = UsersStream
    path = "/users/{user_id}/events"
    primary_keys = ["id"]
    replication_key = None
    schema_filename = "user_events.json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_notfound = False

    def get_records(self, context=None):
        if self.is_notfound:
            self.is_notfound = False
            return []
        return super().get_records(context)

    def post_process(self, row, context):
        if not row.get("attendees"):
            return row
        attendees = row.pop("attendees")
        for attendee in attendees:
            attendee["emailAddress"]["address"] = md5(attendee["emailAddress"]["address"].lower())
            attendee["emailAddress"].pop("name")
        row.update({"attendees": attendees})
        return row

    def validate_response(self, response) -> None:
        if response.status_code == 404:
            self.is_notfound = True
            return
        return super().validate_response(response)
