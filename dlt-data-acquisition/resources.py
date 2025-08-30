from typing import Generator

import dlt
from dlt.sources.helpers.rest_client import RESTClient


def define_resources(client: RESTClient, max_items: int = None) -> list[dlt.resource]:
    """Returns a list of DLT resources using the provided client and max_items limit."""
    @dlt.resource(name="recently_played_tracks", max_table_nesting=0)
    def recently_played_tracks():
        total_tracks_fetched = 0
        for page in client.paginate("/me/player/recently-played"):
            for item in page:
                track = item["track"]
                yield track
                total_tracks_fetched += 1
                if max_items and total_tracks_fetched >= max_items:
                    return

    def fetch_unique_entities_from_recent_tracks(entity_type: str, id_getter: callable) -> Generator:
        seen_entities = set()
        for record in recently_played_tracks():
            for entity in id_getter(record):
                entity_id = entity["id"]
                if entity_id not in seen_entities:
                    seen_entities.add(entity_id)
                    yield client.get(f"{entity_type}/{entity_id}").json()

    @dlt.resource(name="tracks", max_table_nesting=1, write_disposition="replace")
    def tracks():
        yield from fetch_unique_entities_from_recent_tracks("/tracks", lambda r: [r])

    @dlt.resource(name="artists", max_table_nesting=1)
    def artists():
        yield from fetch_unique_entities_from_recent_tracks("/artists", lambda r: r["artists"])

    return [recently_played_tracks(), tracks(), artists()]
