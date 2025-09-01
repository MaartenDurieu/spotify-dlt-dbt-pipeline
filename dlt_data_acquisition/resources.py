import dlt
from dlt.sources.helpers.rest_client import RESTClient
import time

def define_resources(client: RESTClient, max_items: int = None) -> list[dlt.resource]:
    """
    Returns a list of DLT resources using the provided client and max_items limit.
    
    By default, failing requests are retried up to 5 times with an exponentially increasing delay. 
    That means the first retry will wait 1 second, and the fifth retry will wait 16 seconds.
    """

    @dlt.resource(name="recently_played_tracks", write_disposition="replace")
    def recently_played_track_entries():
        total_tracks_fetched = 0

        for page in client.paginate("/me/player/recently-played"):
            for entry in page:
                yield entry
                total_tracks_fetched += 1
                time.sleep(1)  # To avoid hitting rate limits (I have gotten a 24/hr ban :))

                if max_items and total_tracks_fetched >= max_items:
                    return

    @dlt.transformer(data_from=recently_played_track_entries, name="albums", write_disposition={"disposition": "merge", "strategy": "scd2"})
    def albums(entry):
        album_id = entry["track"]["album"]["id"]
        album = client.get(f"/albums/{album_id}").json()
        yield album
        time.sleep(1)  # To avoid hitting rate limits (I have gotten a 24/hr ban :))

    @dlt.transformer(data_from=recently_played_track_entries, name="artists", write_disposition={"disposition": "merge", "strategy": "scd2"})
    def artists(entry):
        artists = entry["track"]["artists"]
        for artist in artists:
            artist_id = artist["id"]
            artist = client.get(f"/artists/{artist_id}").json()
            yield artist
            time.sleep(1)  # To avoid hitting rate limits (I have gotten a 24/hr ban :))

    return [recently_played_track_entries.add_limit, albums, artists]
