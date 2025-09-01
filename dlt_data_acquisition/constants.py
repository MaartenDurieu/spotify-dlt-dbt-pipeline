import dlt


SPOTIFY_BASE_URL = "https://api.spotify.com/v1"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
REDIRECT_URI = "http://127.0.0.1:8080/callback"
REQUIRED_API_SCOPE_PERMISSIONS = "user-read-private user-read-email"
PIPELINE_NAME = "spotify_dlt_scd2_pipeline"
PIPELINE_DEBUG_NAME = "spotify_dlt_scd2_pipeline_debug"
PIPELINE_DESTINATION = dlt.destinations.duckdb("output/dlt_spotify_data.duckdb")
PIPELINE_DEBUG_DESTINATION = dlt.destinations.duckdb("output/dlt_spotify_data_debug.duckdb")
DATASET_NAME = "spotify_data"
DATASET_DEBUG_NAME = "spotify_data_debug"
