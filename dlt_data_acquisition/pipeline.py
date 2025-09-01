import dlt
from api_client import create_spotify_client
from resources import define_resources

from constants import DATASET_DEBUG_NAME, DATASET_NAME, PIPELINE_DEBUG_DESTINATION, PIPELINE_DEBUG_NAME, PIPELINE_DESTINATION, PIPELINE_NAME


def main(max_items: int, debug: bool) -> None:
    client = create_spotify_client()
    resources = define_resources(client, max_items=max_items)

    pipeline_name = PIPELINE_DEBUG_NAME if debug else PIPELINE_NAME
    destination = PIPELINE_DEBUG_DESTINATION if debug else PIPELINE_DESTINATION
    dataset_name = DATASET_DEBUG_NAME if debug else DATASET_NAME

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="tqdm",
    )
    pipeline.run(resources)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--max_items", type=int, default=40)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    # After this has ran, you can check the locally generated DuckDB database for the ingested data.
    # Dlt comes with a premade streamlit application for exploring the ingested data
    # It accesses the duckdb database and can be accessed with `dlt pipeline <pipeline_name> show`
    main(max_items=args.max_items, debug=args.debug)
