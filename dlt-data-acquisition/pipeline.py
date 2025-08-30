import dlt
from api_client import create_spotify_client
from resources import define_resources


def main(max_items: int, destination: str) -> None:
    client = create_spotify_client()
    resources = define_resources(client, max_items=max_items)

    pipeline = dlt.pipeline(
        pipeline_name="spotify_dlt_pipeline",
        destination=destination,
        dataset_name="spotify_data",
        progress="tqdm",
    )
    pipeline.run(resources)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--max_items", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    # When debugging, after this has ran, you can check the locally generated DuckDB database for the ingested data.
    # Dlt comes with a premade streamlit application for exploring the ingested data accessed with `dlt <pipeline_name> show`
    destination = "duckdb" if not args.debug else "databricks"
    main(max_items=args.max_items, destination=destination)
