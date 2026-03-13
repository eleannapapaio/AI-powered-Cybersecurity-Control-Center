import json
import time

from openai import OpenAI

from langgraph_pipeline import validate_and_store

client = OpenAI()

POLL_INTERVAL = 30


def fetch_results(batch_id):

    batch = client.batches.retrieve(batch_id)

    if batch.status != "completed":
        return None

    file = client.files.content(batch.output_file_id)

    results = []

    for line in file.text.splitlines():
        results.append(json.loads(line))

    return results


def run(batch_ids):

    while True:

        for batch_id in list(batch_ids):

            results = fetch_results(batch_id)

            if not results:
                continue

            validate_and_store(results)

            batch_ids.remove(batch_id)

        time.sleep(POLL_INTERVAL)