from concurrent.futures import ThreadPoolExecutor, as_completed


class ParallelResourceRunner:
    """
    Runs independent HubSpot resources concurrently.
    """

    def __init__(self, max_workers=4):
        self.max_workers = max_workers

    def run(self, resources, worker):
        results = {}

        with ThreadPoolExecutor(
            max_workers=min(self.max_workers, len(resources))
        ) as executor:

            futures = {
                executor.submit(worker, resource): resource
                for resource in resources
            }

            for future in as_completed(futures):
                resource = futures[future]

                try:
                    results[resource] = {
                        "status": "completed",
                        "result": future.result(),
                    }

                except Exception as exc:
                    results[resource] = {
                        "status": "failed",
                        "error": str(exc),
                    }

        return results
