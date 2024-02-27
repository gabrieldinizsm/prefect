from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/gabrieldinizsm/prefect.git",
        entrypoint="app.py:main",
    ).deploy(
        name="first-deployment",
        work_pool_name="my-managed-pool",
        cron="0 1 * * *",
    )

    flow.from_source(
        source="https://github.com/gabrieldinizsm/prefect.git",
        entrypoint="sample_pipeline.py:dummy_flow",
    ).deploy(
        name="second-deployment",
        work_pool_name="my-managed-pool"
    )
