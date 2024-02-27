from prefect import flow, task


@task(name="Doing something",
      retries=2,
      log_prints=True)
def do_something():
    print("Doing something")


@task(name="doing another thing",
      retries=2,
      log_prints=True)
def doing_another_thing():
    print("Doing another thing")


@flow
def dummy_flow():
    do_something()
    doing_another_thing()


if __name__ == '__main__':
    dummy_flow()
