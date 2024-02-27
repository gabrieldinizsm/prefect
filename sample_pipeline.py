from prefect import flow, task


@task(name="Doing something",
      retries=2)
def do_something():
    print("Doing something")


@task("doing another thing",
      retries=2)
def doing_another_thing():
    print("Doing another thing")
