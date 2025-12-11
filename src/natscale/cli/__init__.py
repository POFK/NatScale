import typer

from .version import app as version_app
from .push import app as push_task

app = typer.Typer()

app.add_typer(version_app)
app.add_typer(push_task)


def main():
    app()


if __name__ == "__main__":
    main()
