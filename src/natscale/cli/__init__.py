import typer

from .version import app as version_app

app = typer.Typer()

app.add_typer(version_app)


def main():
    app()


if __name__ == "__main__":
    main()
