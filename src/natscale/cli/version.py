import typer

from natscale.utils.helper import get_version_from_installed

app = typer.Typer()


@app.command()
def version():
    print(f"v{get_version_from_installed('natscale')}")
