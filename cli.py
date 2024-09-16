import sys
import uuid
import hashlib
import subprocess
import urllib.parse
from typing import Dict, Any, List
from pathlib import Path

import rich
import typer
from ruamel.yaml import YAML
from pydantic import BaseModel
from pydantic_settings import BaseSettings


app = typer.Typer()


class Settings(BaseSettings):
    endpoint_url: str = "https://uk1s3.embassy.ebi.ac.uk"
    output_bucket: str = "ebi-ngff-challenge-2024"
    config_fpath: Path = Path("ebi-ngff-images.yaml")
    v2_dpath: Path = Path("tmp/v2")
    v3_dpath: Path = Path("tmp/v3")


class ImageSettings(BaseModel):
    v2_s3_uri: str
    name: str
    description: str
    organism_id: str
    modality_id: str
    output_chunks: str = "1,1,1,256,256"
    output_shards: str = "1,1,1,2048,2048"


class ConversionConfig(BaseModel):
    images: Dict[str, ImageSettings]
    

settings = Settings()


def load_raw_config():
    yaml = YAML()
    with open(settings.config_fpath) as fh:
        raw_config = yaml.load(fh)

    return raw_config


def load_config():
    raw_config = load_raw_config()

    return ConversionConfig.parse_obj(raw_config)


def dict_to_uuid(my_dict: Dict[str, Any], attributes_to_consider: List[str]) -> str:
    """
    Create uuid from specific keys in a dictionary
    """

    seed = "".join([f"{my_dict[attr]}" for attr in attributes_to_consider])
    hexdigest = hashlib.md5(seed.encode("utf-8")).hexdigest()
    return str(uuid.UUID(version=4, hex=hexdigest))



def zarr_uri_to_s3_components(zarr_uri):

    def split_path(path):
        path_parts = Path(path).parts
        first = path_parts[0]
        rest = str(Path(*path_parts[1:]))
        return first, rest
    
    result = urllib.parse.urlparse(zarr_uri)

    endpoint_url = f"{result.scheme}://{result.netloc}"
    input_bucket, fragment = split_path(result.path[1:])

    return endpoint_url, input_bucket, fragment


def convert_local_v2_to_local_v3(v2_fpath, v3_fpath, image_config):
    command = (
        f"poetry run ome2024-ngff-challenge resave --cc-by"
        f" {v2_fpath}"
        f" {v3_fpath}" 
        f" --output-overwrite --output-shards={image_config.output_shards} --output-chunks={image_config.output_chunks}"
        f" --rocrate-organism={image_config.organism_id}"
        f" --rocrate-modality={image_config.modality_id}"
        f" --rocrate-name='{image_config.name}'"
        f" --rocrate-description='{image_config.description}'"
    )

    rich.print(f"Running: {command}")
    subprocess.run(command, shell=True)


def stage_from_s3_to_local(image_id, image_config):

    endpoint_url, input_bucket, fragment = zarr_uri_to_s3_components(image_config.v2_s3_uri)

    settings.v2_dpath.mkdir(exist_ok=True, parents=True)
    v2_fpath = f"{settings.v2_dpath}/{image_id}.zarr"
    command = f"aws --endpoint-url {endpoint_url} s3 sync s3://{input_bucket}/{fragment} {v2_fpath}"

    rich.print(f"Running {command}")
    subprocess.run(command, shell=True)


@app.command()
def update_file():
    config = load_config()

    yaml = YAML()

    yaml.dump(config.images, sys.stdout)


@app.command()
def list():
    config = load_config()
    validator_base_uri = "https://deploy-preview-36--ome-ngff-validator.netlify.app/?source="

    for image in config.images.values():
        attributes_to_consider = ["v2_s3_uri"]
        new_uuid = dict_to_uuid(image.__dict__, attributes_to_consider)
        # rich.print(f"{validator_base_uri}{settings.endpoint_url}/{settings.output_bucket}/{new_uuid}.zarr")
        rich.print(f"{settings.endpoint_url}/{settings.output_bucket}/{new_uuid}.zarr")


@app.command()
def process(image_id: str):
    config = load_config()
    image_config = config.images[image_id]

    stage_from_s3_to_local(image_id, image_config)

    attributes_to_consider = ["v2_s3_uri"]
    new_uuid = dict_to_uuid(config.images[image_id].__dict__, attributes_to_consider)

    v2_fpath = f"{settings.v2_dpath}/{image_id}.zarr"
    v3_fpath = settings.v3_dpath / f"{new_uuid}.zarr"
    rich.print(f"Checking {v3_fpath}")
    if not v3_fpath.exists():
        rich.print(f"Does not exist, running conversion")
        convert_local_v2_to_local_v3(v2_fpath, v3_fpath, image_config)

    command =f"aws --endpoint-url {settings.endpoint_url} s3 sync {v3_fpath} s3://{settings.output_bucket}/{new_uuid}.zarr --acl public-read"
    subprocess.run(command, shell=True)

    validator_base_uri = "https://deploy-preview-36--ome-ngff-validator.netlify.app/?source="
    rich.print(f"{validator_base_uri}{settings.endpoint_url}/{settings.output_bucket}/{new_uuid}.zarr")


if __name__ == "__main__":
    app()