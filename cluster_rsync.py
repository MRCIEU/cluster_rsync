#!/bin/env python3.7
"""
A wrapper of rsync with cluster support.

Usage:

./cluster_rsync.py \
  --n-procs <8> --n-chunks <10> --chunk-id <0> \
  --source-dir <source-dir> --target-dir <target-dir>
"""
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Union
from loguru import logger
import click


def transfer_data(source_file: str, target_dir: str) -> None:
    return None


@click.command(help=__doc__)
@click.option("--source-dir", help="source directory", type=str)
@click.option("--target-dir", help="target directory", type=str)
@click.option(
    "-j", "--n-procs", default=8, help="Number of CPU processes to use"
)
@click.option(
    "--n-chunks", default=None, type=int, help="Number of chunks to use"
)
@click.option("--idx-chunks", default=None, type=int, help="Index of chunks")
@click.option("--rsync-args", default="-aLvzP", type=str, help="rsync args")
@click.option("-n", "--dryrun", is_flag=True, help="Dry run")
def main(
    source_dir: str,
    target_dir: str,
    n_procs: int = 1,
    n_chunks: Union[int, None] = None,
    idx_chunks: Union[int, None] = None,
    rsync_args: str = "-aLvzP",
    dryrun: bool = False,
) -> None:
    logger.info(
        f"""
        --source-dir {source_dir}
        --target-dir {target_dir}
        --n-procs {n_procs}
        --n-chunks {n_chunks}
        --idx-chunks {idx_chunks}
        --dryrun {dryrun}
    """
    )

    # Get sub files in source_dir
    # Get candidate files based on chunking

    # Deploy transfer
    if False:
        with Pool(n_procs) as pool:
            pool.map(partial(transfer_data), candidates)

    return None


if __name__ == "__main__":
    main()
