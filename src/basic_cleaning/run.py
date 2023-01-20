#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################

    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info('Data download completed')

    df = pd.read_csv(artifact_local_path)
    logger.info('Data loading completed')

    # Drop outliers
    min_price, max_price = args.min_price, args.max_price
    # idx = df['price'].between(min_price, max_price)
    # df = df[idx].copy()
    df = df.loc[(df['price'].between(min_price, max_price)) & (df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2))]
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f'Minimum price:{df.price.min()}, Maximum price:{df.price.max()}')
    logger.info('DataFrame type infos')
    logger.info(df.info())

    df.to_csv("clean_sample.csv", index=False)
    logger.info('Data save completed')

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
 
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    logger.info('Data upload completed')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help='Name of the input artifact',## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help='Name of the output artifact',## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help='Type of the output artifact',## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help='Describe the output artifact',## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,## INSERT TYPE HERE: str, float or int,
        help='Minimum price allowed',## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,## INSERT TYPE HERE: str, float or int,
        help='Maximum price allowed',## INSERT DESCRIPTION HERE,
        required=True
    )


    args = parser.parse_args()

    go(args)
