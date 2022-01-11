"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact
"""
import argparse
import logging
import os
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO,format ="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def go(args):
    """
    Basic Cleaning Procedure
    
    """
    with wandb.init(job_type='basic_cleaning') as run:
        run.config.update(args)
        
        artifact_local_path = run.use_artifact(args.input_artifact).file()
        
        df=pd.read_csv(artifact_local_path,index_col="id")
        min_price = args.min_price
        max_price = args.max_price
        ix = df['price'].between(min_price,max_price)
        df = df[ix].copy()
        logger.info("Dataframe created with the range of these prices:%s - %s",
                                               args.min_price, args.max_price)
        
        df['last_review'] = pd.to_datetime(df['last_review'])
        logger.info("Dataframe column last_review changed to datetime")
        
        ix = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
        df = df[ix].copy()
        
        logger.info("Dataframe outliers for latitude and longitude removed")
        
        tmp_path = os.path.join(args.temp_directory,args.output_artifact)
        df.to_csv(tmp_path)
        
        logging.info("artifact saved at the temporary location %s" , temp_path)
        
        artifact = wandb.Artifact(args.output_artifact,
                                 type = args.artifact_type,
                                 description = args.output_description,
                                 )
        artifact.add_file(temp_path)
        run.log_artifact(artifact)
        artifact.wait()
        logger.info("Data is cleaned and uploaded to wandb")
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = "data cleaning")
    
    parser.add_argument(
        "--temp_directory",
        type = str,
        help = 'temporary location to save artifacts',
        required = True)
    
    parser.add_argument(
            "--input_artifact",
            type = str,
            help = "Input artifact for the component",
            required = True
    )
    
    parser.add_argument(
            "--output_artifact",
            type = str,
            help = "output artifact for the component",
            required = True)
    
    parser.add_argument(
            "--output_type",
            type = str,
            help = "output artifact type for the component",
            required = True)
    
    parser.add_argument(
            "--min_price",
            type = str,
            help = "min price limit",
            required = True)
    
    parser.add_argument(
            "--max_price",
            type = str,
            help = "max price limit",
            required = True)
    
    all_args = parser.parse_args()
    
    go(all_args)
    