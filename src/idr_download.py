import sys
import time
import subprocess
from typing import Optional
from pathlib import Path
import itertools
import typer
import logging

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("Download Idr dataset")
logger.setLevel(logging.INFO)


def makedirectory(idrId:str, path: Optional[str] = ".data") -> None:
    """Make directories for downloading raw  data"""
    dirpath = Path(path, "raw")
    if not dirpath.exists():
        dirpath.mkdir(exist_ok=True, parents=True)
        logger.info(f"{path}/{idrId}/raw directory created")
    else:
        pass
    return

def check_container_status(container_id:str):  
    '''Check the Container status'''
    p = subprocess.check_output('docker ps', shell=True).decode(sys.stdout.encoding).split('\n')   
    containers = [c.split() for c in p]
    contlist = list(itertools.chain(*containers))
    if container_id in contlist:
        container_status = contlist[-2]      
        return container_status
    
def stop_containers(container_id:str):
    '''Stop and delete container'''
    cmd = f'docker stop {container_id}'
    subprocess.run(cmd, shell=True)
    logger.info(f'{container_id} container is stopped now')
    return


def runcommand(command, verbose = False, *args, **kwargs):
    process = subprocess.Popen(
        command,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    pass

   
def idr_download(idrId:str, path:str = ".data") -> None:  
    """Downloading IDR High Content Screening dataset from following link https://idr.openmicroscopy.org/, 
    Create an folder for each screen and download dataset using "imagedata/download" container image
    
    Args:
        idrId : Id of the Idr dataset
        path : Path to download   
    """
    logger.info('Creating a directory')
    inpDir = Path(path, 'raw')
    if not inpDir.exists():
        inpDir.mkdir(exist_ok=True, parents=True)
        logger.info(f"{inpDir} directory created")

    assert Path.exists(inpDir), f'{inpDir} directory doesnot exist. Please check it again!!'
    command = f'docker run  -v {inpDir}:/data imagedata/download {idrId} . /data'
    p = subprocess.run(command,shell=True,stdout=subprocess.PIPE)
    container_id = p.stdout.decode().strip().split("-")[-1]
    logger.info('Downloading dataset')
    status = 'Unknown'
    i = 0
    while status != 'Succeeded':
        time.sleep(40)
        status = check_container_status(container_id)
        print('Status of Container {} is {}'.format(container_id, status))
        i += 1
        if i > 20 and status == 'Pending':
            raise Exception('Docker contianer {} failed to start'.format(container_id))

        if status == 'Failed':
            raise Exception('Docker contianer {} has failed'.format(container_id))
            
    if status == 'Succeeded': 
        logger.info(f'{idrId} dataset download is completed')
    return 

app = typer.Typer()

@app.command()
def main(
        path: Path = typer.Option(
        ".data",
        "--path",
        help="Path to download data",
    ),
    idr_Id: str = typer.Option(
        ..., "--idrId", help="Id of Idr dataset"
    )
    ):
    logger.info(f"path = {path}")
    logger.info(f"idrId = {idr_Id}")

    path = path.resolve()
    assert path.exists(), f"{path} does not exist!! Please check input path again"

    idr_download(idr_Id, path)

 
if __name__ == "__main__":
    app()