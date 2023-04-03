import io
import boto3
import pandas as pd
from shapely import to_wkt
from shapely.geometry import shape
from fiona.io import ZipMemoryFile

def checkExisting(
        path:str,
        client:boto3.client,
        bucket:str,
        includeBlock:bool = True
        )->set:
    """
        Parameters
        ----------
        path : str
            The name of the folder to store the parquet files 
        client : boto3 client
            The client to connect to S3
        bucket : str
            The S3 bucket name
        includeBlock : bool, optional
            If you do not want to sincronize the block level just set as True (See the readme and documentation)
        
        Response
        ----------
        filesExisting : list
            A list with the files already processed in S3
    """
    
    prefix = f"{path}/"
    filesExisting = set()
    response = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter = '/')

    if includeBlock:
        filesExisting.add('MGN_ANM_MANZANA')

    if 'Contents' in response:
        for r in response['Contents']:
            filesExisting.add(r['Key'].split('/')[1].split('.')[0])
    
    return filesExisting

def createConvex(
        file
        )->list:
    """
        Parameters
        ----------
        file : 
            The S3 zip object containing the geospatial data to be read

        Response
        ----------
        filename : str
            The name of the SHP file to be processed.
        parsed_shape : list
            An object with the file already processed to be converted to a dataframe 
        """
    
    with ZipMemoryFile(file.content) as zip_memory_file:
        filename = zip_memory_file.open().name
        with zip_memory_file.open(f"{filename}.shp", encoding='utf-8') as shapeRead:
            parsed_shape = list(shapeRead)

    return filename, parsed_shape


def createDataFrame(
        parsed_shape:list
        )->pd.DataFrame:
    
    """
        Parameters
        ----------
        parsed_shape : list
            The geospatial data as list (contains attributes and geometry)

        Response
        ----------
        df : Dataframe
            The dataframe with the geospatial data. 
        """

    resultado = dict() 

    for e,i in enumerate(parsed_shape):
        for k,v in i['properties'].items():
            if k not in resultado:
                resultado[k] = []
            resultado[k].append(v)
        
        if 'geometry' not in resultado:
            resultado['geometry'] = []

        geo = shape(i['geometry'])
        
        resultado['geometry'].append(geo)
    
    df = pd.DataFrame(resultado)
    df["geometry"] = df["geometry"].apply(lambda x: to_wkt(x, rounding_precision=-1))

    return df