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
    
    with ZipMemoryFile(file.content) as zip_memory_file:
        filename = zip_memory_file.open().name
        with zip_memory_file.open(f"{filename}.shp", encoding='utf-8') as shapeRead:
            parsed_shape = list(shapeRead)

    return filename, parsed_shape


def createDataFrame(
        parsed_shape:list
        )->pd.DataFrame:
    
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