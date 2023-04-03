from graphlib import TopologicalSorter
from airbyte.airbyte import AirbyteConnection

def loadAirbyte(
        host_ab:str, 
        auth_id_ab:str, 
        config, 
        logger):
    """
        Parameters
        ----------
        host_ab : str
            URL of the airbyte server
        bucket : str
            The S3 bucket name
        config
            The dictionary read from the YAML file with the ids of the Airbyte processes
        logger
            Logging object to write the results of the process
    """
    logger.info("Configuring extract process")
    host = host_ab
    auth_id = auth_id_ab

    try:
        airbyte_deptos = AirbyteConnection(host=host, connection_id=config['MGN_ANM_DPTOS'], auth_id = auth_id)
        airbyte_mcipio = AirbyteConnection(host=host, connection_id=config['MGN_ANM_MPIOS'], auth_id = auth_id)
        airbyte_mcipiocl = AirbyteConnection(host=host, connection_id=config['MGN_ANM_MPIOCL'], auth_id = auth_id)
        airbyte_setrur = AirbyteConnection(host=host, connection_id=config['MGN_ANM_SECTOR_RURAL'], auth_id = auth_id)
        airbyte_secrur = AirbyteConnection(host=host, connection_id=config['MGN_ANM_SECCION_RURAL'], auth_id = auth_id)
        airbyte_zurb = AirbyteConnection(host=host, connection_id=config['MGN_ANM_ZU'], auth_id = auth_id)
        airbyte_seturb = AirbyteConnection(host=host, connection_id=config['MGN_ANM_SECTOR_URBANO'], auth_id = auth_id)
        airbyte_securb = AirbyteConnection(host=host, connection_id=config['MGN_ANM_SECCION_URBANA'], auth_id = auth_id)
        airbyte_manzana = AirbyteConnection(host=host, connection_id=config['MGN_ANM_MANZANA'], auth_id = auth_id)

        dag = TopologicalSorter()

        dag.add(airbyte_deptos)
        dag.add(airbyte_mcipio, airbyte_deptos)
        dag.add(airbyte_mcipiocl, airbyte_mcipio)
        dag.add(airbyte_setrur, airbyte_mcipiocl)
        dag.add(airbyte_secrur, airbyte_setrur)
        dag.add(airbyte_zurb, airbyte_mcipiocl)
        dag.add(airbyte_seturb, airbyte_zurb)
        dag.add(airbyte_securb, airbyte_seturb)
        dag.add(airbyte_manzana, airbyte_securb)

        logger.info("Starting Airbyte")

        dag_rendered = tuple(dag.static_order())
        for node in dag_rendered: 
            node.run()
    except:
        raise Exception("Error: The load process failed")