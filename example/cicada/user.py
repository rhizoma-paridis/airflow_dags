import logging
from CamelLuigi.query_loaders import GsLoader
from CamelLuigi.query_loaders import DataGroupStandAloneLoader

from dataclasses import dataclass

log = logging.getLogger(__name__)

@dataclass
class UserLoader(GsLoader):

    game: str = 'ody'
    server_ids = 60
    query_type: str = 'online'
    filter_sandbox: bool = True


    def __post_init__(self):
        self.sql = """
        select * from user_info limit 10
        """

def queryUser():
    log.info("queryUser")
    df = DataGroupStandAloneLoader('select * from wof_gift_h limit 5').load()
    log.info(df.head(5))