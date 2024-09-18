
from etf import *
from utils import *
import warnings
from tqdm import tqdm
from history import *
from collect_history import *
from preprocess_history import *
from correlation import *
from preprocess_history import *
from postgresql_helper import *


# MAIN PROCESS
# [1] COLLECT CORRELATIONS
# [2] MAKE CORRELATION CHUNKS
# [3] LOAD FILES TO DB

if __name__ == '__main__':
    
    # COLLECT CORRELATIONS
    collect_corrs()


    # MAKE CORRELATION CHUNKS
    save_dfs_by_chunk(
        get_dirpath = './downloads/correlation',
        put_dirpath = './downloads/correlation_chunks',
        prefix_chunk = 'correlation_chunk'
    )


    # LOAD FILES TO DB

    # COPY FILES TO DB: DM_CORRELATION
    copy_csv_files_to_db(
        csv_dir_path = os.path.join("downloads", "correlation_chunks"),
        table_name = "DM_CORRELATION",
        create_table_query = CREATE_TABLE_DM_CORRELATION
    )