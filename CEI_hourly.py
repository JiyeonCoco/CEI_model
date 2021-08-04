# =======================================================
#                    Basic Setting
# =======================================================

# computation
import numpy as np
import pandas as pd
import math

# utils
import os
import pickle
from datetime import timedelta, datetime, date
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings(action='ignore')

# DB Connect
from impala.dbapi import connect
from impala.util import as_pandas
from secureDBapi import connectSecure


# =======================================================
#                      Data Loading
# =======================================================

start_dt = '20210629'
end_dt = '20210729'

start_hh = '0'
end_hh = '23'

sql = """
(
SELECT 'LTE' as nw, '전국' as tp, null as id1, null as id2, '전국' as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.area_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, '본부' as tp, null as id1, null as id2, branch_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.bran_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, '시도' as tp, null as id1, null as id2, city_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.city_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, '구' as tp, null as id1, city_id as id2, gu_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.gu_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, '동' as tp, city_id as id1, gu_id as id2, dong_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.dong_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, 'ems' as tp, null as id1, null as id2, ems_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.ems_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, 'mme' as tp, null as id1, null as id2, mme_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.mme_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, 'pgw' as tp, null as id1, null as id2, pgw_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.pgw_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT 'LTE' as nw, 'sgw' as tp, null as id1, null as id2, sgw_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value
    ,  null as new_data_cfi_qoe1_qos7_value 
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem.sgw_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, '전국' as tp, null as id1, null as id2, '전국' as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.area_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, '본부' as tp, null as id1, null as id2, branch_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.bran_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, '시도' as tp, null as id1, null as id2, city_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.city_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, '구' as tp, null as id1, city_id as id2, gu_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.gu_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, '동' as tp, city_id as id1, gu_id as id2, dong_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.dong_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, 'ems' as tp, null as id1, null as id2, ems_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.ems_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, 'mme' as tp, null as id1, null as id2, mme_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.mme_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, 'pgw' as tp, null as id1, null as id2, pgw_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.pgw_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
UNION ALL
(
SELECT '5G' as nw, 'sgw' as tp, null as id1, null as id2, sgw_id as id3, dt, hh
    ,  new_hdv_cfi_value
    ,  new_hdv_cfi_qoe1_value
    ,  new_hdv_cfi_qoe2_value
    ,  new_hdv_cfi_qoe3_value
    ,  new_hdv_cfi_qoe4_value
    ,  new_hdv_cfi_qoe1_qos1_value
    ,  new_hdv_cfi_qoe1_qos2_value 
    ,  new_hdv_cfi_qoe1_qos3_value 
    ,  new_hdv_cfi_qoe1_qos4_value 
    ,  new_hdv_cfi_qoe1_qos5_value  
    ,  new_hdv_cfi_qoe2_qos1_value
    ,  new_hdv_cfi_qoe2_qos2_value 
    ,  new_hdv_cfi_qoe4_qos1_value
    ,  new_hdv_cfi_qoe4_qos2_value
    ,  new_data_cfi_value
    ,  new_data_nei_value
    ,  new_data_sei_value
    ,  new_data_nei_qoe1_value
    ,  new_data_sei_qoe1_value
    ,  new_data_nei_qoe2_value
    ,  new_data_sei_qoe2_value
    ,  new_data_cfi_qoe3_value
    ,  new_data_cfi_qoe4_value
    ,  new_data_cfi_qoe5_value
    ,  new_data_cfi_qoe1_qos1_value
    ,  new_data_cfi_qoe1_qos2_value 
    ,  new_data_cfi_qoe1_qos3_value
    ,  new_data_cfi_qoe1_qos4_value
    ,  new_data_cfi_qoe1_qos5_value
    ,  new_data_cfi_qoe1_qos6_value 
    ,  new_data_cei_lv as new_data_cfi_qoe1_qos7_value  
    ,  new_data_cfi_qoe2_qos1_value
    ,  new_data_cfi_qoe2_qos2_value
    ,  new_data_cfi_qoe2_qos3_value
    ,  new_data_cfi_qoe2_qos4_value 
    ,  new_data_cfi_qoe2_qos5_value 
    ,  new_data_cfi_qoe2_qos6_value 
    ,  new_data_cfi_qoe3_qos1_value
    ,  new_data_cfi_qoe3_qos2_value
    ,  new_data_cfi_qoe4_qos1_value
    ,  new_data_cfi_qoe5_qos1_value
    ,  new_data_cfi_qoe5_qos2_value
    ,  new_data_cfi_qoe5_qos3_value
    ,  new_data_cfi_qoe5_qos4_value 
FROM o_samson_cem5g.sgw_1h
WHERE dt >= (""" + start_dt + """)  and dt <= (""" + end_dt + """) and hh >= (""" + start_hh + """) and hh <= (""" + end_hh + """)
)
"""

data = GetQuery(sql)



# =======================================================
#                     Pre-Processing
# =======================================================

# 무의미한 id3 값 row 제거
data.drop(data[(data.id3 == '0') | (data.id3 == None) | (data.id3.isnull())].index.values, axis=0, inplace=True)

# datetime, id(nw&tp&id3) column 추가
data['ds'] = pd.to_datetime(data['dt'] + data['hh'], format='%Y%m%d%H')
data['id'] = data.apply(lambda x: x['nw']+'&'+x['tp']+'&'+x['id3'], axis=1)

data.sort_values(by=['ds', 'id'], inplace=True)
data.reset_index(inplace=True, drop=True)

# CEI list setting 및 str->float 형 변환
# CEI 점수가 0 or -100인 경우 Nan값으로 변환 (train data에서 제거하기 위함)
cei_list = data.columns[7:-2].tolist()
data[cei_list] = data[cei_list].apply(pd.to_numeric)
data[cei_list] = np.where((data[cei_list] <= 0), np.nan, data[cei_list])

# Normalization
min_max_scaler_grp = preprocessing.StandardScaler()
scaled = min_max_scaler_grp.fit_transform(data[cei_list])
tr_data = data.iloc[:, :6]
tr_data = pd.concat([tr_data, pd.DataFrame(scaled, columns=cei_list)], axis=1)
tr_data['ds'] = data['ds']
tr_data['id'] = data['id']


# =======================================================
#                     Anomaly Detection
# =======================================================

def check_data(tr_data, test_data, train_data):
    
    thr = 1.0
    
    cur_grp = tr_data.iloc[0]
    
    # IQR setting
    q1 = train_data[train_data['id'] == cur_grp['id']][cei_list].quantile(q=0.25)
    q3 = train_data[train_data['id'] == cur_grp['id']][cei_list].quantile(q=0.75)

    iqr = q3 - q1
    lower_thr = q1 - 1.5 * iqr

    return pd.DataFrame({'ds': [cur_grp['ds']] * len(cei_list),
                          'nw': [cur_grp['nw']] * len(cei_list),
                          'tp': [cur_grp['tp']] * len(cei_list),
                          'id1': [cur_grp['id1']] * len(cei_list),
                          'id2': [cur_grp['id2']] * len(cei_list),
                          'id3': [cur_grp['id3']] * len(cei_list),
                          'cei_factor': cei_list,
                          'data': test_data[test_data.id == cur_grp.id].iloc[0][cei_list].tolist(),
                          'status': ((((cur_grp[cei_list] - lower_thr)**2).values.flatten() > thr) & (cur_grp[cei_list] < lower_thr)).tolist(),
                          'loss': ((cur_grp[cei_list] - lower_thr)**2).values.flatten().tolist()
                        })



result = pd.DataFrame(columns=['ds', 'nw', 'tp', 'id1', 'id2', 'id3', 'cei_factor', 'data', 'status', 'loss'])

# 24H result
for i in range(24):
    
    TEST_DT = datetime.strptime('2021-07-16', '%Y-%m-%d') + timedelta(hours=i)

    # 전시간 / 전일 동시간,전시간 / 전주 동시간,전시간
    TRAIN_DT = [TEST_DT - timedelta(hours=1),
                TEST_DT - timedelta(days=1), TEST_DT - timedelta(days=1) - timedelta(hours=1),
                TEST_DT - timedelta(days=7), TEST_DT - timedelta(days=7) - timedelta(hours=1)]
    
    # 비정상을 판단할 data
    test_data = data[data.ds == TEST_DT]
    test_data.reset_index(inplace=True, drop=True)

    # 비정상 판단을 위해 사용될 training data
    train_data = data[data.ds.isin(TRAIN_DT)]
    train_data.reset_index(inplace=True, drop=True)
    
    test_tr_data = tr_data[tr_data.ds == TEST_DT]
    test_tr_data.reset_index(inplace=True, drop=True)

    train_tr_data = tr_data[tr_data.ds.isin(TRAIN_DT)]
    train_tr_data.reset_index(inplace=True, drop=True)
    
    
    cur_test_data = test_tr_data.copy()
    cur_train_data = train_tr_data.copy()

    # 각 id별로 CEI 지표들의 비정상 판단
    r = cur_test_data.groupby('id', group_keys=False).apply(lambda x: check_data(x, test_data, cur_train_data))
    
    # status : good / bad
    r.replace({'status': False}, {'status': 'good'}, inplace=True)
    r.replace({'status': True}, {'status': 'bad'}, inplace=True)
    r['loss'].fillna(0.0, inplace=True)
    r['data'].fillna(0.0, inplace=True)
    
    result = pd.concat([result, r], ignore_index=True)
    

result.to_csv('CEI_anomaly_result_' + file_date + '_1H.csv', index=False, sep=',')
