import pandas as pd
import numpy as np
import time

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',20)

while True:
    start = time.time()

    dataCsv = pd.read_csv(r'C:\Users\Solomon\Desktop\Local blotter.csv')
    dfCsv = pd.DataFrame(dataCsv)
    print(dfCsv)


    dataJson = pd.read_json(r'C:\Users\Solomon\Desktop\Database.json', orient='split')
    dfJson = pd.DataFrame(dataJson)
    print(dfJson)


    if dfJson.equals(dfCsv):
        print('same')
    else:
        print('diff')
        dfCsv.to_json(r'C:\Users\Solomon\Desktop\Database.json', orient='split',double_precision=15)

    end = time.time()
    print(end - start)

    time.sleep(5)







