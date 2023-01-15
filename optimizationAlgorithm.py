import pandas as pd
import numpy as np
from random import randint

def __assignNewCenter(duration, clt, kTmpClt, df): 
    if len(kTmpClt[clt]) > 2:
        durationTmp = duration.copy() 
        durationTmp = durationTmp[:,kTmpClt[clt]] 
        durationTmp = durationTmp[kTmpClt[clt],:] 
        np.fill_diagonal(durationTmp, 0)
        maxDist = durationTmp.max(axis=0)
        newC = maxDist.argmin()
    elif len(kTmpClt[clt])==2:
        kTmpADO = df[df['spId'].isin(kTmpClt[clt])].groupby('spId')['ado'].apply(list) 
        newC = 1
        if kTmpADO[kTmpClt[clt][0]][0] > kTmpADO[kTmpClt[clt][1]][0]:
            newC = 0 
    else:
        newC = 0
    return kTmpClt[clt][newC]

def __kmeansFit(dbscanResult, k, duration, maxIter, adoArray, radius, ttlADOClt, inputArray):
    bestRandomResult = pd.DataFrame()
    bestRandomADO = 0
    excludeInd = inputArray.copy()

    spxCenter = []
    for i in range(5):
        original = np.random.choice(np.setdiff1d(range(0,len(dbscanResult)), excludeInd), k, replace=False)
        originCenter = np.array(dbscanResult)[original]
        originCenterSPX = list(originCenter) + list(spxCenter)
        maxIter = maxIter
        iter = 0
        tmpBestResult = pd.DataFrame()
        tmpBestADO = -1
        while iter < maxIter:
            durationTmp = duration.copy()
            np.fill_diagonal(durationTmp, 0)
            durationTmp = durationTmp[:,dbscanResult]
            durationTmp = durationTmp[originCenterSPX,:]

            distance = durationTmp.min(axis=0)
            centerInd = durationTmp.argmin(axis=0)
            label = np.array(originCenterSPX)[centerInd]
            df = pd.DataFrame({'Id':dbscanResult, 'center':label, 'distance':distance, 'ado':adoArray})
            df['isCovered'] = 0
            df.loc[(df['distance']<radius), 'isCovered'] = 1
            coveredCltADO = df.loc[df['isCovered'] == 1, 'ado'].sum()
            if coveredCltADO > tmpBestADO:
                tmpBestADO = coveredCltADO
                tmpBestResult = df
    
            iter += 1

            kTmpClt = df.groupby('center')['spId'].apply(list)
            newCenter = []
            for cent in originCenter:
                newCenter.append(__assignNewCenter(duration, cent, kTmpClt, df))
            originCenter = newCenter.copy()
            originCenterSPX = list(originCenter) + list(spxCenter)

            if tmpBestADO >= bestRandomADO:
                bestRandomADO = tmpBestADO
                bestRandomResult = tmpBestResult                                                            
            if bestRandomADO==ttlADOClt:
                bestRandomResult = tmpBestResult
                break
        if bestRandomADO==ttlADOClt:
            bestRandomResult = tmpBestResult
            break
    return bestRandomResult, bestRandomADO
