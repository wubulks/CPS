#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Module Name   : PrepCWRF (CWRF Preprocessing Module)
Description   : Handles the data preparation workflow for the CWRF atmospheric 
                model component.
                
                Key Functions:
                - First_StaticData : Runs Geogrid to generate static geo data.
                - Second_ICBC      : Runs Ungrib, Metgrid, and Real for IC/BCs.
                - Gather_Output    : Organizes WRF input/boundary files.

Author        : Omarjan @ SYSU
Created       : 2025-05-25
Last Modified : 2026-03-04
===============================================================================
"""

import os
import sys
import time
import glob
import shlex
import logging
import configparser
import pandas as pd
import numpy as np
import multiprocessing
from datetime import datetime, timedelta
from Utils import Tools, Consts, ICBC
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = logging.getLogger("CRESMPrep." + __name__)

def First_StaticData(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CWPSPath = envcfg.get('Paths', 'CWPSPath')
    ChaoModis = envcfg.get('Paths', 'ChaoModis')
    WMEJModis = envcfg.get('Paths', 'WMEJModis')
    GlobalLakeDepth = envcfg.get('Paths', 'GlobalLakeDepth')
    GlobalLakeStatus = envcfg.get('Paths', 'GlobalLakeStatus')
    GeogDataPath = envcfg.get('Paths', 'GeogDataPath')
    NCOPath = envcfg.get('Paths', 'NCOPath')
    CDOPath = envcfg.get('Paths', 'CDOPath')
    NCLPath = envcfg.get('Paths', 'NCLPath')
    xesmfenv = envcfg.get('Environment', 'CONDA_XESMF')
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')

    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    StartTime =  casecfg.get(gridname, 'StartTime')
    EndTime = casecfg.get(gridname, 'EndTime')
    Go_Geogrid = casecfg.getboolean('PrepCWRF', 'Go_Geogrid')
    Go_FVC = casecfg.getboolean('PrepCWRF', 'Go_FVC')
    Go_LAI = casecfg.getboolean('PrepCWRF', 'Go_LAI')
    Go_SAI = casecfg.getboolean('PrepCWRF', 'Go_SAI')
    Go_IGBP = casecfg.getboolean('PrepCWRF', 'Go_IGBP')
    Collect_GeogData = casecfg.getboolean('PrepCWRF', 'Collect_GeogData')
    Use_CoLMLAI = casecfg.getboolean('BaseInfo', 'Use_CoLMLAI')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    dx_WE = casecfg.get(gridname, 'dx_WE')
    dy_SN = casecfg.get(gridname, 'dy_SN')
    RefLat = casecfg.get(gridname, 'RefLat')
    RefLon = casecfg.get(gridname, 'RefLon')
    True_Lat1 = casecfg.get(gridname, 'True_Lat1')
    True_Lat2 = casecfg.get(gridname, 'True_Lat2')
    LakeThreshold = casecfg.getfloat(gridname, 'LakeThreshold')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    CWPSNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwps.{gridname}'
    CWRFNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwrf.{gridname}'
    maxmin_wgs = Tools.Get_Area_MaxMin_Coords(casecfg, gridname)
    SinGridList = Tools.Build_SinGridList_From_MaxMinWGS(maxmin_wgs, Expand_Deg = 1.0, Return_String = True)
    chaomodisenv = envcfg.get('Environment', 'CONDA_CHAO')
    SYS_CWRF = envcfg.get('Environment', 'SYS_CWRF')


    if (Go_Geogrid) or (Go_FVC) or (Go_LAI) or (Go_SAI) or (Go_IGBP) or (Collect_GeogData):
        logger.info(f"The steps are: ●  Create Geogrid -> Create FVC -> Geog-Post-Process -> Create LAI -> Create IGBP -> Create SAI -> Collect Data ●\n")
        if Go_Geogrid:
            logger.info(f'{Consts.S4}==========> Creating Geogrid Data <==========')
            st = time.time()
            # cd to First_StaticData/Geogrid
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid')
        
            # link CWPS files to First_StaticData/Geogrid
            Tools.Link(f'{CWPSPath}/*', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/')

            # copy namelist file
            Tools.Copy(f'{CWPSNMLPath}', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/namelist.wps')

            # link global lake depth data
            Tools.Link(f'{GlobalLakeDepth}', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/GlobalLakeDepth.dat')
            
            # link global lake status data
            Tools.Link(f'{GlobalLakeStatus}', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/GlobalLakeStatus.dat')
            
            # run geogrid.exe and wait for it to finish
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.geogrid'
            cmd = f'mpirun -n {CWRFCoreNum} ./geogrid.exe > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run geogrid.exe", env=SYS_CWRF)


            # link CorrectGeoEM.py
            cmd = f'/usr/bin/ln -sf  {ProcessScriptPath}/PrepCWRF/CorrectGeoEM.py .'
            Tools.Run_CMD(cmd, "Link CorrectGeoEM.py")
            Tools.Link(f'{ProcessScriptPath}/PrepCWRF/CorrectGeoEM.py', './CorrectGeoEM.py')

            # backup geo_em.d01.nc
            Tools.Copy('geo_em.d01.nc', 'geo_em.d01.bck.nc')

            # run CorrectGeoEM.py
            # LandSeaMask = 'world_union.shp'
            LandSeaMask = f'{GeogDataPath}/Land-and-sea-boundary-data/world_union.shp'
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.CorrectGeoEM'
            cmd = f'conda run -n {xesmfenv} --no-capture-output python -u CorrectGeoEM.py -lk {LakeThreshold} -lsbdy {LandSeaMask} > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run CorrectGeoEM.py")
            logger.info(f"{Consts.S4}✓  Corrected geo_em.d01.nc")
            
            geo_em_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/geo_em.d01.nc'
            # check geo_em.d01.nc
            Tools.File_Exist(geo_em_path, level='error')
            
            logger.info(f"{Consts.S4}-> Geogrid Path: {geo_em_path}")
            logger.info(f"{Consts.S4}✓  Geogrid finished!")
        else:
            geo_em_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/geo_em.d01.nc'
            if not Tools.File_Exist(geo_em_path, level='warning'):
                logger.warning(f'{Consts.S4}But Go_Geogrid is set to False.')
                logger.warning(f'{Consts.S4}Please check the geogrid.exe')
            logger.info(f'{Consts.S4}==========> Skip Geogrid <==========')
            logger.info(f'{Consts.S4}-> Geogrid Path: {geo_em_path}')
            logger.info(f"{Consts.S4}✓  Geogrid finished!")

        geo_em_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/geo_em.d01.nc'
        # check geo_em_veg file
        if not Tools.File_Exist(geo_em_path, level='warning'):
            geo_em_path_ = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/geo_em.d01_veg.nc'
            if Tools.File_Exist(geo_em_path_, level='warning'):
                geo_em_path = geo_em_path_
                logger.warning(f'{Consts.S4}Use geo_em_veg from Geog_{gridname} instead.')
            logger.warning(f'{Consts.S4}Please check the geog-post-process step or provide the correct geo_em_veg file in Geog_{gridname}')

        if Go_FVC:
            #---------------------- FVC ----------------------
            logger.info(f'{Consts.S4}==========> Creating FVC Data <==========')
            # clear old files
            cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/FVC/*'
            Tools.Run_CMD(cmd, "Clear old files in FVC")
            
            # cd to First_StaticData/FVC
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/FVC')
            
            # link MODIS2CWRF files to First_StaticData/FVC
            Tools.Link(f'{ChaoModis}/MODIS2CWRF_SBC/*', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/FVC')

            # link geo_em.d01 to First_StaticData/FVC
            Tools.Link(geo_em_path, f'./geo_em.d01.nc')

            # link Run MODIS2CWRF.py
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.FVC'
            # syears = StartTime.year
            # eyears = EndTime.year
            syears = 2000  # Hardcoded to cover all MODIS data
            eyears = 2023  # Hardcoded to cover all MODIS data
            cmd = f'conda run -n {chaomodisenv} --no-capture-output python -u MODIS2CWRF.py -hvs "{SinGridList}" -YS {syears} -YE {eyears} > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run MODIS2CWRF.py")

            # Check FVC files
            FVCPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/FVC/modis_{syears}*_FVC_daily.nc'
            FVCFiles = glob.glob(FVCPath)
            if len(FVCFiles) == 0:
                logger.error(f'FVC files not found in {FVCFiles}')
                logger.error(f'Please check the MODIS2CWRF.py')
                raise FileNotFoundError('FVC files not found')
            logger.info(f'{Consts.S4}-> MODIS FVC Path: {FVCFiles[0]}')
            logger.info(F'{Consts.S4}✓  Create FVC finished!')
        
            #---------------------- Geog-Post-Process ----------------------
            logger.info(F'{Consts.S4}==========> Geog-Post-Process <==========')
            # cd to First_StaticData/Geog
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/GeogPostProcess/') 
            
            # copy geo_em.d01.nc to First_StaticData/GeogPostProcess
            Tools.Copy(geo_em_path, f'./geo_em.d01.nc')
            
            # copy ocean mask file to First_StaticData/Geog
            Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/ocean_mask.nc', './ocean_mask.nc')

            # copy FVC file to First_StaticData/GeogPostProcess
            Tools.Link(FVCFiles[0], f'./MODIS2CWRF_SBC_d01.nc')

            # copy namelist file to First_StaticData/GeogPostProcess
            Tools.Link(CWRFNMLPath, './namelist.input')
            Tools.Link(CWPSNMLPath, './namelist.wps')
        
            # copy post-process script to First_StaticData/GeogPostProcess
            Tools.Link(f'{ProcessScriptPath}/PrepCWRF/generate_cwrf_flow.py', './generate_cwrf_flow.py')
            Tools.Link(f'{ProcessScriptPath}/PrepCWRF/process_geo.py', './process_geo.py')
            Tools.Link(f'{ProcessScriptPath}/PrepCWRF/take_care_all.py', './take_care_all.py')
        
            # run Geog-post-process script
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.GeogPostProcess'
            Tools.Run_CMD(f'rm -f {log_file}', "Remove old log file")
            cmd = f'conda run -n {cresmenv} --no-capture-output python -u generate_cwrf_flow.py > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run generate_cwrf_flow.py")
            cmd = f'conda run -n {xesmfenv} --no-capture-output python -u process_geo.py -res {dx_WE} -lk {LakeThreshold} -cpu {CWRFCoreNum} >> {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run process_geo.py")
            cmd = f'conda run -n {xesmfenv} --no-capture-output python -u take_care_all.py >> {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run take_care_all.py")
            logger.info(f"{Consts.S4}-> GeogPostProcess Path: {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/GeogPostProcess/")
            geo_em_veg_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/GeogPostProcess/geo_em.d01_veg.nc'
            if not Tools.File_Exist(geo_em_veg_path, level='error'):
                logger.error(f'{Consts.S4}GeogPostProcess failed to produce geo_em.d01_veg.nc')
                logger.error(f'Please check the log file: {log_file}')
                raise FileNotFoundError('geo_em.d01_veg.nc not found')
            logger.info(f"{Consts.S4}✓  GeogPostProcess finished!")
            os.chdir(old_path)
        else:
            logger.info(f'{Consts.S4}==========> Skip FVC <==========')
            logger.info(f'{Consts.S4}-> MODIS FVC Path: {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/FVC/modis_{StartTime.year}*_FVC_daily.nc')
            geo_em_veg_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/GeogPostProcess/geo_em.d01_veg.nc'
            if not Tools.File_Exist(geo_em_veg_path, level='warning'):
                geo_em_veg_path_ = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/geo_em.d01_veg.nc'
                if Tools.File_Exist(geo_em_veg_path_, level='warning'):
                    geo_em_veg_path = geo_em_veg_path_
                    logger.warning(f'{Consts.S4}Use geo_em_veg from Geog_{gridname} instead.')
                logger.warning(f'{Consts.S4}Please check the geog-post-process step or provide the correct geo_em_veg file in Geog_{gridname}')


        if Go_LAI:
            if Use_CoLMLAI:
                '''
                Use the CoLM LAI data (Lin et al. 2022) with the WMEJ script. doi:10.1016/j.rse.2011.01.001
                ''' 
                #---------------------- LAI ----------------------
                logger.info(f'{Consts.S4}==========> Creating LAI Data <==========')
                
                # clear old files
                cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/*_LAI_*.nc '
                Tools.Run_CMD(cmd, "Clear old files in First_StaticData/LAI")

                # cd to First_StaticData/LAI
                os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI')
                
                # link WMEJModis files to First_StaticData/LAI
                Tools.Link(f'{WMEJModis}/*', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/')
                
                # link geo_em.d01 to First_StaticData/LAI
                Tools.Link(geo_em_veg_path, './geo_em.d01.nc')
                
                # Run First_StaticData/LAI/Regrid2CWRF.py
                # syears = StartTime.year
                # eyears = EndTime.year
                syears = 2001  # Hardcoded to cover all MODIS data
                eyears = 2020  # Hardcoded to cover all MODIS data
                log_file = f'{CaseOutputPath}/{gridname}/Log/log.LAI.WMEJ'
                # cmd = f'conda run -n {xesmfenv} --no-capture-output python -u Regrid2CWRF.py '
                # cmd +=f' -SY {syears} -EY {eyears} -DX {dx_WE} -DY {dy_SN} '
                # cmd +=f' -RefLat {RefLat} -RefLon {RefLon} -TrueLat1 {True_Lat1} -TrueLat2 {True_Lat2} '
                # cmd +=f' -CPU {CWRFCoreNum} -NCO {NCOPath} -CDO {CDOPath} > {log_file} 2>&1 '
                """New script with parallel"""
                cmd = f'conda run -n {xesmfenv} --no-capture-output python -u Regrid2CWRF_Parallel.py '
                cmd +=f' -sy {syears} -ey {eyears} -dx {dx_WE} -dy {dy_SN} '
                cmd +=f' -reflat {RefLat} -reflon {RefLon} -truelat1 {True_Lat1} -truelat2 {True_Lat2} '
                cmd +=f' -cpu {CWRFCoreNum} > {log_file} 2>&1 '
                Tools.Run_CMD(cmd, "Regrid CoLM LAI data to CWRF grid")
            
                # Check LAI files
                # LAIPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/MODIS_LAI_8day_*.nc'
                LAIPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/MODIS_LAI_*.nc'
                LAIFiles = glob.glob(LAIPath)
                LAIFiles = sorted(LAIFiles)
                if len(LAIFiles) == 0:
                    logger.error(f'LAI files not found in {LAIFiles}')
                    logger.error(f'Please check the Regrid2CWRF.py')
                    sys.exit(1)
                logger.info(f'{Consts.S4}-> CoLM LAI Path: ')
                for path in LAIFiles:
                    logger.info(f'{Consts.S8}{path}')
                logger.info(f'{Consts.S4}✓  Create LAI finished!')
            
                #---------------------- Merge LAI ----------------------
                logger.info(f'{Consts.S4}==========> Merging LAI <==========')
                # clear old files
                cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/temp/*'
                Tools.Run_CMD(cmd, "Clear old files in temp")
                
                # cd to First_StaticData/temp
                os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/temp')
                
                #link MODIS_LAI_ymonmean.nc to First_StaticData/temp
                Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/MODIS_LAI_ymonmean.nc', './MODIS_LAI_ymonmean.nc')
                logger.info(f'{Consts.S4}✓  Merging LAI finished!')
            else:
                '''
                Use the original MODIS data to produce LAI data, with the Chao’s script.
                '''
                #---------------------- LAI ----------------------
                logger.info(f'{Consts.S4}==========> Creating LAI Data <==========')
                
                # clear old files
                cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/*'
                Tools.Run_CMD(cmd, "Clear old files in LAI")
                
                # cd to First_StaticData/lai2sai
                os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI')
                
                # link lai2sai files to First_StaticData/lai2sai
                Tools.Link(f'{ChaoModis}/lai2sai/MODIS_LAI2CWRF_LAI/*', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/')

                # cd to First_StaticData/LAI
                os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI')
                
                # link geo_em.d01 to First_StaticData/LAI
                Tools.Link(geo_em_veg_path, './geo_em.d01.nc')
                
                # Run First_StaticData/LAI/MODIS2CWRF.py
                cmds = []
                # syears = StartTime.year
                # eyears = EndTime.year
                syears = 2002  # Hardcoded to cover all MODIS data
                eyears = 2020  # Hardcoded to cover all MODIS data
                for year in range(syears, eyears+1):
                    log_file = f'{CaseOutputPath}/{gridname}/Log/log.LAI_{year}.Chao'
                    cmd = f'conda run -n {chaomodisenv} --no-capture-output python -u MODIS2CWRF.py -hvs "{SinGridList}" -YS {year} -YE {year} > {log_file} 2>&1'
                    cmds.append(cmd)
                # with multiprocessing.Pool(processes=CWRFCoreNum) as pool:
                #     pool.map(Tools.Run_CMD, cmds)
                Tools.Run_Parallel(Tools.Run_CMD, [(cmd,) for cmd in cmds], CWRFCoreNum, "LAI")
                
                # Check LAI files
                LAIPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/LAI/modis_*_LAI_daily.nc'
                LAIFiles = glob.glob(LAIPath)
                LAIFiles = sorted(LAIFiles)
                if len(LAIFiles) == 0:
                    logger.error(f'LAI files not found in {LAIFiles}')
                    logger.error(f'Please check the MODIS2CWRF.py')
                    raise FileNotFoundError('LAI files not found')
                logger.info(f'{Consts.S4}-> MODIS LAI Path: ')
                for path in LAIFiles:
                    logger.info(f'{Consts.S8}{path}')
                logger.info(f'{Consts.S4}✓  Create LAI finished!')
                os.chdir(old_path)
            
                #---------------------- Merge LAI ----------------------
                logger.info(f'{Consts.S4}==========> Merging LAI <==========')
                # clear old files
                cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/temp/*'
                Tools.Run_CMD(cmd, "Clear old files in temp")
                
                # cd to First_StaticData/temp
                os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/temp')
                
                #link modis_*_LAI_daily.nc to First_StaticData/temp
                Tools.Link(LAIFiles, './')
                
                # Merge LAI
                cmd = f'{CDOPath} -O mergetime modis_20* MODIS_LAI_merge.nc && {CDOPath} -O ymonmean MODIS_LAI_merge.nc MODIS_LAI_ymonmean.nc '
                Tools.Run_CMD(cmd, "Merge LAI")
                logger.info(f'{Consts.S4}✓  Merging LAI finished!')
        else:
            logger.info(f'{Consts.S4}==========> Skip LAI <==========')
        
        
        # Check Merged LAI files
        LAIPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/temp/MODIS_LAI_ymonmean.nc'
        if not Tools.File_Exist(LAIPath, level='warning'):
            logger.warning(f'{Consts.S4} check the cdo command')
        logger.info(f'{Consts.S4}-> MODIS LAI Path: {LAIPath}')
        os.chdir(old_path)


        if Go_IGBP:
            #---------------------- IGBP ----------------------
            logger.info(f'{Consts.S4}==========> Creating IGBP Data <==========')
            
            # clear old files
            cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/IGBP/*'
            Tools.Run_CMD(cmd, "Clear old files in IGBP")
            
            # cd to First_StaticData/IGBP
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/IGBP')
            
            # link lai2sai files to First_StaticData/IGBP
            Tools.Link(f'{ChaoModis}/lai2sai/MODIS2CWRF_MCD12Q1/*', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/IGBP/')
            
            # cd to First_StaticData/IGBP
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/IGBP')
            
            # link geo_em.d01 to First_StaticData/IGBP
            Tools.Link(geo_em_veg_path, './geo_em.d01.nc')
            
            # Run First_StaticData/IGBP/MODIS2CWRF.py
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.IGBP'
            cmd = f'conda run -n {chaomodisenv} --no-capture-output python -u MODIS2CWRF.py -hvs "{SinGridList}" -YS {StartTime.year} -YE {EndTime.year} > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run MODIS2CWRF.py")
            logger.info(f'{Consts.S4}✓  Create IGBP finished!')
        else:
            logger.info(f'{Consts.S4}==========> Skip IGBP <==========')
        
        # Check IGBP files
        IGBPPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/IGBP/modis_{StartTime.year}*_IGBP_daily.nc'
        IGBPPath = glob.glob(IGBPPath)
        if len(IGBPPath) == 0:
            logger.warning(f'{Consts.S4}IGBP files not found in {IGBPPath}')
            logger.warning(f'{Consts.S4}Please check the MODIS2CWRF.py')
        else:
            IGBPPath = IGBPPath[0]
            if not Tools.File_Exist(IGBPPath, level='warning'):
                logger.warning(f'{Consts.S4}Please check the MODIS2CWRF.py')
        logger.info(f'{Consts.S4}-> MODIS IGBP Path: {IGBPPath}')

        os.chdir(old_path)

        if Go_SAI:
            #---------------------- SAI ----------------------
            logger.info(f'{Consts.S4}==========> Creating SAI Data <==========')
            
            # clear old files
            cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/*'
            Tools.Run_CMD(cmd, "Clear old files in SAI")
            
            #clear old files
            cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/sbcs'
            Tools.Run_CMD(cmd, "Clear old files in sbcs")
            
            # mkdir
            cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/sbcs'
            Tools.Run_CMD(cmd, "Create directory sbcs")
            
            # cd to First_StaticData/SAI
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI')
            
            # link lai2sai files to First_StaticData/SAI
            Tools.Link(f'{ChaoModis}/lai2sai/LAI2SAI/*', f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/')
            
            # link geo_em.d01 to First_StaticData/SAI
            Tools.Link(geo_em_veg_path, './geo_em.d01.nc')
            
            #link MODIS_LAI_ymonmean.nc to First_StaticData/SAI
            Tools.Link(LAIPath, './MODIS_LAI_ymonmean.nc')

            #link modis_*_IGBP_daily.nc to First_StaticData/SAI
            Tools.Link(IGBPPath, './MODIS_IGBP_daily.nc')

            # Run First_StaticData/SAI/lai2sai.py
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.SAI'
            cmd = f'conda run -n {chaomodisenv} --no-capture-output python -u lai2sai.py > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run lai2sai.py")
            
            # Split SAI and LAI files
            log_file = f'{CaseOutputPath}/{gridname}/Log/log.split_sai_lai'
            cmd = f'{NCLPath} split_lat_sai.ncl > {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Split SAI and LAI files")
            logger.info(f'{Consts.S4}✓  Create SAI finished!')
        else:
            logger.info(f'{Consts.S4}==========> Skip SAI <==========')

        # check SAI files
        SAIPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/MODIS_SAI_ymonmean.nc'
        if not Tools.File_Exist(SAIPath, level='warning'):
            logger.warning(f'{Consts.S4}Please check the lai2sai.py')
        logger.info(f'{Consts.S4}-> MODIS SAI Path: {SAIPath}')
        laisaipath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/sbcs/'
        laisai = glob.glob(f'{laisaipath}/*')
        laisai = sorted(laisai)
        if len(laisai) == 0:
            logger.warning(f'{Consts.S4}SAI files not found in {laisaipath}')
            logger.warning(f'{Consts.S4}Please check the lai2sai.py')
        logger.info(f'{Consts.S4}-> SAI and LAI Path:')
        for path in laisai:
            logger.info(f'{Consts.S8}{path}')
        os.chdir(old_path)

        if Collect_GeogData:
            #---------------------- Collect GeogData ----------------------
            logger.info(f'{Consts.S4}==========> Collect GeogData <==========')
            laisai = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/SAI/sbcs'

            # cd to First_StaticData/Geog_{gridname}
            os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/')
            
            # clear old files
            cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/*'
            Tools.Run_CMD(cmd, f"Clear old files in First_StaticData/Geog_{gridname}")

            # mkdir
            cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs'
            Tools.Run_CMD(cmd, "Create directory sbcs")

            # copy placeholder data to First_StaticData/Geog_{gridname}
            Tools.Copy(f'{laisai}/*', f'./sbcs/')
            laifiles = glob.glob(f'./sbcs/lai*')
            for laifile in laifiles:
                Tools.Copy(laifile, f'{laifile.replace("lai", "vegb")}')
            laifiles = glob.glob(f'./sbcs/sai*')
            for laifile in laifiles:
                Tools.Copy(laifile, f'{laifile.replace("sai", "albb")}')
            
            # copy geo_em.d01 to First_StaticData/Geog_{gridname}
            Tools.Copy(geo_em_veg_path, f'./geo_em.d01_veg.nc')
            
            # copy ocean mask file to First_StaticData/Geog_{gridname}
            Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geogrid/ocean_mask.nc', './ocean_mask.nc')

            # copy MODIS2CWRF_SBC_d01.nc to First_StaticData/Geog_{gridname}
            Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/GeogPostProcess/MODIS2CWRF_SBC_d01.nc', './MODIS2CWRF_SBC_d01.nc')
            
            # # copy namelist file to First_StaticData/Geog_{gridname}
            # Tools.Copy(CWRFNMLPath, './namelist.input')
            # Tools.Copy(CWPSNMLPath, './namelist.wps')
            
            # Get unique Geog ID
            GeogID = Tools.Get_Unique_GeogID(casecfg, envcfg, gridname)
            logger.info(f'{Consts.S4}-> Geog ID: {GeogID}')
            GeogIDfile = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/Geog.ID'
            cmd = f'rm -f {GeogIDfile}'
            Tools.Run_CMD(cmd, f"Remove old Geog ID file {GeogIDfile}")            
            with open(GeogIDfile, 'w') as f:
                f.write(f'{GeogID}\n')
            
            os.chdir(old_path)
        else:
            logger.info(f'{Consts.S4}==========> Skip Collect GeogData <==========')
        
        GeogDataPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}'
        Tools.File_Exist(GeogDataPath, level='error')
        Tools.Copy(GeogDataPath, f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/')
        Tools.Copy(GeogDataPath, f'{CaseOutputPath}/{gridname}/{gridname}/Grid_{gridname}/')
        
    else:
        logger.info(f'{Consts.S4}==========> Skip Geogrid <==========')
        logger.info(f'{Consts.S4}==========> Skip FVC <==========')
        logger.info(f'{Consts.S4}==========> Skip LAI <==========')
        logger.info(f'{Consts.S4}==========> Skip SAI <==========')
        logger.info(f'{Consts.S4}==========> Skip IGBP <==========')
        logger.info(f'{Consts.S4}==========> Skip Collect GeogData <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')

    logger.info(f'{Consts.S4}◉  First_StaticData Complete!\n\n')
    os.chdir(old_path)



def Copy_Exist_GeogData(casecfg, envcfg, gridname, GeogDataPath):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    
    # Check if GeogDataPath exists
    Tools.File_Exist(GeogDataPath, level='error')
    
    # [1] check geo_em.d01_veg.nc
    geo_em_veg_path = f'{GeogDataPath}/geo_em.d01_veg.nc'
    Tools.File_Exist(geo_em_veg_path, level='error')
    
    # [2] check ocean_mask.nc
    ocean_mask_path = f'{GeogDataPath}/ocean_mask.nc'
    Tools.File_Exist(ocean_mask_path, level='error')
    
    # [3] check MODIS2CWRF_SBC_d01.nc
    MODIS2CWRF_SBC_path = f'{GeogDataPath}/MODIS2CWRF_SBC_d01.nc'
    Tools.File_Exist(MODIS2CWRF_SBC_path, level='error')
    
    # [4] check sbcs files: lai*, sai*, albb*, vegb*
    sbcspath = f'{GeogDataPath}/sbcs'
    Tools.File_Exist(sbcspath, level='error')
    laifiles = glob.glob(f'{sbcspath}/lai*')
    if len(laifiles) == 0:
        logger.error(f'LAI files not found in {sbcspath}/lai*')
        raise FileNotFoundError('LAI files not found')
    saifiles = glob.glob(f'{sbcspath}/sai*')
    if len(saifiles) == 0:
        logger.error(f'SAI files not found in {sbcspath}/sai*')
        raise FileNotFoundError('SAI files not found')
    albbfiles = glob.glob(f'{sbcspath}/albb*')
    if len(albbfiles) == 0:
        logger.error(f'ALBB files not found in {sbcspath}/albb*')
        raise FileNotFoundError('ALBB files not found')
    vegbfiles = glob.glob(f'{sbcspath}/vegb*')
    if len(vegbfiles) == 0:
        logger.error(f'VEGB files not found in {sbcspath}/vegb*')
        raise FileNotFoundError('VEGB files not found')

    # [5] check Geog.ID
    GeogIDfile = f'{GeogDataPath}/Geog.ID'
    with open(GeogIDfile, 'r') as f:
        ExitsGeogID = f.readline().strip()
    logger.info(f'{Consts.S4}-> Exits Geog ID: {ExitsGeogID}')
    
    # Get unique Geog ID from casecfg
    GeogID = Tools.Get_Unique_GeogID(casecfg, envcfg, gridname)
    logger.info(f'{Consts.S4}-> New Case Geog ID: {GeogID}')
    if ExitsGeogID != GeogID:
        logger.error(f'Geog ID in {GeogIDfile} is not match with the current Geog ID: {GeogID}')
        logger.error(f'Please check your case setting .')
        logger.error(f'If you want to use the existing GeogGatherPath, please use the same Geog setting in your case.')
        raise ValueError('Geog ID is not match')
    else:
        logger.info(f'{Consts.S4}-> Geog ID is match, continue to copy GeogGatherPath')
    
    logger.info(f'{Consts.S4}==========> Skip Geogrid <==========')
    logger.info(f'{Consts.S4}==========> Skip FVC <==========')
    logger.info(f'{Consts.S4}==========> Skip LAI <==========')
    logger.info(f'{Consts.S4}==========> Skip SAI <==========')
    logger.info(f'{Consts.S4}==========> Skip IGBP <==========')
    logger.info(f'{Consts.S4}==========> Skip Collect GeogData <==========')
    logger.info(f'{Consts.S4}-> GeogDataPath: {GeogDataPath}')
    logger.info(f'{Consts.S4}-> Copy GeogDataPath to {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}')
    logger.info(f'{Consts.S4}-> Copy GeogDataPath to {CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}')

    # cd to CaseOutputPath/gridname/PrepCWRF
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/')

    # mkdir
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs'
    Tools.Run_CMD(cmd, "Create directory sbcs")

    # copy placeholder data to First_StaticData/Geog_{gridname}
    Tools.Copy(laifiles, f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs/')
    Tools.Copy(saifiles, f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs/')
    Tools.Copy(albbfiles, f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs/')
    Tools.Copy(vegbfiles, f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs/')

    # copy geo_em.d01 to First_StaticData/Geog_{gridname}
    Tools.Copy(geo_em_veg_path, f'./geo_em.d01_veg.nc')
    
    # copy ocean mask file to First_StaticData/Geog_{gridname}
    Tools.Copy(ocean_mask_path, './ocean_mask.nc')

    # copy MODIS2CWRF_SBC_d01.nc to First_StaticData/Geog_{gridname}
    Tools.Copy(MODIS2CWRF_SBC_path, './MODIS2CWRF_SBC_d01.nc')
    
    # copy Geog.ID file to First_StaticData/Geog_{gridname}
    Tools.Copy(GeogIDfile, './Geog.ID')

    # copy to PrepCWRF/gridname/Geog_{gridname}
    cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}'
    Tools.Copy(GeogDataPath, f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}')

    logger.info(f'{Consts.S4}◉  Copy Geog Gather Data finished!\n\n')
    os.chdir(old_path)



def Second_ICBC(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    StartTime = casecfg.get(gridname, 'StartTime')
    EndTime = casecfg.get(gridname, 'EndTime')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    Enable_TimeChunk = casecfg.getboolean('BaseInfo', 'Enable_TimeChunk')
    TimeChunkCount = casecfg.getint('BaseInfo', 'TimeChunkCount')
    Go_Ungrib = casecfg.getboolean('PrepCWRF', 'Go_Ungrib')
    Go_Metgrid = casecfg.getboolean('PrepCWRF', 'Go_Metgrid')
    Go_Real = casecfg.getboolean('PrepCWRF', 'Go_Real')
    Go_VBS = casecfg.getboolean('PrepCWRF', 'Go_VBS')
    xesmfenv = envcfg.get('Environment', 'CONDA_XESMF')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')

    # cd to Third_ICBC/Geog_Gather/gridname_GEOG
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Geog_Gather/')

    if (Go_Ungrib) or (Go_Metgrid) or (Go_Real) or (Go_VBS):
        logger.info(f"The steps are: ●  Ungrib 2D -> Ungrib SST -> Ungrib 3D -> mod_levs -> Metgrid -> SST_avg -> Real -> Vbs  ●\n")

        # Link Geog_Gather to Third_ICBC/Geog_Gather
        GeogPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/*'
        Tools.Link(GeogPath, f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Geog_Gather/')

        # run ICBC
        timespan = (EndTime-StartTime).days
        if timespan < 10: # less than 30 days
            if Enable_TimeChunk:
                logger.warning(f'{Consts.S4}The time span is less than 30 days, will not perform slicing.')
                logger.warning(f'{Consts.S4}Please check the Enable_TimeChunk flag in the configuration file.')
                Enable_TimeChunk = False

        if Enable_TimeChunk: # slice process the whole period
            t = time.gmtime(timespan * 4 * 60)
            days = t.tm_yday - 1
            logger.info(f'{Consts.S4}!!! Processing the whole period with slicing and multiprocessing !!!')
            logger.info(f'{Consts.S8}--> It may take {days} days {t.tm_hour} hours {t.tm_min} minutes {t.tm_sec} seconds <--\n')
            timelist = Tools.Split_Days(StartTime, EndTime, TimeChunkCount)
            args = []
            for i in range(TimeChunkCount):
                start_time, end_time = timelist[i]
                args.append((casecfg, envcfg, gridname, start_time, end_time))
                ICBC.Link_CWPS_Files(casecfg, envcfg, gridname, start_time, end_time)

            Workers = TimeChunkCount

            # --- Ungrib --- 
            Tools.Run_Parallel(ICBC.Ungrib, args, Workers, "Ungrib")
            logger.info(f"{Consts.S4}✦  Ungrib Step Complete!")

            # --- Metgrid --- 
            Tools.Run_Parallel(ICBC.Metgrid, args, Workers, "Metgrid")
            logger.info(f"{Consts.S4}✦  Metgrid Step Complete!")
         
            # --- Real --- 
            ICBC.Real(casecfg, envcfg,  gridname, timelist)
            logger.info(f'{Consts.S4}✦  Real Step Complete!')
            
        else: # process the whole period
            logger.info(f'{Consts.S4}!!! Processing the whole period in one go !!!')
            logger.info(f'{Consts.S8}--> It may take {timespan*4} minutes <--\n')
            start_time = StartTime
            end_time = EndTime + timedelta(hours=12) # add 12 hours to include the last time
            timelist = [(start_time, end_time)]
            ICBC.Link_CWPS_Files(casecfg, envcfg, gridname, start_time, end_time)
            # --- Ungrib ---
            ICBC.Ungrib(casecfg, envcfg, gridname, start_time, end_time)
            logger.info(f'{Consts.S4}✦  Ungrib Step Complete!')
            # --- Metgrid ---
            ICBC.Metgrid(casecfg, envcfg, gridname, start_time, end_time)
            logger.info(f'{Consts.S4}✦  Metgrid Step Complete!')
            # --- Real --- 
            ICBC.Real(casecfg, envcfg, gridname, timelist)
            logger.info(f'{Consts.S4}✦  Real Step Complete!')
    else:
        logger.info(f'{Consts.S4}==========> Skip Ungrib <==========')
        logger.info(f'{Consts.S4}==========> Skip Metgrid <==========')
        logger.info(f'{Consts.S4}==========> Skip Real <==========')
        logger.info(f'{Consts.S4}==========> Skip Vbs <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  Third_ICBC Complete!\n\n')

    os.chdir(old_path)  



def Gather_CWRF_Output(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Copy_CWRF_Output = casecfg.getboolean('PrepCWRF', 'Copy_CWRF_Output')
    dx_WE = casecfg.getfloat(gridname, 'dx_WE')
    dy_SN = casecfg.getfloat(gridname, 'dy_SN')
    RefLat = casecfg.getfloat(gridname, 'RefLat')
    RefLon = casecfg.getfloat(gridname, 'RefLon')
    True_Lat1 = casecfg.getfloat(gridname, 'True_Lat1')
    True_Lat2 = casecfg.getfloat(gridname, 'True_Lat2')
    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    NCOPath = envcfg.get('Paths', 'NCOPath')
    xesmfenv = envcfg.get('Environment', 'CONDA_XESMF')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    CWPSNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwps.{gridname}'
    CWRFNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwrf.{gridname}'
    
    if Copy_CWRF_Output:
        logger.info(f'{Consts.S4}==========> Copy PrepCWRF Result <==========')
        st = time.time()
        
        # move wrfbdy and wrfinput files to Result
        wrfinput = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real/wrfinput_d01')
        wrfbdy = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real/wrfbdy_d01')
        wrflowinp_d01 = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real/wrflowinp_d01')
        wrfveg_d01 = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real/wrfveg_d01')
        wrfsst_d01 = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real/wrfsst_d01')
        GeogGatherPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}'
        
        # check if files exist
        Tools.File_Exist(wrfinput, level='error')
        Tools.File_Exist(wrfbdy, level='error')
        Tools.File_Exist(wrflowinp_d01, level='error')
        Tools.File_Exist(wrfveg_d01, level='error')
        Tools.File_Exist(wrfsst_d01, level='error')
        Tools.File_Exist(GeogGatherPath, level='error')
        
        # copy all files to Result
        Tools.Copy(wrfbdy[0], f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/')
        # Tools.Copy(wrfinput[0], f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/')
        Tools.Copy(wrflowinp_d01[0], f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/')
        Tools.Copy(wrfveg_d01[0], f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/')
        Tools.Copy(wrfsst_d01[0], f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/')
        Tools.Copy(CWPSNMLPath, f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/namelist.wps')
        Tools.Copy(CWRFNMLPath, f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/namelist.input')
        Tools.Copy(GeogGatherPath, f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}')
        logger.info(f'{Consts.S4}✓  Copy PrepCWRF Result finished!')
        
        # Post-Process
        logger.info(f'{Consts.S4}==========> PrepCWRF Post-Process <==========')
        os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}')
        directory = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/PostProcess'
        os.makedirs(directory, exist_ok=True)
        os.chdir(directory)
        Tools.Link(f"{ProcessScriptPath}/PrepCWRF/PostCorrect.py", ".")
        # Tools.Link(f"{ProcessScriptPath}/PrepCWRF/chanlu.ncl", ".")
        Tools.Link(f"{ProcessScriptPath}/PrepCWRF/CoLMSoilParams.py", ".")
        Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/ocean_mask.nc", ".")
        Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/MODIS2CWRF_SBC_d01.nc", ".")
        Tools.Copy(f"{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/geo_em.d01_veg.nc", "./geo_em.d01.nc")
        Tools.Copy(wrfinput[0], '.')


        #- Post processing
        # cmd = rf'{NCLPath} chanlu.ncl suffix=\"d01\" > {log_file} 2>&1'
        # Tools.Run_CMD(cmd, "Run chanlu.ncl")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.Post-Process'
        cmd = f'rm -f {log_file}'
        cmd = f'conda run -n {xesmfenv} --no-capture-output python -u PostCorrect.py > {log_file} 2>&1'        
        Tools.Run_CMD(cmd, "Run PostCorrect.py")

        # CoLMSoilParams
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.SoilParams'
        cmd = f'rm -f {log_file}'
        Tools.Run_CMD(cmd, "remove old SoilParams log file")
        cmd  = f'conda run -n {xesmfenv} --no-capture-output python -u CoLMSoilParams.py'
        cmd += f' -dx {dx_WE} -dy {dy_SN} -reflat {RefLat} -reflon {RefLon}'
        cmd += f' -truelat1 {True_Lat1} -truelat2 {True_Lat2} -geofile ./wrfinput_d01'
        cmd += f' -cpu {CWRFCoreNum} -nco {NCOPath} > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run CoLMSoilParams.py")
        logger.info(f'{Consts.S4}✓  Post-Process finished!')

        # copy wrfinput_d01 to Result
        Tools.Copy("./wrfinput_d01*", '../')
        
    else:
        logger.info(f'{Consts.S4}==========> Skip Copy PrepCWRF Result <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!\n\n')
    os.chdir(old_path)



