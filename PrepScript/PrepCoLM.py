#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Module Name   : PrepCoLM (CoLM Preprocessing Module)
Description   : Handles the data preparation workflow for the CoLM land surface 
                model component.
                
                Key Functions:
                - First_GenMesh    : Generates unstructured mesh grids.
                - Second_MakeSrf   : Creates surface datasets (mksrf).
                - Second_CoLMIni   : Generates initial conditions.
                - Third_Remap      : Remaps history files or restart files.

Author        : Omarjan @ SYSU
Created       : 2025-05-25
Last Modified : 2026-01-21
===============================================================================
"""

import os
import sys
import time
import glob
import shlex
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Utils import Tools, Consts
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = logging.getLogger("CRESMPrep." + __name__)


def Copy_Exist_CoLMSrf(casecfg, envcfg, gridname, CoLMSrfPath):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    
    PrepCoLMModelPath = f'{CaseOutputPath}/{gridname}/PrepCoLM'

    logger.info(f'{Consts.S4}==========> Copy Existing CoLMSrf <==========')
    st = time.time()
    
    # check if CoLMSrfPath exists
    Tools.File_Exist(CoLMSrfPath, level='error')
    
    # copy Srf files to PrepCoLM/Second_MakeSrf
    landdata = glob.glob(f'{CoLMSrfPath}/landdata')
    print(landdata)
    meshfile = glob.glob(f'{CoLMSrfPath}/mesh_cwrf_*.nc')

    # check if files exist
    Tools.File_Exist(landdata, level='error')
    Tools.File_Exist(meshfile, level='error')
    
    CoLMSrfIDfile = f'{CoLMSrfPath}/CoLMSrf.ID'
    with open(CoLMSrfIDfile, 'r') as f:
        ExitsCoLMSrfID = f.readline().strip()
    logger.info(f'{Consts.S4}->    Exits CoLMSrf ID: {ExitsCoLMSrfID}')

    CoLMSrfID = Tools.Get_Unique_CoLMSrfID(casecfg, envcfg, gridname)
    logger.info(f'{Consts.S4}-> New Case CoLMSrf ID: {CoLMSrfID}')
    if ExitsCoLMSrfID != CoLMSrfID:
        logger.error(f'New Case CoLMSrf ID is not match with the Exits CoLMSrf ID.')
        logger.error(f'!!! Please check your case setting !!!')
        logger.error(f'Different CoLMSrf ID means different surface data, which may cause unexpected results.')
        logger.error(f'If you want to use the existing CoLM surface data, please use the same Geog setting in your case.')
        sys.exit(1)
    else:
        logger.info(f'{Consts.S4}-> Geog ID is match, continue to copy CoLMSrfPath')

    logger.info(f'{Consts.S4}==========> Skip GenMeshGrid <==========')
    logger.info(f'{Consts.S4}==========> Skip MakeCoLMSrf <==========')
    logger.info(f'{Consts.S4}-> CoLMSrfPath: {CoLMSrfPath}')
    logger.info(f'{Consts.S4}-> Copy CoLMSrfPath to {PrepCoLMModelPath}')

    # copy files to PrepCoLM/First_GenMesh
    cmd = f"rm -f {PrepCoLMModelPath}/First_GenMesh/mesh_cwrf_{gridname}.nc"
    Tools.Run_CMD(cmd, "Remove old mesh file")
    Tools.Copy(meshfile[0], f'{PrepCoLMModelPath}/First_GenMesh/mesh_cwrf_{gridname}.nc')

    # copy files to PrepCoLM/Second_MakeSrf
    cmd = f"rm -f {PrepCoLMModelPath}/Second_MakeSrf/mesh_cwrf_{gridname}.nc"
    Tools.Run_CMD(cmd, "Remove old mesh file")
    Tools.Copy(meshfile[0], f'{PrepCoLMModelPath}/Second_MakeSrf/mesh_cwrf_{gridname}.nc')
    cmd = f"rm -rf {PrepCoLMModelPath}/Second_MakeSrf/unstructured_cwrf_{gridname}/landdata"
    Tools.Run_CMD(cmd, "Remove old landdata directory")
    Tools.Copy(landdata[0], f'{PrepCoLMModelPath}/Second_MakeSrf/unstructured_cwrf_{gridname}/landdata')
    
    # clean restart and history files
    cmd = f'rm -rf {PrepCoLMModelPath}/Second_MakeSrf/unstructured_cwrf_{gridname}/restart/*'
    Tools.Run_CMD(cmd, "Remove restart files")
    cmd = f'rm -rf {PrepCoLMModelPath}/Second_MakeSrf/unstructured_cwrf_{gridname}/history/*'
    Tools.Run_CMD(cmd, "Remove history files")

    # clean CoLMSrf_{gridname} folder if exists
    cmd = f'rm -rf {PrepCoLMModelPath}/Second_MakeSrf/CoLMSrf_{gridname}'
    Tools.Run_CMD(cmd, f"Remove old CoLMSrf_{gridname} directory")
    Tools.Copy(CoLMSrfPath, f'{PrepCoLMModelPath}/Second_MakeSrf/CoLMSrf_{gridname}')

    # clean CoLMSrf_{gridname} folder if exists
    cmd = f'rm -rf {PrepCoLMModelPath}/{gridname}/CoLMSrf_{gridname}'
    Tools.Run_CMD(cmd, f"Remove old CoLMSrf_{gridname} directory")
    Tools.Copy(CoLMSrfPath, f'{PrepCoLMModelPath}/{gridname}/CoLMSrf_{gridname}')

    logger.info(f'{Consts.S4}✓  Copy Existing Srf finished!\n\n')
    os.chdir(old_path)



def First_GenMesh(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_MeshGrid = casecfg.getboolean('PrepCoLM', 'Go_MeshGrid')
    dx_WE = casecfg.get(gridname, 'dx_WE')
    dy_SN = casecfg.get(gridname, 'dy_SN')
    RefLat = casecfg.get(gridname, 'RefLat')
    RefLon = casecfg.get(gridname, 'RefLon')
    True_Lat1 = casecfg.get(gridname, 'True_Lat1')
    True_Lat2 = casecfg.get(gridname, 'True_Lat2')
    MeshSize = casecfg.get(gridname, 'MeshSize')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')

    
    geo_em_veg_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}/geo_em.d01_veg.nc'
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/')

    if Go_MeshGrid:
        logger.info(f'{Consts.S4}==========> GenMeshGrid <==========')
        
        # copy GenCWRF2CoLMMesh.py to First_GenMesh
        Tools.Link(f'{ProcessScriptPath}/PrepCoLM/GenCWRF2CoLMMesh.py', f'./GenCWRF2CoLMMesh.py')

        # link geo_em.d01_veg.nc to First_GenMesh
        Tools.Link(f'{geo_em_veg_path}', f'./geo_em.d01_veg.nc')

        # link run script
        cmd = f'conda run -n {cresmenv} --no-capture-output python -u GenCWRF2CoLMMesh.py -dx {dx_WE} -dy {dy_SN} -reflat {RefLat} -reflon {RefLon} -truelat1 {True_Lat1} -truelat2 {True_Lat2} -meshsize {MeshSize} -geofile ./geo_em.d01_veg.nc -savename ./mesh_cwrf_{gridname}.nc'
        Tools.Run_CMD(cmd, "Run GenCWRF2CoLMMesh.py")
        
        logger.info(f'{Consts.S4}✓  GenMeshGrid finished!')
        if Tools.File_Exist(f'./mesh_cwrf_{gridname}.nc'):
            logger.info(f'{Consts.S4}-> mesh_cwrf_{gridname}.nc created!')
        else:
            logger.warning(f'{Consts.S4}mesh_cwrf_{gridname}.nc not found!')
    else:
        logger.info(f'{Consts.S4}==========> Skip GenMeshGrid <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
        if not Tools.File_Exist(f'./mesh_cwrf_{gridname}.nc', level='warning'):
            logger.warning(f'{Consts.S4}mesh_cwrf_{gridname}.nc not found!')
    logger.info(f'{Consts.S4}◉  Generate Mesh Grid Complete!\n\n')
    os.chdir(old_path)



def Second_MakeSrf(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_MakeSrf = casecfg.getboolean('PrepCoLM', 'Go_MakeSrf')
    CoLMCoreNum = casecfg.getint('PrepCoLM', 'CoLMCoreNum')
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CoLMModelPath = envcfg.get('Paths', 'CoLMModelPath')
    CoLMForcingPath = envcfg.get('Paths', 'CoLMForcingPath')
    ForcingName = os.path.basename(os.path.abspath(CoLMForcingPath))
    SYS_CoLM = envcfg.get('Environment', 'SYS_CoLM')
   
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/')
   
    if (Go_MakeSrf) :
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc', level='error')

        # link colm executable files
        Tools.Link(f'{CoLMModelPath}/run/mksrfdata.x', './mksrfdata.x')
        Tools.Link(f'{CoLMModelPath}/run/mkinidata.x', './mkinidata.x')
        Tools.Link(f'{CoLMModelPath}/run/colm.x', './colm.x')
        
        # copy namelist files
        logger.debug(f'ForcingName: {ForcingName}')
        Tools.Copy(f'{CaseOutputPath}/{gridname}/NMLS/unstructured_cwrf.colm.{gridname}.icbc', './unstructured_cwrf.colm.nml')
        Tools.Copy(f'{ScriptPath}/NML/CoLM_Forcing/{ForcingName}.nml', f'./{ForcingName}.nml')
        Tools.Copy(f'{ScriptPath}/NML/history.colm.ctl', './history.nml')
        
        #replace forcingdir to Forcing Path
        cmd = f'sed -i "s|forcingdir|{CoLMForcingPath}|g" {ForcingName}.nml'
        Tools.Run_CMD(cmd, f"Replace forcingdir in {ForcingName}.nml")
        
        # link mesh file
        Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc", f'./mesh_cwrf_{gridname}.nc')
    
        # ---------------------- MakeCoLMSrf ----------------------
        logger.info(f'{Consts.S4}==========> MakeCoLMSrf <==========')
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.mksrf'
        cmd = f'rm -f {log_file}'
        Tools.Run_CMD(cmd, "Remove old log file")
        cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/landdata/*'
        Tools.Run_CMD(cmd, "Remove old landdata directory")
        cmd = f'mpirun -np {CoLMCoreNum} ./mksrfdata.x unstructured_cwrf.colm.nml > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run mksrfdata.x", env=SYS_CoLM)
        files = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/landdata/*')
        if len(files) == 0:
            logger.error(f'{Consts.S4}unstructured_cwrf_{gridname}/landdata/* not found!')
            logger.error(f'{Consts.S4}Please check the Second_MakeSrf part')
            sys.exit(1)
        logger.info(f'{Consts.S4}-> landdata files :')
        logger.info(f'{Consts.S8}{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/landdata/*')
        for file in files:
            logger.info(F'{Consts.S8}unstructured_cwrf_{gridname}/landdata/{os.path.basename(file)}')
            
        # Get unique Geog ID
        CoLMSrfID = Tools.Get_Unique_CoLMSrfID(casecfg, envcfg, gridname)
        logger.info(f'{Consts.S4}-> CoLMSrf ID: {CoLMSrfID}')
        CoLMSrfIDfile = f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}/CoLMSrf.ID'
        cmd = f'rm -f {CoLMSrfIDfile}'
        Tools.Run_CMD(cmd, f"Remove old CoLMSrf ID file {CoLMSrfIDfile}")            
        with open(CoLMSrfIDfile, 'w') as f:
            f.write(f'{CoLMSrfID}\n')
            
        # Copy CoLMSrf_{gridname} to CoLMSrf_{gridname}
        cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}/landdata'
        Tools.Run_CMD(cmd, f"Remove old landdata directory in CoLMSrf_{gridname}")
        Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/landdata', f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}/')
        
        cmd = f'rm -f {CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}/mesh_cwrf_{gridname}.nc'
        Tools.Run_CMD(cmd, f"Remove old mesh file in CoLMSrf_{gridname}")
        Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc', f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}/')

        cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/CoLMSrf_{gridname}'
        Tools.Run_CMD(cmd, f"Remove old CoLMSrf_{gridname} directory")
        Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/CoLMSrf_{gridname}')

        logger.info(f'{Consts.S4}✓  Make CoLM surface data finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip MakeCoLMSrf <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  Make CoLM Surface Data Complete!\n\n')
    os.chdir(old_path)



def Second_CoLMIni(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_MakeIni = casecfg.getboolean('PrepCoLM', 'Go_MakeIni')
    CoLMCoreNum = casecfg.getint('PrepCoLM', 'CoLMCoreNum')
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CoLMModelPath = envcfg.get('Paths', 'CoLMModelPath')
    CoLMForcingPath = envcfg.get('Paths', 'CoLMForcingPath')
    ForcingName = os.path.basename(os.path.abspath(CoLMForcingPath))
    SYS_CoLM = envcfg.get('Environment', 'SYS_CoLM')

    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/')
   
    if (Go_MakeIni):
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc', level='error')

        # link colm executable files
        Tools.Link(f'{CoLMModelPath}/run/mksrfdata.x', './mksrfdata.x')
        Tools.Link(f'{CoLMModelPath}/run/mkinidata.x', './mkinidata.x')
        Tools.Link(f'{CoLMModelPath}/run/colm.x', './colm.x')
        
        # copy namelist files
        Tools.Copy(f'{CaseOutputPath}/{gridname}/NMLS/unstructured_cwrf.colm.{gridname}.icbc', './unstructured_cwrf.colm.nml')
        Tools.Copy(f'{ScriptPath}/NML/CoLM_Forcing/{ForcingName}.nml', f'./{ForcingName}.nml')
        Tools.Copy(f'{ScriptPath}/NML/history.colm.ctl', './history.nml')
        
        #replace forcingdir to Forcing Path
        cmd = f'sed -i "s|forcingdir|{CoLMForcingPath}|g" {ForcingName}.nml'
        Tools.Run_CMD(cmd, f"Replace forcingdir in {ForcingName}.nml")

        # link mesh file
        Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc", f'./mesh_cwrf_{gridname}.nc')
        
        # ---------------------- MakeCoLMIni ----------------------
        logger.info(f'{Consts.S4}==========> MakeCoLMIni <==========')
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.mkini'
        cmd = f'rm -f {log_file}'
        Tools.Run_CMD(cmd, "Remove old log file")
        cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/restart/*'
        Tools.Run_CMD(cmd, "Remove old restart directory")
        cmd = f'mpirun -np {CoLMCoreNum} ./mkinidata.x unstructured_cwrf.colm.nml > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run mkinidata.x", env=SYS_CoLM)
        files = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/restart/*')
        if files == 0:
            logger.error(f'{Consts.S4}unstructured_cwrf_{gridname}/restart/* not found!')
            logger.error(f'{Consts.S4}Please check the Second_MakeSrf part')
            sys.exit(1)
        logger.info(f'{Consts.S4}-> restart files:')
        logger.info(f'{Consts.S8}{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/restart/*')
        for file in files:
            logger.info(f'{Consts.S8}unstructured_cwrf_{gridname}/restart/{os.path.basename(file)}')

        logger.info(f'{Consts.S4}✓  Make CoLM initial data finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip MakeCoLMIni <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  CoLM Initialization Complete!\n\n')
    os.chdir(old_path)



def Second_CoLMRun(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_CoLMTempRun = casecfg.getboolean('PrepCoLM', 'Go_CoLMTempRun')
    CoLMCoreNum = casecfg.getint('PrepCoLM', 'CoLMCoreNum')
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CoLMModelPath = envcfg.get('Paths', 'CoLMModelPath')
    CoLMForcingPath = envcfg.get('Paths', 'CoLMForcingPath')
    ForcingName = os.path.basename(os.path.abspath(CoLMForcingPath))
    SYS_CoLM = envcfg.get('Environment', 'SYS_CoLM')
   
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/')
   
    if (Go_CoLMTempRun):
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc', level='error')

        # link colm executable files
        Tools.Link(f'{CoLMModelPath}/run/mksrfdata.x', './mksrfdata.x')
        Tools.Link(f'{CoLMModelPath}/run/mkinidata.x', './mkinidata.x')
        Tools.Link(f'{CoLMModelPath}/run/colm.x', './colm.x')
        
        # copy namelist files
        Tools.Copy(f'{CaseOutputPath}/{gridname}/NMLS/unstructured_cwrf.colm.{gridname}.icbc', './unstructured_cwrf.colm.nml')
        Tools.Copy(f'{ScriptPath}/NML/CoLM_Forcing/{ForcingName}.nml', f'./{ForcingName}.nml')
        Tools.Copy(f'{ScriptPath}/NML/history.colm.ctl', './history.nml')
        
        #replace forcingdir to Forcing Path
        cmd = f'sed -i "s|forcingdir|{CoLMForcingPath}|g" {ForcingName}.nml'
        Tools.Run_CMD(cmd, f"Replace forcingdir in {ForcingName}.nml")
        
        # link mesh file
        Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc", f'./mesh_cwrf_{gridname}.nc')

        # ---------------------- CoLMRun ----------------------
        logger.info(f'{Consts.S4}==========> CoLMRun <==========')
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.colm'
        cmd = f'rm -f {log_file}'
        Tools.Run_CMD(cmd, "Remove old log file")
        cmd = f'rm -rf {CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/history/*'
        Tools.Run_CMD(cmd, "Remove old history directory")
        cmd = f'mpirun -np {CoLMCoreNum} ./colm.x unstructured_cwrf.colm.nml > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run colm.x", env=SYS_CoLM)
        files = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/history/*')
        if len(files) == 0:
            logger.error(f'{Consts.S4}unstructured_cwrf_{gridname}/history/* not found!')
            logger.error(f'{Consts.S4}Please check the Second_MakeSrf part')
            sys.exit(1)
        logger.info(f'{Consts.S4}-> history files:')
        logger.info(f'{Consts.S8}{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/history/*')
        for file in files:
            logger.info(f'{Consts.S8}unstructured_cwrf_{gridname}/history/{os.path.basename(file)}')

        logger.info(f'{Consts.S4}✓  Run CoLM Finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip CoLMRun <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  CoLM Temporary Run Complete!\n\n')
    os.chdir(old_path)



def Third_Remap(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CoLMModelPath = envcfg.get('Paths', 'CoLMModelPath')
    CoLMForcingPath = envcfg.get('Paths', 'CoLMForcingPath')
    NCLPath = envcfg.get('Paths', 'NCLPath')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CoLMCoreNum = casecfg.getint('PrepCoLM', 'CoLMCoreNum')
    Go_Remap = casecfg.getboolean('PrepCoLM', 'Go_Remap')
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    StartTime = casecfg.get(gridname, 'StartTime')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')

    
    if Go_Remap:
        #---------------------- Remap CoLM History File ----------------------
        logger.info(f'{Consts.S4}==========> Remap History File <==========')
        os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/')
        
        histfiles = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}/history/*')
        if len(histfiles) == 0:
            logger.error(f'{Consts.S4}unstructured_cwrf_{gridname}/history/* not found!')
            logger.error(f'{Consts.S4}Please check the Second_MakeSrf part')
            raise FileNotFoundError(f'{Consts.S4}unstructured_cwrf_{gridname}/history/* not found!')
        
        geo_em_veg_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}/geo_em.d01_veg.nc'
        Tools.File_Exist(geo_em_veg_path, level='error')

        # link geo_em.d01_veg.nc to Third_Remap
        Tools.Link(geo_em_veg_path, f'./geo_em.d01_veg.nc')

        # copy unstructured_cwrf_{gridname} files
        cmd = f'cp -r {CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname} .'
        Tools.Run_CMD(cmd, "Copy Make CoLM Srf Result")
        Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{gridname}', '.')

        # link CoLM_Remap.py
        Tools.Link(f'{ProcessScriptPath}/PrepCoLM/CoLM_Remap.py', f'./unstructured_cwrf_{gridname}/history/CoLM_Remap.py')

        # link generate_glmask_htop.ncl
        Tools.Link(f'{ProcessScriptPath}/PrepCoLM/generate_glmask_htop.ncl', './generate_glmask_htop.ncl')
        
        # cd to unstructured_cwrf_{gridname}/history
        os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/unstructured_cwrf_{gridname}/history/')
        
        # remap
        cmd = f'conda run -n {cresmenv} --no-capture-output python -u CoLM_Remap.py -lons {int(EdgeNum_WE)-1} -lats {int(EdgeNum_SN)-1} -cpus 1'
        Tools.Run_CMD(cmd, "Run CoLM_Remap.py")
        remapfiles = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/unstructured_cwrf_{gridname}/history/unstructured_cwrf_{gridname}_hist_{StartTime.year}_remap.nc')
        if len(remapfiles) == 0:
            logger.error(f'{Consts.S4}unstructured_cwrf_{gridname}/history/unstructured_cwrf_{gridname}_hist_{StartTime.year}_remap.nc not found!')
            logger.error(f'{Consts.S4}Please check the Third_Remap part')
            raise FileNotFoundError(f'{Consts.S4}unstructured_cwrf_{gridname}/history/unstructured_cwrf_{gridname}_hist_{StartTime.year}_remap.nc not found!')
        for file in remapfiles:
            Tools.File_Exist(f'{file}', level='error')

        # cd Third_Remap
        os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/')

        # cp remap file
        file = remapfiles[0] # get the first remap file
        Tools.Copy(file, f'./CoLM_ref_{gridname}.nc')
        # Tools.Copy(f'./unstructured_cwrf_{gridname}/landdata/diag/htop_patch_2020.nc', f'./htop_patch_2020_{gridname}.nc')

        # generate glmask
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.remap'
        os.system(f'rm -f {log_file}')
        cmd = rf'{NCLPath} generate_glmask_htop.ncl geogName=\"{gridname}\" > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run generate_glmask_htop.ncl")
        Tools.File_Exist(f'./GLMASK.nc', level='error')
        
        logger.info(f'{Consts.S4}✓  Remap CoLM History File Finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip Remap History File <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    
    logger.info(f'{Consts.S4}◉  Remap History File Complete!\n\n')
    os.chdir(old_path)



def CopyPrepCoLMResult(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Copy_CoLM_Output = casecfg.getboolean('PrepCoLM', 'Copy_CoLM_Output')
    
    if Copy_CoLM_Output:
        logger.info(f'{Consts.S4}==========> Copy PrepCoLM Result <==========')
        os.chdir(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')

        # move ColM result files to Result
        CoLMref = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/CoLM_ref_{gridname}.nc')
        glmask = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/GLMASK.nc')
        # htop_patch = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/htop_patch_2020_{gridname}.nc')
        htop_rcm = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/htop_rcm.nc')
        runcase = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/Third_Remap/unstructured_cwrf_{gridname}')
        meshfile = glob.glob(f'{CaseOutputPath}/{gridname}/PrepCoLM/First_GenMesh/mesh_cwrf_{gridname}.nc')
        
        # check if files exist
        Tools.File_Exist(CoLMref, level='error')
        Tools.File_Exist(glmask, level='error')
        # Tools.File_Exist(htop_patch, level='error')
        Tools.File_Exist(htop_rcm, level='error')
        Tools.File_Exist(runcase, level='error')
        Tools.File_Exist(meshfile, level='error')

        # copy all files to Result
        Tools.Copy(f'{CoLMref[0]}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')
        Tools.Copy(f'{glmask[0]}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')
        # Tools.Copy(f'{htop_patch[0]}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')
        Tools.Copy(f'{htop_rcm[0]}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')
        Tools.Copy(f'{runcase[0]}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')
        Tools.Copy(f'{meshfile[0]}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')
        Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}', f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/')  
        logger.info(f'{Consts.S4}✓  Copy PrepCoLM Result finished!')
        
    else:
        logger.info(f'{Consts.S4}==========> Skip Copy PrepCoLM Result <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    
    logger.info(f'{Consts.S4}◉  Copy PrepCoLM Result Complete!\n\n')
    os.chdir(old_path)


