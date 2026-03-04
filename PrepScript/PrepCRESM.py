#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Module Name   : PrepCRESM (Coupler Preprocessing Module)
Description   : Prepares necessary files for the CRESM coupler (CPL7/CIME).
                
                Key Functions:
                - Coupler_Prep     : Generates mapping weights, domain files,
                                     and namelists for the coupler.

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


def Coupler_Prep(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    NCLPath = envcfg.get('Paths', 'NCLPath')
    xesmfenv = envcfg.get('Environment', 'CONDA_XESMF')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_Coupler_Prep = casecfg.getboolean('PrepCRESM', 'Go_Coupler_Prep')
    StartTime = casecfg.get(gridname, 'StartTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    RootToolBox = envcfg.get('Paths', 'RootToolBox')

    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCRESM/')

    if Go_Coupler_Prep:
        geo_em_veg_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}/geo_em.d01_veg.nc'
        Tools.File_Exist(geo_em_veg_path, level='error')
        # link geo_em.d01_veg.nc 
        Tools.Link(geo_em_veg_path, f'./{gridname}/geo_em.d01.nc')
        Tools.Link(geo_em_veg_path, f'./{gridname}/colm_grd.nc')
        
        # # link unstructured_cwrf
        unstructured_cwrf_path = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/unstructured_cwrf_{gridname}'
        Tools.File_Exist(unstructured_cwrf_path, level='error')
        Tools.Link(unstructured_cwrf_path, f'./{gridname}/')
        
        # link unstructured_cwrf
        Tools.File_Exist(f'{unstructured_cwrf_path}/history/unstructured_cwrf_{gridname}_hist_{StartTime.year}.nc', level='error')
        Tools.Link(f'{unstructured_cwrf_path}/history/unstructured_cwrf_{gridname}_hist_{StartTime.year}.nc', f'./{gridname}/CoLM_ref_{gridname}_vector.nc')
        
        # link CoLM ref data
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/CoLM_ref_{gridname}.nc', level='error')
        Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/CoLM_ref_{gridname}.nc', f'./{gridname}/CoLM_ref_{gridname}.nc')

        # link GLMASK
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/GLMASK.nc', level='error')
        Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/GLMASK.nc', f'./{gridname}/GLMASK_{gridname}_noFVCOM.nc')

        # link htop_rcm
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/htop_rcm.nc', level='error')
        Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/htop_rcm.nc', f'./{gridname}/htop_rcm_{gridname}.nc')

        # link mesh file
        Tools.File_Exist(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/mesh_cwrf_{gridname}.nc', level='error')
        Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/mesh_cwrf_{gridname}.nc', f'./{gridname}/mesh_cwrf_{gridname}.nc')

        # link cpl7 weight generation script
        Tools.Link(f'{ProcessScriptPath}/PrepCRESM/generate_cpl7_wgt_ALO_step1.ncl', f'./generate_cpl7_wgt_ALO_step1.ncl')
        Tools.Link(f'{ProcessScriptPath}/PrepCRESM/generate_cpl7_wgt_ALO_step2.py', f'./generate_cpl7_wgt_ALO_step2.py')
        Tools.Link(f'{RootToolBox}/Data/CRESM_OCEAN/CN_COAST_restart_modified2019.nc', f'./CN_COAST_restart_modified2019.nc')
        
        # generate cpl7 data
        logger.info(f'{Consts.S4}==========> Prep CRESM Step1 <==========')
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.cpldata_step1'
        os.system(f'rm -f {log_file}')
        cmd = rf'{NCLPath} generate_cpl7_wgt_ALO_step1.ncl geogName=\"{gridname}\" > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run generate_cpl7_wgt_ALO_step1.ncl")
        
        logger.info(f'{Consts.S4}==========> Prep CRESM Step2 <==========')
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.cpldata_step2'
        os.system(f'rm -f {log_file}')
        cmd = f'conda run -n {xesmfenv} --no-capture-output python -u generate_cpl7_wgt_ALO_step2.py {gridname} > {log_file} 2>&1' 
        Tools.Run_CMD(cmd, "Run generate_cpl7_wgt_ALO_step2.ncl")
        cmd = f'mv PET0.RegridWeightGen.Log ./cpl7data'
        
        # mv log
        cmd = f'mv ./PET0.RegridWeightGen.Log ./{gridname}/cpl7data'
        Tools.Run_CMD(cmd, "Move log file")

        logger.info(f'{Consts.S4}✓  PrepCRESM finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip PrepCRESM <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  Prepare For CPL7 Data Finished!')
    os.chdir(old_path)
    


