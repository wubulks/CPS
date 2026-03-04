"""
Author: Omarjan @ SYSU 2025-05-25

conda environment:
    cresm: /home/wumej22/anaconda3/envs/cresm
    xesmf: /home/wumej22/anaconda3/envs/xesmf
    Chaomodis: /home/wumej22/anaconda3/envs/Chaomodis
"""

import os
import sys
import time
import glob
import logging
import argparse
import subprocess
import configparser
import multiprocessing
from datetime import datetime, timedelta


# ==========> Global Variables <==========
S4 = " "*4
S6 = " "*6
S8 = " "*8
cresmenv = 'cresm'
xesmfenv = 'cresm_xesmf'
chaomodisenv = 'Chaomodis'


# =========> Functions <==========
def ReadConfig():
    filepath = f'./SpinUp.ini'
    config = configparser.ConfigParser()
    config.read(filepath)
    logging.info(f'{S4}Read configuration file: {filepath}\n\n')
    return config



def PrintUsefulCase(config):
    """
    Print the case name in the configuration file
    """
    print("\n  Useful Case Name:\n")
    sections = config.sections()
    sections.remove('BaseInfo')
    sectionslist = []
    sections = sorted(sections)
    for i, section in enumerate(sections):
        print(f"{S4}[-] {section}\n")
        sectionslist.append(section)
    return sectionslist



def PrintConfigHelp():
    """
    Print help information for the configuration file
    """
    print("\n")
    print("--help: Print help information for the configuration file --")
    print("\n")
    print("[BaseInfo] (Must contain)")
    print("  - CoLMCoreNum      : The number of cores for CoLM")
    print("  - ScriptPath       : The path to the script directory")
    print("  - CoLMRawDataPath  : The path to the raw data for CoLM")
    print("  - CoLMRunDataPath  : The path to the run data for CoLM")
    print("  - CoLMForcDataPath : The path to the forcing data for CoLM")
    print("  - CleanTempFiles   : Whether to clean temporary files (True/False)")
    print("\n")
    print("[GridName] (e.g. Case1)")
    print("  - CoLMCasePath     : The path to the case directory")
    print("  - MeshFilePath     : The path to the mesh file")
    print("  - SpinUpStartTime  : The start time for the spin-up period (format: YYYY-MM-DD_HH:MM:SS)")
    print("  - SpinUpEndTime    : The end time for the spin-up period (format: YYYY-MM-DD_HH:MM:SS)")
    print("  - TimeStep         : The time step for CoLM (in seconds)")
    print("  - WriteRestartFreq : The frequency of writing restart files (e.g. YEARLY, MONTHLY, DAILY, HOURLY)")
    print("  - WriteHistoryFreq : The frequency of writing history files (e.g. YEARLY, MONTHLY, DAILY, HOURLY)")
    print("\n")
    print("Note: the spin-up period is divided into three parts, and the WriteRestartFreq and WriteHistoryFreq should be set for each part.")
    print("      The three parts are defined as follows:")
    print("")
    print("                Year          Month        Day   ->      Year      Month          Day  ")
    print("           |----------|-------------|------------|----------|-------------|-----------|")
    print("   First :  StartYear - SStartMonth - StartDay  ->  EndYear -         Jan -        01  ")
    print("   Second:    EndYear -         Jan -       01  ->  EndYear -    EndMonth -        01  ")
    print("   Three :    EndYear -    EndMonth -       01  ->  EndYear -    EndMonth -    EndDay  ")
    print("")
    print("  e.g.:                                                                                      ")
    print("   SpinUpStartTime  = 1990-01-01_00:00:00                                                    ")
    print("   SpinUpEndTime    = 2000-03-16_00:00:00                                                    ")
    print("   WriteRestartFreq = YEARLY, MONTHLY, DAILY                                                 ")
    print("   WriteHistoryFreq = YEARLY, MONTHLY, MONTHLY                                               ")
    print("")
    print("             Period:     | -------- First --------- | ---- Second ---- | - Three - |          ")
    print("               Time: 1990-01-01                 2000-01-01         2000-03-01  2000-03-16     ") 
    print("                         ●--------------------------●------------------●-----------●          ")
    print("   WriteRestartFreq:     ●          YEARLY          ●      MONTHLY     ●   DAILY   ●          ")
    print("   WriteHistoryFreq:     ●          YEARLY          ●      MONTHLY     ●  MONTHLY  ●          ")
    print("\n")
    print("   Note: If the SpinUpEndTime is the first month of the year, the second period will not run.")
    print("   Note: If the SpinUpEndTime is the first day of the month, the third period will not run.")
    print("\n")
    sys.exit(0)



def PrintTimePeriod(config, gridname):
    StartTime = config.get(gridname, 'SpinUpStartTime')
    EndTime = config.get(gridname, 'SpinUpEndTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    WriteRestartFreq = config.get(gridname, 'WriteRestartFreq')
    WriteRestartFreq = WriteRestartFreq.split(',')
    WriteRestartFreq = [Freq.strip() for Freq in WriteRestartFreq]
    WriteHistoryFreq = config.get(gridname, 'WriteHistoryFreq')
    WriteHistoryFreq = WriteHistoryFreq.split(',')
    WriteHistoryFreq = [Freq.strip() for Freq in WriteHistoryFreq]
    
    FirstStartTime = StartTime
    FirstEndTime = datetime(EndTime.year, 1, 1, EndTime.hour, EndTime.minute, EndTime.second)
    if FirstStartTime.year == EndTime.year:
        WriteRestartFreq[0] = ' * SKIP *'
        WriteHistoryFreq[0] = ' * SKIP *'
    SecondStartTime = FirstEndTime
    SecondEndTime = datetime(EndTime.year, EndTime.month, 1, EndTime.hour, EndTime.minute, EndTime.second)        
    if SecondStartTime.month == EndTime.month:
        WriteRestartFreq[1] = ' * SKIP *'
        WriteHistoryFreq[1] = ' * SKIP *'
    ThirdStartTime = SecondEndTime
    ThirdEndTime = EndTime
    if ThirdStartTime.day == EndTime.day:
        WriteRestartFreq[2] = ' * SKIP *'
        WriteHistoryFreq[2] = ' * SKIP *'
    Time1 = f"{FirstStartTime.year}-{FirstStartTime.month:02d}-{FirstStartTime.day:02d}"
    Time2 = f"{SecondStartTime.year}-{SecondStartTime.month:02d}-{SecondStartTime.day:02d}"
    Time3 = f"{ThirdStartTime.year}-{ThirdStartTime.month:02d}-{ThirdStartTime.day:02d}"
    Time4 = f"{ThirdEndTime.year}-{ThirdEndTime.month:02d}-{ThirdEndTime.day:02d}"
    
    logging.info("")
    logging.info(f"          Period:     | --------- First ---------- | ----- Second ----- | --- Three --- |")
    logging.info(f"            Time: {Time1}                   {Time2}           {Time3}      {Time4}") 
    logging.info(f"                      ●----------------------------●--------------------●---------------●")
    logging.info(f"WriteRestartFreq:     ●{WriteRestartFreq[0].center(28)}●{WriteRestartFreq[1].center(20)}●{WriteRestartFreq[2].center(15)}●")
    logging.info(f"WriteHistoryFreq:     ●{WriteHistoryFreq[0].center(28)}●{WriteHistoryFreq[1].center(20)}●{WriteHistoryFreq[2].center(15)}●\n\n")




def RunCMD(cmd, description=None):
    """
    Execute a command and check if it is successful
    """
    if description:
        logging.debug(description)
    logging.debug(f"Executing command: {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if result.returncode != 0:
        logging.error(f"Command failed: {cmd}")
        logging.error(f"Error output: {result.stderr}")
        if '2>&1' in cmd:
            cmd1 = cmd.replace('2>&1', '')
            log_file = cmd1.split('>')[-1].strip()
            os.system(f'tail {log_file}')
        logging.error(f"")
        logging.error(f"Error at: {os.getcwd()}")
        sys.exit(1)  # Exit the program if the command fails
    else:
        logging.debug(f"Command output: {result.stdout}")



def FileExist(filepath, level=None):
    """
    Check if a file exists
    """
    if not os.path.exists(filepath):
        if level == 'error':
            logging.error(f"File not found: {filepath}")
            sys.exit(1)
        elif level == 'warning':
            logging.warning(f"{S4}File not found: {filepath}")
        else:
            logging.info(f"{S4}File not found: {filepath}")
        return False
    else:
        logging.debug(f"{S4}File exists: {filepath}")
        return True



def CheckConfig(config, gridname):
    """
    Check the configuration file
    """
    logging.info(f'{S4}==========> Check Configuration <==========')
    # Check Sections
    MustExistsSections = ['BaseInfo', gridname]
    for section in MustExistsSections:
        if not config.has_section(section):
            logging.error(f"{S4}Section not found: {section}")
            sys.exit(1)
        formatted_key = 'Section'.ljust(17)
        logging.info(f"{S6}{formatted_key}: {section}")
        
    # Check Booleans
    MustBoolean = { 'CleanTempFiles': 'BaseInfo', 'GoFirstPeriod': 'SpinUp',
                    'GoSecondPeriod': 'SpinUp', 'GoThirdPeriod': 'SpinUp' ,
                    'GoMakeIni': 'SpinUp'}
    section_order = ['BaseInfo', gridname]
    # 构造排序后的字典（忽略不在排序列表中的）
    sorted_items = sorted(
        MustBoolean.items(),
        key=lambda item: section_order.index(item[1]) if item[1] in section_order else len(section_order)
    )
    # 执行逻辑
    for key, section in sorted_items:
        try:
            config.getboolean(section, key)
        except ValueError:
            logging.error(f"{S4}{key} is {config.get(section, key)}")
            logging.error(f"{S4}{key} must be True or False.")
            sys.exit(1)
        formatted_key = key.ljust(17)
        logging.info(f"{S6}{formatted_key}: {config.getboolean(section, key)}")
    
    # Check Path
    MustExistsPath = {'CoLMCasePath':gridname, 'ScriptPath':'BaseInfo', 'MeshFilePath':gridname, 
                      'CoLMRawDataPath':'BaseInfo', 'CoLMRunDataPath':'BaseInfo', 'CoLMForcDataPath':'BaseInfo'}
    for key, section in MustExistsPath.items():
        FileExist(config.get(section, key), level='error')
        formatted_key = key.ljust(17) 
        logging.info(f"{S6}{formatted_key}: {config.get(section, key)}")
    
    # Check script path
    ScriptPath = config.get('BaseInfo', 'ScriptPath')
    NMLPath = f"{ScriptPath}/NML"
    CtlCoLMNML = f"{ScriptPath}/NML/unstructured_cwrf.colm.ctl"
    CtlHistNML = f"{ScriptPath}/NML/history.colm.ctl"
    filelist = [f'{NMLPath}',f'{CtlCoLMNML}', f'{CtlHistNML}', ]
    for file in filelist:
        FileExist(file, level='error')
    
    # Check Time
    StartTime = config.get(gridname, 'SpinUpStartTime')
    EndTime = config.get(gridname, 'SpinUpEndTime')
    try:
        datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
        datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    except ValueError:
        logging.error(f"{S4}Time format error: {StartTime}, {EndTime}")
        logging.error(f"{S4}Time format must be like: 2021-01-01_00:00:00")
        sys.exit(1)
    if StartTime >= EndTime:
        logging.error(f"{S4}SpinUpStartTime must be less than SpinUpEndTime.")
        sys.exit(1)
    key = 'Time'
    formatted_key = key.ljust(17)
    logging.info(f"{S6}{formatted_key}: {StartTime} - {EndTime}")
    
    # Check Integers
    MustGT0 = {'CoLMCoreNum':'BaseInfo','TimeStep': gridname}
    for key, section in MustGT0.items():
        if config.getint(section, key) <= 0:
            logging.error(f"{S4}{key} must be greater than 0.")
            sys.exit(1)
        formatted_key = key.ljust(17) 
        logging.info(f"{S6}{formatted_key}: {config.getint(section ,key)}")
    
    # Check the frequency of writing restart files
    MustInList = ['YEARLY', 'MONTHLY', 'DAILY', 'HOURLY']
    FreqDict = {'WriteRestartFreq': gridname, 'WriteHistoryFreq': gridname}
    for key in FreqDict.keys():
        WriteFreq = config.get(FreqDict[key], key)
        WriteFreq = WriteFreq.split(',')
        WriteFreq = [Freq.strip() for Freq in WriteFreq]
        if len(WriteFreq) != 3:
            logging.error(f"{S4}WriteFreq must be a list of 3 items.")
            sys.exit(1)
        for item in WriteFreq:
            if item not in MustInList:
                logging.error(f"{S4}WriteFreq must be in {MustInList}.")
                sys.exit(1)
        formatted_key = key.ljust(17)
        logging.info(f"{S6}{formatted_key}: {', '.join(WriteFreq)}")
    WriteFreq = config.get(gridname, 'WriteRestartFreq')
    WriteFreq = WriteFreq.split(',')
    WriteFreq = [Freq.strip() for Freq in WriteFreq]
    if WriteFreq[1] in ['YEARLY'] :
        logging.error(f'{S4} The WriteRestartFreq for the second period must not be YEARLY.')
        logging.error(f'{S4} It is too long for the second period, please set it to MONTHLY or DAILY or HOURLY.') 
    if WriteFreq[2] in ['YEARLY', 'MONTHLY'] :              
        logging.error(f'{S4} The WriteRestartFreq for the third period must not be YEARLY or MONTHLY.')
        logging.error(f'{S4} It is too long for the third period, please set it to DAILY or HOURLY.')                

    # Check the forcing name
    MustInList = ['PRINCETON', 'GSWP3', 'QIAN', 'CRUNCEPV4', 'CRUNCEPV7',
                   'ERA5LAND', 'ERA5', 'MSWX', 'WFDE5', 'CRUJRA',
                   'WFDEI', 'JRA55', 'GDAS', 'CMFD']
    ForcingDataName = config.get(gridname, 'ForcingDataName')
    if ForcingDataName not in MustInList:
        logging.error(f"{S4}ForcingDataName must be in {MustInList}.")
        sys.exit(1)
    formatted_key = 'ForcingDataName'.ljust(17)
    logging.info(f"{S6}{formatted_key}: {ForcingDataName}")
    logging.info(f'{S4}✓  Check configuration file finished!\n\n')



def make_dir(config, gridname):
    """
    Make directories for the case
    """
    CoLMCasePath = config.get(gridname, 'CoLMCasePath')
    directories = [
        f'{CoLMCasePath}/tmpdir',
    ]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            logging.debug(f"{S4}Created directory: {directory}")
        else:
            logging.debug(f"{S4}Directory already exists: {directory}")
    logging.info(f"{S4}Directories for case < {gridname} > created.\n\n")



def ModifyCoLMNML(config, gridname, run_type):
    ScriptPath = config.get('BaseInfo', 'ScriptPath')
    CoLMCasePath = config.get(gridname, 'CoLMCasePath')
    CtlCoLMNML = f"{ScriptPath}/NML/unstructured_cwrf.colm.ctl"
    CoLMRawDataPath = config.get('BaseInfo', 'CoLMRawDataPath')
    CoLMRunDataPath = config.get('BaseInfo', 'CoLMRunDataPath')
    StartTime = config.get(gridname, 'SpinUpStartTime')
    EndTime = config.get(gridname, 'SpinUpEndTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    WriteRestartFreq = config.get(gridname, 'WriteRestartFreq')
    WriteRestartFreq = WriteRestartFreq.split(',')
    WriteRestartFreq = [Freq.strip() for Freq in WriteRestartFreq]
    WriteHistoryFreq = config.get(gridname, 'WriteHistoryFreq')
    WriteHistoryFreq = WriteHistoryFreq.split(',')
    WriteHistoryFreq = [Freq.strip() for Freq in WriteHistoryFreq]
    MeshFilePath = config.get(gridname, 'MeshFilePath')
    TimeStep = config.getint(gridname, 'TimeStep')
    ForcingDataName = config.get(gridname, 'ForcingDataName')
    start_seconds = StartTime.hour * 3600 + StartTime.minute * 60 + StartTime.second
    end_seconds = EndTime.hour * 3600 + EndTime.minute * 60 + EndTime.second
    TimeStep = config.getint(gridname, 'TimeStep')
    
    if run_type == 'First':
        StartTime = StartTime
        EndTime = datetime(EndTime.year, 1, 1, EndTime.hour, EndTime.minute, EndTime.second)
        if StartTime.year == EndTime.year:
            config.set('SpinUp', 'GoFirstPeriod', 'False')
    elif run_type == 'Second':
        StartTime = datetime(EndTime.year, 1, 1, EndTime.hour, EndTime.minute, EndTime.second)
        EndTime = datetime(EndTime.year, EndTime.month, 1, EndTime.hour, EndTime.minute, EndTime.second)        
        if StartTime.month == EndTime.month:
            config.set('SpinUp', 'GoSecondPeriod', 'False')
    elif run_type == 'Third':
        StartTime = datetime(EndTime.year, EndTime.month, 1, EndTime.hour, EndTime.minute, EndTime.second)
        EndTime = EndTime
        if StartTime.day == EndTime.day:
            config.set('SpinUp', 'GoThirdPeriod', 'False')
    
    if (run_type == 'First') and (config.getboolean('SpinUp', 'GoFirstPeriod') is False):
        logging.info(f"{S4}** The SpinUpStartTime and SpinUpEndTime are in the same year, so the first period will not run.")
        return config
    elif (run_type == 'Second') and (config.getboolean('SpinUp', 'GoSecondPeriod') is False):
        logging.info(f"{S4}** The SpinUpStartTime is the first month of the year, so the second period will not run.")
        return config
    elif (run_type == 'Third') and (config.getboolean('SpinUp', 'GoThirdPeriod') is False):
        logging.info(f"{S4}** The SpinUpStartTime is the first day of the month, so the third period will not run.")
        return config
    
    # Check if the CoLM namelist file exists
    FileExist(CtlCoLMNML, level='error')

    with open(CtlCoLMNML, 'r') as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if 'CASENAME' in line:
            lines[i] = lines[i].replace('CASENAME', f'unstructured_cwrf_{gridname}')
        elif 'SYEAR' in line:
            lines[i] = lines[i].replace('SYEAR', f'{StartTime.year}')
        elif 'SMONTH' in line:
            lines[i] = lines[i].replace('SMONTH', f'{StartTime.month}')
        elif 'SDAY' in line:
            lines[i] = lines[i].replace('SDAY', f'{StartTime.day}')
        elif 'SSEC' in line:
            lines[i] = lines[i].replace('SSEC', f'{start_seconds}')
        elif 'EYEAR' in line:
            lines[i] = lines[i].replace('EYEAR', f'{EndTime.year}')
        elif 'EMONTH' in line:
            lines[i] = lines[i].replace('EMONTH', f'{EndTime.month}')
        elif 'EDAY' in line:
            lines[i] = lines[i].replace('EDAY', f'{EndTime.day}')
        elif 'ESEC' in line:
            lines[i] = lines[i].replace('ESEC', f'{end_seconds}')
        elif 'COLMTIMESTEP' in line:
            lines[i] = lines[i].replace('COLMTIMESTEP', f'{TimeStep}')
        elif 'COLMRAWDATA' in line:
            lines[i] = lines[i].replace('COLMRAWDATA', f'{CoLMRawDataPath}/')    
        elif 'COLMRUNDATA' in line:
            lines[i] = lines[i].replace('COLMRUNDATA', f'{CoLMRunDataPath}/')
        elif 'COLMRUNPATH' in line:
            lines[i] = lines[i].replace('COLMRUNPATH', f'{CoLMCasePath}/')
        elif 'MESHNAME' in line:
            lines[i] = lines[i].replace('MESHNAME', f'{MeshFilePath}')
        elif 'WRESTFREQ' in line:
            if run_type == 'First':
                lines[i] = lines[i].replace('WRESTFREQ', f'{WriteRestartFreq[0].upper()}')
            elif run_type == 'Second':
                lines[i] = lines[i].replace('WRESTFREQ', f'{WriteRestartFreq[1].upper()}')
            elif run_type == 'Third':
                lines[i] = lines[i].replace('WRESTFREQ', f'{WriteRestartFreq[2].upper()}')
        elif 'HISTFREQ' in line:
            if run_type == 'First':
                lines[i] = lines[i].replace('HISTFREQ', f'{WriteHistoryFreq[0].upper()}')
            elif run_type == 'Second':
                lines[i] = lines[i].replace('HISTFREQ', f'{WriteHistoryFreq[1].upper()}')
            elif run_type == 'Third':
                lines[i] = lines[i].replace('HISTFREQ', f'{WriteHistoryFreq[2].upper()}')
        elif 'HISTGROUPBY' in line:
            lines[i] = lines[i].replace('HISTGROUPBY', f'YEAR')
        elif 'FORCINGNAME' in line:
            lines[i] = lines[i].replace('FORCINGNAME', f'{ForcingDataName.upper()}')
            
        NewCoLMNML = f'{CoLMCasePath}/tmpdir/unstructured_cwrf.colm.{gridname}.{run_type}'
        with open(NewCoLMNML, 'w') as file:
            file.writelines(lines)

    logging.info(f"{S4}-> Modified CoLM namelist file: unstructured_cwrf.colm.{gridname}.{run_type}")
    return config



def CoLMMakeIni(config, gridname):
    old_path = os.getcwd()
    ScriptPath = config.get('BaseInfo', 'ScriptPath')
    CoLMCoreNum = config.getint('BaseInfo', 'CoLMCoreNum')
    CoLMPath = config.get('BaseInfo', 'CoLMPath')
    CoLMForcDataPath = config.get('BaseInfo', 'CoLMForcDataPath')
    CoLMCasePath = config.get(gridname, 'CoLMCasePath')
    MeshFilePath = config.get(gridname, 'MeshFilePath')
    ForcingDataName = config.get(gridname, 'ForcingDataName')
    GoMakeIni = config.getboolean('SpinUp', 'GoFirstPeriod')
    StartTime = config.get(gridname, 'SpinUpStartTime')
    EndTime = config.get(gridname, 'SpinUpEndTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    seconds = EndTime.hour * 3600 + EndTime.minute * 60 + EndTime.second
    dayofyear = StartTime.timetuple().tm_yday
   
    os.chdir(f'{CoLMCasePath}/tmpdir')
    
    restartpath = f'{CoLMCasePath}/unstructured_cwrf_{gridname}/restart/{StartTime.year}-{dayofyear:03d}-{seconds:05d}'
    if os.path.exists(restartpath):
        logging.warning(f'{S4}Restart path exists: {restartpath}')
        logging.warning(f'{S4}Using the existing restart files.')
        GoMakeIni = False
    else:
        GoMakeIni = config.getboolean('SpinUp', 'GoMakeIni')
        if not GoMakeIni:
            logging.error(f'{S4}Restart path not found: {restartpath}')
            logging.error(f'{S4}GoMakeIni is set to False, but the restart path does not exist.')
            sys.exit(1)
    
    if GoMakeIni:
        
        # link colm executable files
        cmd = f'ln -sf {CoLMPath}/run/mksrfdata.x ./mksrfdata.x'
        RunCMD(cmd, "Link mksrfdata.x")
        
        cmd = f'ln -sf {CoLMPath}/run/mkinidata.x ./mkinidata.x'
        RunCMD(cmd, "Link mkinidata.x")
        
        cmd = f'ln -sf {CoLMPath}/run/colm.x ./colm.x'
        RunCMD(cmd, "Link colm.x")
        
        # link namelist files
        run_type = None
        if run_type == None and os.path.exists(f'{CoLMCasePath}/tmpdir/unstructured_cwrf.colm.{gridname}.First'):
            run_type = 'First'
            logging.info(f'{S4}Initializing for {run_type} period...')
        if run_type == None and os.path.exists(f'{CoLMCasePath}/tmpdir/unstructured_cwrf.colm.{gridname}.Second'):
            run_type = 'Second'
            logging.info(f'{S4} Not necessary to initialize for First period.')
            logging.info(f'{S4}Initializing for {run_type} period...')
        if run_type == None and os.path.exists(f'{CoLMCasePath}/tmpdir/unstructured_cwrf.colm.{gridname}.Third'):
            run_type = 'Third'
            logging.info(f'{S4} Not necessary to initialize for First and Second period.')
            logging.info(f'{S4}Initializing for {run_type} period...')
        if run_type == None:
            logging.error(f'{S4}No unstructured_cwrf.colm.{gridname}.{run_type} found in {CoLMCasePath}/tmpdir/')
            logging.info(f'{S4}Initializing for {run_type} period...')
            sys.exit(1)
        
            
        cmd = f'cp {CoLMCasePath}/tmpdir/unstructured_cwrf.colm.{gridname}.{run_type} ./unstructured_cwrf.colm.nml'
        RunCMD(cmd, "Copy unstructured_cwrf.colm")
        
        cmd = f'cp {ScriptPath}/NML/CoLM_Forcing/{ForcingDataName}.nml ./{ForcingDataName}.nml'
        RunCMD(cmd, f"Copy {ForcingDataName}.nml")
        
        #替换{ForcingDataName}.nml中的路径forcingdir为/shr03/CoLM_Forcing/{ForcingDataName}/
        cmd = f'sed -i "s|forcingdir|{CoLMForcDataPath}/{ForcingDataName}/|g" {ForcingDataName}.nml'
        RunCMD(cmd, f"Replace forcingdir in {ForcingDataName}.nml")
        
        cmd = f'ln -sf {ScriptPath}/NML/history.colm.ctl ./history.nml'
        RunCMD(cmd, "Link history.nml")
        
        # link mesh file
        cmd = f'ln -sf {MeshFilePath} .'
        RunCMD(cmd, f"Copy Mesh file: {MeshFilePath}")

        # Check Landdata
        landdatadatapath = f'{CoLMCasePath}/unstructured_cwrf_{gridname}/landdata'
        files = glob.glob(f'{landdatadatapath}/*')
        if len(files) == 0:
            logging.error(f'{S4}Restart not found in {landdatadatapath}!')
            logging.error(f'{S4}Please Run the CRESM_Prepare_Data.py script first.')
            sys.exit(1)
    
        # ---------------------- MakeIni ----------------------
        logging.info(f'{S4}==========> Make Initial Data <==========')
        log_file = f'{CoLMCasePath}/tmpdir/log.mkini.{run_type}'
        cmd = f'rm -f {log_file}'
        RunCMD(cmd, "Remove old log file")
        cmd = f'mpirun -np {CoLMCoreNum} ./mkinidata.x unstructured_cwrf.colm.nml > {log_file} 2>&1'
        RunCMD(cmd, "Run mkinidata.x")
        landdatadatapath = f'{CoLMCasePath}/unstructured_cwrf_{gridname}/restart'
        files = glob.glob(f'{landdatadatapath}/*')
        files = sorted(files, key=os.path.getmtime)
        logging.info(f'{S4}-> restart files:')
        for file in files:
            logging.info(f'{S8}unstructured_cwrf_{gridname}/restart/{os.path.basename(file)}')
        logging.info(f'{S4}✓  Make CoLM initial data finished!\n\n')
    else:
        logging.info(f'{S4}==========> Use Existing Initial Data <==========\n\n')

    os.chdir(old_path)




def CoLMSpinUp(config, gridname, run_type):
    old_path = os.getcwd()
    ScriptPath = config.get('BaseInfo', 'ScriptPath')
    CoLMCoreNum = config.getint('BaseInfo', 'CoLMCoreNum')
    CoLMPath = config.get('BaseInfo', 'CoLMPath')
    CoLMForcDataPath = config.get('BaseInfo', 'CoLMForcDataPath')
    CoLMCasePath = config.get(gridname, 'CoLMCasePath')
    MeshFilePath = config.get(gridname, 'MeshFilePath')
    ForcingDataName = config.get(gridname, 'ForcingDataName')
    StartTime = config.get(gridname, 'SpinUpStartTime')
    EndTime = config.get(gridname, 'SpinUpEndTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    seconds = EndTime.hour * 3600 + EndTime.minute * 60 + EndTime.second

    if run_type == 'First':
        RunTag = config.getboolean('SpinUp', f'GoFirstPeriod')
        StartTime = StartTime
        EndTime = datetime(EndTime.year, 1, 1, EndTime.hour, EndTime.minute, EndTime.second)
        if StartTime.year == EndTime.year:
            logging.info(f"{S4}** The SpinUpStartTime and SpinUpEndTime are in the same year, so the first period will not run.")
            RunTag = False
    elif run_type == 'Second':
        RunTag = config.getboolean('SpinUp', f'GoSecondPeriod')
        StartTime = datetime(EndTime.year, 1, 1, EndTime.hour, EndTime.minute, EndTime.second)
        EndTime = datetime(EndTime.year, EndTime.month, 1, EndTime.hour, EndTime.minute, EndTime.second)        
        if StartTime.month == EndTime.month:
            logging.info(f"{S4}** The SpinUpStartTime is the first month of the year, so the second period will not run.")
            RunTag = False
    elif run_type == 'Third':
        RunTag = config.getboolean('SpinUp', f'GoThirdPeriod')
        StartTime = datetime(EndTime.year, EndTime.month, 1, EndTime.hour, EndTime.minute, EndTime.second)
        EndTime = EndTime
        if StartTime.day == EndTime.day:
            logging.info(f"{S4}** The SpinUpStartTime is the first day of the month, so the third period will not run.")
            RunTag = False
            
    if RunTag:
        logging.info(f'{S4}==========> CoLM Spin-Up <==========')
        logging.info(f"{S4}StartTime : {StartTime.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"{S4}EndTime   : {EndTime.strftime('%Y-%m-%d %H:%M:%S')}")
        dayofyear = StartTime.timetuple().tm_yday
    
        os.chdir(f'{CoLMCasePath}/tmpdir')
        
        # link colm executable files
        cmd = f'ln -sf {CoLMPath}/run/mksrfdata.x ./mksrfdata.x'
        RunCMD(cmd, "Link mksrfdata.x")
        
        cmd = f'ln -sf {CoLMPath}/run/mkinidata.x ./mkinidata.x'
        RunCMD(cmd, "Link mkinidata.x")
        
        cmd = f'ln -sf {CoLMPath}/run/colm.x ./colm.x'
        RunCMD(cmd, "Link colm.x")
        
        # link namelist files
        cmd = f'cp {CoLMCasePath}/tmpdir/unstructured_cwrf.colm.{gridname}.{run_type} ./unstructured_cwrf.colm.nml'
        RunCMD(cmd, "Copy unstructured_cwrf.colm")
        
        cmd = f'cp {ScriptPath}/NML/CoLM_Forcing/{ForcingDataName}.nml ./{ForcingDataName}.nml'
        RunCMD(cmd, f"Copy {ForcingDataName}.nml")
        
        #替换{ForcingDataName}.nml中的路径forcingdir为/shr03/CoLM_Forcing/{ForcingDataName}/
        cmd = f'sed -i "s|forcingdir|{CoLMForcDataPath}/{ForcingDataName}/|g" {ForcingDataName}.nml'
        RunCMD(cmd, f"Replace forcingdir in {ForcingDataName}.nml")
        
        cmd = f'ln -sf {ScriptPath}/NML/history.colm.ctl ./history.nml'
        RunCMD(cmd, "Link history.nml")
        
        # link mesh file
        cmd = f'ln -sf {MeshFilePath} .'
        RunCMD(cmd, f"Copy Mesh file: {MeshFilePath}")

        # Check Landdata
        landdatapath = f'{CoLMCasePath}/unstructured_cwrf_{gridname}/landdata'
        files = glob.glob(f'{landdatapath}/*')
        if len(files) == 0:
            logging.error(f'{S4}Landdata not found in {landdatapath}!')
            logging.error(f'{S4}Please Run the CRESM_Prepare_Data.py script first.')
            sys.exit(1)
        
        restartpath = f'{CoLMCasePath}/unstructured_cwrf_{gridname}/restart/{StartTime.year}-{dayofyear:03d}-{seconds:05d}'
        if not os.path.exists(restartpath):
            logging.error(f'{S4}Restart path not found: {restartpath}')
            logging.error(f'{S4}Please keep GoMakeIni as True in the configuration file.')  
            sys.exit(1)

        # ---------------------- CoLMRun ----------------------
        log_file = f'{CoLMCasePath}/tmpdir/log.colm.{run_type}'
        cmd = f'rm -f {log_file}'
        RunCMD(cmd, "Remove old log file")
        cmd = f'mpirun -np {CoLMCoreNum} ./colm.x unstructured_cwrf.colm.nml > {log_file} 2>&1'
        RunCMD(cmd, "Run colm.x")
        files = glob.glob(f'{CoLMCasePath}/unstructured_cwrf_{gridname}/history/*')
        if len(files) == 0:
            logging.error(f'{S4}unstructured_cwrf_{gridname}/history/* not found!')
            logging.error(f'{S4}Please check the Second_MakeSrf part')
            sys.exit(1)
        files = glob.glob(f'{CoLMCasePath}/unstructured_cwrf_{gridname}/restart/*')
        files = sorted(files, key=os.path.getmtime)
        logging.info(f'{S4}-> restart files:')
        for file in files:
            logging.info(f'{S8}unstructured_cwrf_{gridname}/restart/{os.path.basename(file)}')
        logging.info(f'{S4}✓  {run_type} period finished!\n\n')
    else: 
        logging.info(f'{S4}==========> Skip {run_type} Period <==========\n\n')

    os.chdir(old_path)
    



def CleanTempFiles(config, gridname):
    """
    Clean temporary files
    """
    CoLMCasePath = config.get(gridname, 'CoLMCasePath')
    tmpfiles = glob.glob(f'{CoLMCasePath}/tmpdir/*')
    tmpfiles += glob.glob(f'{CoLMCasePath}/unstructured_cwrf_{gridname}/history/*.nc')
    tmpfiles = glob.glob(f'{CoLMCasePath}/tmpdir')
    # 从tmpfiles中排除不需要删除的文件
    tmpfiles = [file for file in tmpfiles if not file.endswith('_remap.nc')]
    time.sleep(2)  # Ensure the log file is written before cleaning up
    
    logging.info(f'{S4}==========> Clean Temporary Files <==========')
    if config.getboolean('BaseInfo', 'CleanTempFiles'):
        for tmpfile in tmpfiles:
            if os.path.exists(tmpfile):
                cmd = f'rm -rf {tmpfile}'
                RunCMD(cmd, f"Remove {tmpfile}")
                logging.info(f'{S4}-> Remove {tmpfile}')
            else:
                logging.warning(f'{S6}Temporary file not found: {tmpfile}')
    else:
        logging.info(f'{S4}==========> Skip Clean Temp <==========')
        logging.info(f'{S4}!!! Temporary files will not be cleaned. !!!\n\n')




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('-c','--config', action='store_true', help='Display the configuration file help')
    parser.add_argument('-f','--usefulcase', action='store_true', help='Display useful case name in the CWRF_Prepare.ini')
    parser.add_argument('-n','--gridname', type=str, help='Case Name in the CWRF_Prepare.ini', default=None)
    args = parser.parse_args()
    gridname = args.gridname
    loglevel = logging.DEBUG if args.debug else logging.INFO
    if args.config:
        PrintConfigHelp()
        
    if args.usefulcase:
        config = ReadConfig()
        sectionslist = PrintUsefulCase(config)
        sys.exit(0)

    if gridname is None:
        print("Please provide a case name using -n or --gridname")
        sys.exit(1)
    os.system(f'rm -f ./log.DataPrepare.{gridname}')
    
    # If you want debug information, you can set the logging level to DEBUG
    # Set up logging
    logging.basicConfig(
        level=loglevel,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        format="%(asctime)s [%(levelname)-8s]    %(message)s",
        datefmt="%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(f"log.DataPrepare.{gridname}"),
            logging.StreamHandler()
        ]
    )
    
    logging.info('')
    logging.info('      **********************************')
    logging.info('        *** CoLM Spin-Up for CRESM ***  ')
    logging.info('      **********************************\n\n')
    logging.info(f'       Case Name: {gridname} \n\n')
    codestart = time.time()
    
    # Read configuration file
    logging.info('Reading configuration file...')
    config = ReadConfig()

    # Check the configuration file
    logging.info('Checking the configuration file...')
    CheckConfig(config, gridname)

    # make directory
    logging.info('Making directory...')
    make_dir(config, gridname)
    
    # Modify CoLM namelist file
    logging.info('Modifying CoLM namelist file...')
    config = ModifyCoLMNML(config, gridname, run_type='First')
    config  = ModifyCoLMNML(config, gridname, run_type='Second')
    config  = ModifyCoLMNML(config, gridname, run_type='Third')
    logging.info(f'{S4}✓  CoLM namelist file modified!\n\n')
    
    logging.info('Printing time period information...')
    PrintTimePeriod(config, gridname)
    
    # Make initial data
    logging.info(' *** Making Initial Data ***')
    CoLMMakeIni(config, gridname)
    
    # Running The First period
    logging.info(' *** Running The First period ***')
    CoLMSpinUp(config, gridname, run_type='First')
    
    # Running The Second period
    logging.info(' *** Running The Second period ***')
    CoLMSpinUp(config, gridname, run_type='Second')
    
    # Running The Third period
    logging.info(' *** Running The Third period ***')
    CoLMSpinUp(config, gridname, run_type='Third')

    # Clean temporary files
    logging.info('[PostProc] *** Cleaning Temporary Files ***')
    CleanTempFiles(config, gridname)

    logging.info('')
    logging.info('      *********************************')
    logging.info('        *** Successfully Finished ***  ')
    logging.info('      *********************************\n')
    logging.info(f'Total time: {round(time.time()-codestart,2)} seconds')
