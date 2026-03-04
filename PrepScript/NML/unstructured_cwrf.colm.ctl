&nl_colm
   
   DEF_CASE_NAME = 'CASENAME' 

   
   DEF_domain%edges = EDGESSOUTH
   DEF_domain%edgen = EDGENORTH
   DEF_domain%edgew = EDGEWEST
   DEF_domain%edgee = EDGEEAST
 
   DEF_nx_blocks = 18
   DEF_ny_blocks = 9
   DEF_PIO_groupsize = 47
   
   DEF_simulation_time%greenwich    = .TRUE.
   DEF_simulation_time%start_year   = SYEAR
   DEF_simulation_time%start_month  = SMONTH
   DEF_simulation_time%start_day    = SDAY
   DEF_simulation_time%start_sec    = SSEC
   DEF_simulation_time%end_year     = EYEAR
   DEF_simulation_time%end_month    = EMONTH
   DEF_simulation_time%end_day      = EDAY
   DEF_simulation_time%end_sec      = ESEC
   DEF_simulation_time%spinup_year  = 1900
   DEF_simulation_time%spinup_month = 06
   DEF_simulation_time%spinup_day   = 01
   DEF_simulation_time%spinup_sec   = 0
   DEF_simulation_time%spinup_repeat = 0
   DEF_simulation_time%timestep     = COLMTIMESTEP
   
   DEF_dir_rawdata  = 'COLMRAWDATA' 
   DEF_dir_runtime  = 'COLMRUNDATA'
   DEF_dir_output   = 'COLMRUNPATH'

   !----in coupling mode----
   readinputfromfile=.false.                ! not used in offline run
   DEF_dir_rstfilename='./wrfinput_d01'    ! not used in offline run
   !----in coupling mode----

   ! ----- land units and land sets -----
   ! for UNSTRUCTURED
   DEF_file_mesh = 'MESHNAME'

   ! LAI & LANDCOVER setting
   DEF_LAI_CHANGE_YEARLY = .false.
   DEF_LC_YEAR = 2020
   DEF_LAI_MONTHLY = .true.
   
   !for BGC
   DEF_USE_CN_INIT  = .false.
   DEF_file_cn_init = '/share/home/dq010/CoLM/data/rawdata/CROP-NITRIF/CLMrawdata_updating/cnsteadystate.nc'
   DEF_USE_PN       =   .false.
   DEF_USE_SASU     = .false.

   ! ----Soil Surface Resistance options----
   ! 0: NONE soil surface resistance
   ! 1: SL14, Swenson and Lawrence (2014)
   ! 2: SZ09, Sakaguchi and Zeng (2009)
   ! 3: TR13, Tang and Riley (2013)
   ! 4: LP92, Lee and Pielke (1992)
   ! 5: S92,  Sellers et al (1992)
   DEF_RSS_SCHEME = 5

   !---- Urban options ----
   ! urban type options
   ! Options :
   ! 1: NCAR Urban Classification, 3 urban type with Tall Building, High Density and Medium Density
   ! 2: LCZ Classification, 10 urban type with LCZ 1-10
   DEF_URBAN_type_scheme = 1

   ! urban module options
   DEF_URBAN_ONLY = .false.
   DEF_URBAN_TREE = .true.
   DEF_URBAN_WATER= .true.
   DEF_URBAN_BEM  = .true.
   DEF_URBAN_LUCY = .true.
   ! -----------------------

   ! Canopy DEF Interception scheme selection
   DEF_Interception_scheme=1 !1:CoLM2014?~[2:CLM4.5; 3:CLM5; 4:Noah-MP; 5:MATSIRO; 6:VIC

   ! ---- Hydrology module ----
   DEF_USE_SUPERCOOL_WATER       = .true.
   DEF_USE_VariablySaturatedFlow = .false.
   DEF_USE_PLANTHYDRAULICS       = .false.
   ! --------------------------

      ! ---- SNICAR ----
   DEF_USE_SNICAR     = .true.
   DEF_Aerosol_Readin = .true.
   DEF_Aerosol_Clim   = .false.
   ! ----------------

   ! ---- Ozone MODULE ----
   DEF_USE_OZONESTRESS = .false.
   DEF_USE_OZONEDATA   = .false.
   ! ----------------------

   ! ---- Bedrock ----
   DEF_USE_BEDROCK = .false.
   ! -----------------

   ! ---- Split Soil Snow ----
   DEF_SPLIT_SOILSNOW = .false.
   DEF_VEG_SNOW       = .false.
   ! -------------------------

   ! ---- Initializaion ----
   DEF_USE_SoilInit  = .true.
   DEF_file_SoilInit = '/share/home/dq013/zhwei/colm/data/soilstate/soilstate.nc'
   DEF_USE_SnowInit = .false.
   ! -----------------------

   ! ---- Forcing Downscalling ----
   DEF_USE_Forcing_Downscaling        = .false.
   DEF_DS_precipitation_adjust_scheme = 'II'
   DEF_DS_longwave_adjust_scheme      = 'II'
   ! ------------------------------

   ! ---- Rain&Snow ----
   DEF_precip_phase_discrimination_scheme = 'II'
   ! -------------------

      ! ---- Stomata module ----
   DEF_USE_WUEST    = .false.
   DEF_USE_MEDLYNST = .false.
   ! ------------------------

   ! Model settings
   DEF_LANDONLY = .true.

 
! ----- forcing -----
   ! Options :
   ! PRINCETON | GSWP3   | QIAN  | CRUNCEPV4 | CRUNCEPV7 | ERA5LAND | ERA5 |  MSWX
   ! WFDE5     | CRUJRA  | WFDEI | JRA55     | GDAS      | CMFD     | POINT
   DEF_forcing_namelist = './ERA5LAND.nml'

   ! ----- history -----
   DEF_HISTORY_IN_VECTOR = .true.

   DEF_WRST_FREQ = 'WRESTFREQ' ! write restart file frequency: HOURLY/DAILY/MONTHLY/YEARLY
   DEF_HIST_FREQ = 'HISTFREQ'   ! write history file frequency: HOURLY/DAILY/MONTHLY/YEARLY
   DEF_HIST_groupby = 'HISTGROUPBY'  ! history in one file: DAY/MONTH/YEAR
   DEF_HIST_mode = 'one' ! history in one or block
   DEF_REST_CompressLevel = 1
   DEF_HIST_CompressLevel = 1

   DEF_HIST_WriteBack = .false.
   DEF_hist_vars_out_default = .true.
   DEF_hist_vars_namelist = './history.nml'
   
/
