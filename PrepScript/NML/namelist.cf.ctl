&seq_cplflds_userspec
 !cplflds_custom = 'Sa_foo->a2x', 'Sa_foo->x2a'
 cplflds_custom =''
/
&seq_infodata_inparm
 cpl_decomp = 0
 case_name = 'test3'
 start_type = 'startup'
 tchkpt_dir = './timing/checkpoints'
 timing_dir = './timing'
/
&seq_timemgr_inparm
 calendar = 'gregorian'
 atm_cpl_dt = 600
 atm_cpl_offset=0
 ocn_cpl_dt = 3600
 ocn_cpl_offset=0
 lnd_cpl_dt = 600
 lnd_cpl_offset=0
 start_ymd = stymd
 start_tod = sthour
 end_ymd = edymd
 end_tod = ethour
/
&mapping
 atm2ocn1_fmaptype = 'Y'
 atm2ocn1_smaptype = 'X'
 atm2ocn2_fmaptype = 'Y'
 atm2ocn2_smaptype = 'Y'
 ocn2atm1_fmaptype = 'Y'
 ocn2atm1_smaptype = 'Y'
 ocn2atm2_fmaptype = 'Y'
 ocn2atm2_smaptype = 'Y'
 atm2ocn1_fmapname='./cpl7data/cwrf_s2fvcom_e_wgts_pat_CASENAME.nc'
 atm2ocn1_smapname='./cpl7data/cwrf_s2fvcom_e_wgts_bl_CASENAME.nc'
 atm2ocn2_fmapname='./cpl7data/cwrf_s2fvcom_n_wgts_pat_CASENAME.nc'
 atm2ocn2_smapname='./cpl7data/cwrf_s2fvcom_n_wgts_bl_CASENAME.nc'
 ocn2atm1_fmapname='./cpl7data/fvcom_e2cwrf_s_wgts_bl_CASENAME.nc'
 ocn2atm1_smapname='./cpl7data/fvcom_e2cwrf_s_wgts_bl_CASENAME.nc'
 ocn2atm2_fmapname='./cpl7data/fvcom_n2cwrf_s_wgts_bl_CASENAME.nc'
 ocn2atm2_smapname='./cpl7data/fvcom_n2cwrf_s_wgts_bl_CASENAME.nc'
 atm2lnd_smaptype='X'
 atm2lnd_fmaptype='Y'
 lnd2atm_smaptype='Y'
 lnd2atm_fmaptype='Y'
 atm2lnd_smapname='./cpl7data/cwrf_s2colm_wgts_bl_CASENAME_final.nc'
 atm2lnd_fmapname='./cpl7data/cwrf_s2colm_wgts_pat_CASENAME_final.nc'
 lnd2atm_smapname='./cpl7data/colm_2cwrf_s_wgt_bl_CASENAME_final.nc'
 lnd2atm_fmapname='./cpl7data/colm_2cwrf_s_wgt_pat_CASENAME_final.nc'



/
&cf_pes
 cpl_rootpe = 0
 cpl_ntasks = 
 cpl_nthreads = 1
 cpl_pestride = 1
 atm_layout = 'sequential'
 atm_rootpe = 0
 atm_ntasks = 240
 atm_nthreads = 1
 atm_pestride = 1
 lnd_layout = 'sequential'
 lnd_rootpe = 0
 lnd_ntasks = 240
 lnd_nthreads = 1
 lnd_pestride = 1
 ocn_layout = 'sequential'
 ocn_rootpe = 0
 ocn_ntasks = 240
 ocn_nthreads = 1
 ocn_pestride = 1

/
&atm
 atm_lag_count = 1
/
&lnd
 lnd_lag_count = 1
/
&domainsize
 atmXsize = EdgeNum_WE
 atmYsize = EdgeNum_SN
/
