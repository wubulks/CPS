colmref=addfile("./CoLMref.nc", "r")
tgrnd=colmref->f_t_grnd(0,:,:)
tgrnd_mask=where(tgrnd.lt.0,0,1)
CWRFinfil=addfile("./wrfinput_d01", "w")
scwater=CWRFinfil->SC_WATER(0,:,:)


glmask=scwater*1
glmask=glmask*0
glmask@_FillValue=-9999
glmask=where(scwater.eq.8,2,glmask@_FillValue)
glmask=where(tgrnd_mask.eq.1,3,glmask)
glmask=where(scwater.eq.8,2,glmask)
glmask=where(ismissing(glmask),1,glmask)
problems=where(ismissing(glmask),1,0)


SC_WATER_mod=CWRFinfil->SC_WATER

XFVEG_mod=CWRFinfil->XFVEG
printVarSummary(SC_WATER_mod)
printVarSummary(problems)
SC_WATER_mod_values=where(problems.eq.1,8.0,SC_WATER_mod(0,:,:))
SC_WATER_mod(0,:,:)=(/SC_WATER_mod_values/)
CWRFinfil->SC_WATER=SC_WATER_mod



XFVEG=CWRFinfil->XFVEG
LU_INDEX=CWRFinfil->LU_INDEX
templuindex=LU_INDEX(0,:,:)
tempxfveg=XFVEG(0,:,:)
tempxfveg=where(templuindex.eq.16,0,tempxfveg)
XFVEG(0,:,:)=tempxfveg
CWRFinfil->XFVEG=XFVEG
templuindex=where(templuindex.eq.24,22,templuindex)
CWRFinfil->LU_INDEX=LU_INDEX
CWRFinfil->IVGTYP=toint(LU_INDEX)


tempfil="temp.nc"
system("rm -f "+tempfil)
outfil=addfile(tempfil, "c")
outfil->LU_INDEX =LU_INDEX
outfil->glmask =glmask
outfil->problems =problems
outfil->problems2=where(templuindex.eq.16.and.tempxfveg.gt.0,1,0)
