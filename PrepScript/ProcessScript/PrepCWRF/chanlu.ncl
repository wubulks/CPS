infile=addfile("wrfinput_"+suffix+"","w")
rf=addfile("./geo_em."+suffix+".nc","r")
lu=rf->LU_INDEX

fvcf=addfile("./MODIS2CWRF_SBC_"+suffix+".nc","r")
fvc=fvcf->FVC(:,0,:,:)
oldfvc=infile->XFVEG
oldfvc(:,:,:)=(/fvc/)

xland = infile->LANDMASK
scw = infile->SC_WATER


inconsisten_scw_xland=where(scw.le.3.and.xland.eq.0,1,0)
inconsisten_xfveg_xland=where(fvc.gt.0.and.xland.eq.0,1,0)


oldfvc=where(oldfvc.gt.0.and.lu.eq.16,0,oldfvc)
inconsisten_fvc_lu=where(fvc.gt.0.and.lu.eq.16,1,0)
inconsisten_ldm_lu=where(xland.ne.0.and.lu.eq.16,1,0)


inconsisten_fvc_lu=where(ismissing(inconsisten_fvc_lu),0,inconsisten_fvc_lu)
inconsisten_ldm_lu=where(ismissing(inconsisten_ldm_lu),0,inconsisten_ldm_lu)
lu=where(inconsisten_fvc_lu.eq.1.or.inconsisten_ldm_lu.eq.1,7,lu)
lu=where(lu.eq.24,22,lu)
infile->LU_INDEX=lu
infile->SC_LANDU=lu
infile->IVGTYP=toint(lu)
infile->XFVEG=oldfvc


