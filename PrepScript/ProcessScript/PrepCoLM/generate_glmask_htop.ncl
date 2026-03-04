CWRFinfil=addfile("./geo_em.d01_veg.nc", "r")
CoLMinfil=addfile("./CoLM_ref_"+geogName+".nc", "r")
htopinfil=addfile("./htop_patch_2020_"+geogName+".nc","r")


XLAT_U=CWRFinfil->XLAT_U(0,:,:)
XLONG_U=CWRFinfil->XLONG_U(0,:,:)
XLAT_V=CWRFinfil->XLAT_V(0,:,:)
XLONG_V=CWRFinfil->XLONG_V(0,:,:)



scwater=CWRFinfil->SC_WATER(0,:,:)

sizes=dimsizes(scwater)
lats=sizes(0)
lons=sizes(1)

tgrnd=CoLMinfil->f_t_grnd(0,:,:)
printVarSummary(tgrnd)
tgrnd_mask=where(ismissing(tgrnd),0,1)





htop_max=new((/lats,lons/),float)
do ilon=0,lons-1
    do ilat=0,lats-1
        leftlon=XLONG_U(ilat,ilon)
        rightlon=XLONG_U(ilat,ilon+1)
        toplat=XLAT_V(ilat+1,ilon)
        bottomlat=XLAT_V(ilat,ilon)
        htoptemp:=htopinfil->htop({bottomlat:toplat},{leftlon:rightlon},:)
        ; printVarSummary(htoptemp)
        htop_max(ilat,ilon)=tofloat(max(htoptemp))
    end do
end do

outfilename="htop_rcm.nc"
system("rm -f "+outfilename)
outfil=addfile(outfilename, "c")
htop_max@description="htop_max"
outfil->htop_max=htop_max

glmask=scwater*0
glmask@_FillValue=-9999
glmask=where(scwater.eq.8,2,glmask@_FillValue)
glmask=where(tgrnd_mask.eq.1,3,glmask)
glmask=where(scwater.eq.8,2,glmask)
glmask=where(ismissing(glmask),2,glmask)

glmask(0,:)=2
glmask(lats-1,:)=2
glmask(:,0)=2
glmask(:,lons-1)=2

; glmask=where(glmask.eq.2.and. .not. ismissing(htop_max),3,glmask)

outfilename="GLMASK.nc"
system("rm -f "+outfilename)
outfil=addfile(outfilename, "c")
glmask@description="GLMASK"
outfil->GL_MASK=glmask
