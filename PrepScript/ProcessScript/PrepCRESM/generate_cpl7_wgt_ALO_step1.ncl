load "$NCARG_ROOT/lib/ncarg/nclscripts/esmf/ESMF_regridding.ncl"
setfileoption("nc","Format","NetCDF4")


; geogName="NECN_15km"
geofilname = geogName+"/geo_em.d01.nc"
colmfilname = geogName+"/colm_grd.nc"  ;colm can have the same or a different grid as cwrf
; system("cp "+geofilname+" "+colmfilname)
pathdir = geogName+"/cpl7data/"
elmind = geogName+"/CoLM_ref_"+geogName+"_vector.nc"
; system("cp ../PrepCoLM/"+geogName+"/unstructured_cwrf_"+geogName+"/history/unstructured_cwrf_"+geogName+"_hist_2000-06-01.nc "+elmind)

; description file
; n=>node e=>element s=>scalar
fvcom_n_des=pathdir+"fvcom_grd_n_des_"+geogName+".nc"
fvcom_e_des=pathdir+"fvcom_grd_e_des_"+geogName+".nc"
colm_des=pathdir+"colm_grd_des_"+geogName+".nc"
cwrf_s_des=pathdir+"cwrf_s_des_"+geogName+".nc"
; mesh_des=pathdir+"colmmesh_des_"+geogName+".nc"
; weights file
cwrf_s2fvcom_n_wgts_bl=pathdir+"cwrf_s2fvcom_n_wgts_bl_"+geogName+".nc"
cwrf_s2fvcom_n_wgts_pat=pathdir+"cwrf_s2fvcom_n_wgts_pat_"+geogName+".nc"
cwrf_s2fvcom_e_wgts_bl=pathdir+"cwrf_s2fvcom_e_wgts_bl_"+geogName+".nc"
cwrf_s2fvcom_e_wgts_pat=pathdir+"cwrf_s2fvcom_e_wgts_pat_"+geogName+".nc"

cwrf_s2colm_wgts_bl=pathdir+"cwrf_s2colm_wgts_bl_"+geogName+".nc"
cwrf_s2colm_wgts_pat=pathdir+"cwrf_s2colm_wgts_pat_"+geogName+".nc"

colm_2cwrf_s_wgt_bl=pathdir+"colm_2cwrf_s_wgt_bl_"+geogName+".nc"
colm_2cwrf_s_wgt_pat=pathdir+"colm_2cwrf_s_wgt_pat_"+geogName+".nc"

fvcom_n2cwrf_s_wgts_bl=pathdir+"fvcom_n2cwrf_s_wgts_bl_"+geogName+".nc"
fvcom_n2cwrf_s_wgts_pat=pathdir+"fvcom_n2cwrf_s_wgts_pat_"+geogName+".nc"
fvcom_e2cwrf_s_wgts_bl=pathdir+"fvcom_e2cwrf_s_wgts_bl_"+geogName+".nc"
fvcom_e2cwrf_s_wgts_pat=pathdir+"fvcom_e2cwrf_s_wgts_pat_"+geogName+".nc"


; opt0=True
; opt0@ForceOverwrite = True
; opt0@PrintTimings   = True
; yoffset:=todouble(180.0)/21600.0
; xoffset:=todouble(360.0)/43200.0
; lat_g:=fspan(-90+yoffset, 90-yoffset, 21600)
; lon_g:=fspan(-180+xoffset, 180-xoffset, 43200)
; rectilinear_to_SCRIP(mesh_des, lat_g, lon_g, opt0)


; ; node lat/lon
; f1="./lat.gl"
; f2="./lon.gl"
f_geo=addfile("./CN_COAST_restart_modified2019.nc", "r")

lats=f_geo->lat
lons=f_geo->lon
; lats=asciiread(f1, -1, "float")
; lons=asciiread(f2, -1, "float")

opt1=True
opt1@ForceOverwrite = True
opt1@PrintTimings   = True
unstructured_to_ESMF(fvcom_n_des, lats, lons, opt1)

; ---------------------------------------------------------------

; element lat/lon

lat_c=f_geo->latc
lon_c=f_geo->lonc

unstructured_to_ESMF(fvcom_e_des, lat_c, lon_c, opt1)

; ---------------------------------------------------------------

; colm
f3=addfile(colmfilname, "r")

f99=addfile(elmind, "r")
elmindex=f99->elmindex
elmindex:=toint(elmindex)

colm_lat=f3->XLAT_M(0,:,:)
colm_lon=f3->XLONG_M(0,:,:)
colmsizes=dimsizes(colm_lat)
colm_latsizes=colmsizes(0)
colm_lonsizes=colmsizes(1)
colm_lat_1d_re=reshape(colm_lat(west_east|:,south_north|:),(/colm_latsizes*colm_lonsizes/))
colm_lon_1d_re=reshape(colm_lon(west_east|:,south_north|:),(/colm_latsizes*colm_lonsizes/))

colm_lat_1d_re := colm_lat_1d_re(elmindex-1)
colm_lon_1d_re := colm_lon_1d_re(elmindex-1)

unstructured_to_ESMF(colm_des, colm_lat_1d_re, colm_lon_1d_re, opt1)


; ---------------------------------------------------------------


f3=addfile(geofilname, "r")

cwrf_lat=f3->XLAT_M(0,:,:)
cwrf_lon=f3->XLONG_M(0,:,:)
sc_water=f3->SC_WATER(0,:,:)

sizes=dimsizes(cwrf_lat)
latsizes=sizes(0)
lonsizes=sizes(1)

cwrf_lat_scalar=cwrf_lat
cwrf_lon_scalar=cwrf_lon

curvilinear_to_SCRIP(cwrf_s_des, cwrf_lat_scalar, cwrf_lon_scalar, opt1)



; cwrf=>colm
opt3=True
opt3@ForceOverwrite = True
opt3@PrintTimings   = True
opt3@DstESMF=True

opt3@InterpMethod="neareststod"
ESMF_regrid_gen_weights(cwrf_s_des, colm_des, cwrf_s2colm_wgts_bl, opt3)
opt3@InterpMethod="neareststod"
ESMF_regrid_gen_weights(cwrf_s_des, colm_des, cwrf_s2colm_wgts_pat, opt3)

; colm=>cwrf
opt4=True
opt4@ForceOverwrite = True
opt4@PrintTimings   = True
opt4@SrcESMF=True

opt4@InterpMethod="neareststod"

ESMF_regrid_gen_weights(colm_des, cwrf_s_des, colm_2cwrf_s_wgt_bl, opt4)

opt4@InterpMethod="neareststod"
ESMF_regrid_gen_weights(colm_des, cwrf_s_des, colm_2cwrf_s_wgt_pat, opt4)





; cwrf=>fvcom
opt3=True
opt3@ForceOverwrite = True
opt3@PrintTimings   = True
opt3@DstESMF=True

opt3@InterpMethod="bilinear"
ESMF_regrid_gen_weights(cwrf_s_des, fvcom_n_des, cwrf_s2fvcom_n_wgts_bl, opt3)
ESMF_regrid_gen_weights(cwrf_s_des, fvcom_e_des, cwrf_s2fvcom_e_wgts_bl, opt3)

opt3@InterpMethod="patch"
ESMF_regrid_gen_weights(cwrf_s_des, fvcom_n_des, cwrf_s2fvcom_n_wgts_pat, opt3)
ESMF_regrid_gen_weights(cwrf_s_des, fvcom_e_des, cwrf_s2fvcom_e_wgts_pat, opt3)


; fvcom=>cwrf
opt4=True
opt4@ForceOverwrite = True
opt4@PrintTimings   = True
opt4@SrcESMF=True

opt4@InterpMethod="bilinear"

ESMF_regrid_gen_weights(fvcom_n_des, cwrf_s_des, fvcom_n2cwrf_s_wgts_bl, opt4)
ESMF_regrid_gen_weights(fvcom_e_des, cwrf_s_des, fvcom_e2cwrf_s_wgts_bl, opt4)

opt4@InterpMethod="patch"
ESMF_regrid_gen_weights(fvcom_n_des, cwrf_s_des, fvcom_n2cwrf_s_wgts_pat, opt4)
ESMF_regrid_gen_weights(fvcom_e_des, cwrf_s_des, fvcom_e2cwrf_s_wgts_pat, opt4)
















; exit

; ; fvcom=>cwrf
; ; opt@SrcESMF=True
; ; ESMF_regrid_gen_weights("fvcom_grd.nc", "cwrf_grd.nc", "fvcom2cwrf.nc", opt)




; ; t2m=f3->T2M(0,:,:)
; ; Opt                = True
; ; Opt@PrintTimings   = True
; ; wgtFileName="cwrf2fvcom.nc"
; ; temp_regrid = ESMF_regrid_with_weights(t2m,wgtFileName,Opt)
; ; printVarSummary(temp_regrid)
; ; print (temp_regrid@lat1d+" "+temp_regrid@lon1d)




; temp_fvcom=new(14630, "float")
; do i=0,dimsizes(temp_fvcom)-1
; 	temp_fvcom(i)=i
; end do
; Opt                = True
; Opt@PrintTimings   = True
; wgtFileName="fvcom2cwrf.nc"
; temp_regrid = ESMF_regrid_with_weights(temp_fvcom,wgtFileName,Opt)



; wks  = gsn_open_wks("pdf","fvcomcwrf")    ; send graphics to PNG file

; res1=True
; res1@vpWidthF=0.6
; res1@vpHeightF=0.6
; res1@cnLinesOn=False
; res1@gsnFrame=False
; res1@gsnDraw=False
; res1@cnLineLabelsOn=False
; ; res1@cnFillOn            = True
; plot = gsn_csm_contour(wks,(/temp_regrid/),res1)      ; contour the variable




; res2=True
; res2@gsMarkerSizeF=0.01
; res2@gsMarkerIndex=1
; ; res2@gsnFrame=False
; ; res2@gsnDraw=False

; res3=True
; res3@gsMarkerSizeF=0.006
; res3@gsMarkerIndex=1
; res3@gsMarkerColor="red"

; do i=0,138-1
; 	do j=0, 195-1
; 		if ( .not. ismissing(temp_regrid(i,j)))  then
; 			print (i+" "+j) 
; 			gsn_polymarker(wks, plot, j, i, res2)
; 		end if

; 		if ( sc_water(i, j).eq.6) then
; 			gsn_polymarker(wks, plot, j, i, res3)
; 		end if
; 	end do
; end do
	




; draw(plot)
; frame(wks)










