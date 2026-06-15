infile = addfile("wrfinput_d01", "w")
rf     = addfile("./geo_em.d01.nc", "r")

; 读取土地利用和植被信息
lu     = rf->LU_INDEX
fvc    = infile->XFVEG(:,:,:)
oldfvc = infile->XFVEG
xland  = infile->LANDMASK
scw    = infile->SC_WATER

; 检查不一致情况
scw = where(scw.eq.0, 8, scw)                    
inconsisten_scw_xland   = where(scw .le. 3 .and. xland .eq. 0, 1, 0)
inconsisten_xfveg_xland = where(fvc .gt. 0 .and. xland .eq. 0, 1, 0)

; 修正植被覆盖对应的土地利用
oldfvc = where(oldfvc .gt. 0 .and. lu .eq. 16, 0, oldfvc)
inconsisten_fvc_lu = where(fvc .gt. 0 .and. lu .eq. 16, 1, 0)
inconsisten_ldm_lu = where(xland .ne. 0 .and. lu .eq. 16, 1, 0)

; 处理缺失值
inconsisten_fvc_lu = where(ismissing(inconsisten_fvc_lu), 0, inconsisten_fvc_lu)
inconsisten_ldm_lu = where(ismissing(inconsisten_ldm_lu), 0, inconsisten_ldm_lu)

; 修正土地利用
lu = where(inconsisten_fvc_lu .eq. 1 .or. inconsisten_ldm_lu .eq. 1, 7, lu)
lu = where(lu .eq. 24, 22, lu)

; 写回文件
infile->LU_INDEX = lu
infile->SC_LANDU = lu
infile->IVGTYP   = toint(lu)
infile->XFVEG    = oldfvc
infile->SC_WATER = scw

