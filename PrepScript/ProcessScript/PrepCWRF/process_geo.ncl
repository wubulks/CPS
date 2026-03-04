setfileoption("nc","Format","LargeFile")


system("cp -f ./geo_em.d01.nc a.d01.nc")
fw=addfile("a.d01.nc","w")
landmask=fw->LANDMASK

name="XMOANI"
var=fw->$name$
var=where(var.le.0.3,0.3,var)
fw->$name$=var
delete(var)

print("XMOANI is done.")
name="XFSKY"
var=fw->$name$
var=where(var.le.0.0,1.0,var)
fw->$name$=var
delete(var)
print("XFSKY is done.")

name="BEDROCK"
sf=fw->sea_floor
var=fw->$name$
fw->$name$=var
delete(var)
print("BEDROCK is done.")


; flowin=addfile("/stu01/zhanghan/CWRF/CWPS_static/geo_em.d01_updated201705.nc","r")
; flowdir=flowin->FLOWDIR
; xflowacn=flowin->XFLOWACN

; geoflowdir=fw->FLOWDIR
; ; geoxflowacn=fw->XFLOWACN
; printVarSummary(flowdir)
; printVarSummary(geoflowdir)
; printVarSummary(xflowacn)
; geoflowdir(:,:,:,:)=flowdir(:,:,:,:)

; fw->FLOWDIR=geoflowdir
; fw->XFLOWACN=xflowacn

fdirfil=addfile("./fdir.nc","r")
fdir=fdirfil->fdir
geoflowdir=fw->FLOWDIR
geoflowdir(0,0,:,:)=tofloat(fdir(:,:))
geoflowdir(0,1,:,:)=tofloat(fdir(:,:))
fw->FLOWDIR=geoflowdir

accfil=addfile("./acc.nc","r")
acc=landmask
acc(0,:,:)=tofloat(accfil->acc)
acc@description="FlowACN"
acc@FieldType = 104 ;
acc@MemoryOrder = "XY " ;
acc@units = "" ;
acc@stagger = "M" ;
acc@sr_x = 1 ;
acc@sr_y = 1 ;
fw->XFLOWACN=acc


;-----chaochao
name="FR_SAND"
var=fw->$name$
var=var/10000.0

dimuse=dimsizes(var)
do i=0,dimuse(3)-1
    do j=0,dimuse(2)-1
        if landmask(0,j,i).eq.1 then
            do k=0,dimuse(1)-1
                if var(0,k,j,i).lt.0 then
                    var(0,k,j,i)=var(0,0,j,i)
                end if
            end do
        end if
    end do
end do
var=where(var.eq.-999.or.var.eq.1.e20,-999,var)
var=where(var.eq.-1.0,0.5,var)

fw->$name$=var
delete(var)
print("FR_SAND is done.")

name="FR_CLAY"
var=fw->$name$
var=var/10000.0
var=where(var.eq.-999.or.var.eq.1.e20,-999,var)
var=where(var.eq.-1.0,0.5,var)
fw->$name$=var
sis=dimsizes(var)
delete(var)
print(name+" is done.")

tsc = fw->FR_SAND + fw->FR_CLAY
frs = fw->FR_SAND
frc = fw->FR_CLAY

frs = where(tsc.lt.0.1,0.5,frs)
frc = where(tsc.lt.0.1,0.5,frc)

fw->FR_SAND = frs
fw->FR_CLAY = frc


var=fw->FLOWDIR
print("0000000000000000000000000000000000000")
printVarSummary(var)
print("0000000000000000000000000000000000000")
var(:,:,:,:)=resolution*1000. ;=dx {m}
var@units = "m" ;
var@description = "channel widths across flowdir" ;
var@sr_y = 1 ;
var@sr_x = 1 ;
var@stagger = "M" ;
var@FieldType = 104 ;
var@MemoryOrder = "XYZ" ;
var@_FillValue = -999.0 ;
name="CHWD"
fw->$name$=var
delete(var)
print(name+" is done.")


name="BEDROCK"
var=fw->$name$
var=where(var.le.0.2,0.2,var)
fw->$name$=var
delete(var)
print(name+" is done.")

name="XALBSOL"
solour_table = (/0.12, 0.11, 0.10, 0.09, 0.08, 0.07, 0.06, 0.05/)
var=fw->SC_WATER ; sci
dim=dimsizes(var)
do i=0,dim(1)-1
    do j=0,dim(2)-1
        var(0,i,j)=(/solour_table(floattoint(var(0,i,j))-1)/)
    end do
end do
var@description="soil albedo (visible beam) when wet"
var@FieldType = 104 ;
var@MemoryOrder = "XY " ;
var@units = "" ;
var@stagger = "M" ;
var@sr_x = 1 ;
var@sr_y = 1 ;
fw->$name$=var
delete(var)
delete(dim)

print(name+" is done.")

; now 24 category's consistency

name="LANDF,LANDUSEF CONSISTENCY" ;==================================
var=new((/1,sis(2),sis(3)/),"float")
landusef=fw->LANDUSEF
landmask=fw->LANDMASK
landusef=where(landusef.eq.-9999.,-999.,landusef)
landusef=where(landusef.gt.1.0e10,1.e20,landusef)
landusef@_FillValue = -999.
landmask=where(landmask.eq.-9999.,-999.,landmask)
landmask=where(landmask.gt.1.0e10,1.e20,landmask)
landmask@_FillValue = -999.
dim=dimsizes(landusef)
sluf=dim_sum_n(landusef,1) ; Time,land_cat,south_north,west_east
sluf=1.0/sluf
do i=0,dim(1)-1
    landusef(0,i,:,:)=landusef(0,i,:,:)*sluf(0,:,:)
end do

m=0
index_lf=new(dimsizes(var),"float")
do i=0,dim(2)-1
    do j=0,dim(3)-1
        index_lf(0,i,j)=int2flt(maxind(landusef(0,:,i,j)))+1.0
        if(floattoint(index_lf(0,i,j)) .eq. 16) then
           if(landusef(0,15,i,j) .le. 0.5) then
              rxum = landusef(0,15,i,j)
              landusef(0,15,i,j) = -1.
              index_lf(0,i,j) = int2flt(maxind(landusef(0,:,i,j)))+1.0
              m=m+1
              landusef(0,15,i,j) = rxum
           end if
        end if
    end do
end do
print(name+" is done.")

print("less than 0.5 points="+m)
printVarSummary(index_lf)

scwater=fw->SC_WATER
XORO=fw->XORO
index_lf=where(index_lf.eq.24.and.XORO.eq.0,14,index_lf)
delete(scwater)
index_lf=where(index_lf.eq.24,22,index_lf)
fw->LU_INDEX=index_lf
sluf=dim_sum_n(landusef,1)

var2=(/0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,  \
       16,17,18,19,20,21,22,23/)
; landf=new((/1,sis(2),sis(3)/),"float");fw->LANDF
landf=fw->SC_WATER
landf(:,:,:)=0.
do i=0,dimsizes(var2)-1
    landf=landf+landusef(:,var2(i),:,:)
end do
var@description="fraction of land cover in a grid"
xoro=fw->XORO
do i=0,dim(2)-1
    do j=0,dim(3)-1
        if (index_lf(0,i,j).eq.16..and.\
           (landf(0,i,j).gt.0.50.or.xoro(0,i,j).eq.1.0)) then
           print("bad point , water but landf > 0.5")
           print ((/i, j/))
           print ((/landf(0,i,j), landusef(0,15,i,j)/))
           landusef(0,:,i,j)=landusef(0,:,i,j)*0.499/landf(0,i,j)
            landusef(0,15,i,j)=1-sum(landusef(0,:,i,j))+landusef(0,15,i,j) ;add 22sep 2009
           landf(0,i,j)=0.499
        end if
        if (index_lf(0,i,j).ne.16.0.and.\ ;not sea water
           (landf(0,i,j).lt.0.5 .or.(xoro(0,i,j).ne.1.0 .and. xoro(0,i,j).ne.2.0))) then ;非海水 且 landf 小于 0.5 或者 oro =0
           print("bad point , land but landf < 0.5 landf="+landf(0,i,j)+" oro="+xoro(0,i,j))
           print("i="+i+" j="+j+" oro="+xoro(0,i,j)+" landf="+landf(0,i,j)+" index_lf="+index_lf(0,i,j))
           landusef(0,:,i,j)=landusef(0,:,i,j)*0.501/landf(0,i,j)
           tpv=1-sum(landusef(0,:,i,j))
           do k=0,23
              if (k.ne.15) then ; not sea
                  landusef(0,k,i,j)=landusef(0,k,i,j)+tpv/23.
              end if
           end do
            delete(tpv)
           landf(0,i,j)=0.501
         end if
    end do
end do
xoro=where(index_lf.ne.16.and.(xoro.eq.0),1,xoro)
landf=where(((index_lf.eq.16.and.(landf.gt.0.5.or.xoro.eq.1)).or.(index_lf.ne.16.and.(landf.lt.0.5.or. (xoro.ne.1 .and. xoro.ne.2))) ).and.landf.le.0.5,0.501,landf)
delete(var)
var=floattoint(where( ((index_lf.eq.16.and.(landf.gt.0.5.or.xoro.eq.1)).or. \
                       (index_lf.ne.16.and.(landf.lt.0.5.or. (xoro.ne.1 .and. xoro.ne.2))) ),1.,0.))

oooo=where(index_lf.eq.16.and.(landf.gt.0.5.or.xoro.eq.1),1,0)
print(sum(oooo)+"total")

oooo=where(index_lf.eq.16.and.(landf.gt.0.5),1,0)
print(sum(oooo)+"totalssssssss")

oooo=where(index_lf.eq.16.and.(xoro.eq.0),1,0)
print(sum(oooo)+"total0")

oooo=where(index_lf.eq.16.and.(xoro.eq.1),1,0)
print(sum(oooo)+"total1")

oooo=where(index_lf.eq.16.and.(xoro.eq.2),1,0)
print(sum(oooo)+"total2")

oooo=where(index_lf.eq.16.and.(xoro.eq.3),1,0)
print(sum(oooo)+"total3")

oooo=where(index_lf.eq.16.and.(landf.lt.0.5),1,0)
print(sum(oooo)+"total")
; landf
if (sum(var).gt.0) then
    print(sprinti("%i",sum(var))+\
    " Error points LU_INDEX is 16 while landf bigger than 0.5 or XORO is 1" \
    )
    print("SCRIPT STOP!!!")
do i=0,dim(2)-1
    do j=0,dim(3)-1
       if( (index_lf(0,i,j) .eq. 16 .and. (landf(0,i,j) .gt. 0.5 .or. xoro(0,i,j) .eq. 1)) .or. \
           (index_lf(0,i,j) .ne. 16 .and. (landf(0,i,j) .lt. 0.5 .or. (xoro(0,i,j) .ne. 1 .and. xoro(0,i,j) .ne. 2))) ) then
        ;    print("i="+i+" j="+j)
           print ((/index_lf(0,i,j), landf(0,i,j), xoro(0,i,j)/))
           xoro(0,i,j)=1
       end if
    end do
end do
fw->XORO=xoro
end if

landf@units = "" ;
landf@description = "fraction of land cover in a grid" ;
landf@sr_y = 1 ;
landf@sr_x = 1 ;
landf@stagger = "M" ;
landf@FieldType = 104 ;
landf@MemoryOrder = "XY " ;
landf@_FillValue = -999.0
fw->LANDF=landf
fw->LANDUSEF=landusef
print("LANDF is done.")


name="BEDROCK"
bedrock=fw->$name$
name="DPLAKE"
DPLAKE=fw->$name$
name="sea_floor"
sea_floor=fw->$name$
do i=0,dim(2)-1
    do j=0,dim(3)-1
       if(xoro(0,i,j).eq.0) then
         if((sea_floor(0,i,j).lt.0).and.(sea_floor(0,i,j).gt.-500)) then
         bedrock(0,i,j)=abs(sea_floor(0,i,j))*100
         else
         bedrock(0,i,j)=10
         end if
       end if

       if(xoro(0,i,j).eq.2) then
         if((sea_floor(0,i,j).lt.0).and.(sea_floor(0,i,j).gt.-500)) then
         bedrock(0,i,j)=abs(sea_floor(0,i,j))*100
         else
         bedrock(0,i,j)=10
         end if
       end if

       if(xoro(0,i,j).eq.3) then
         if((DPLAKE(0,i,j).gt.0).and.(DPLAKE(0,i,j).lt.300)) then
         bedrock(0,i,j)=DPLAKE(0,i,j)
         else
         bedrock(0,i,j)=1
         end if
       end if
    end do
end do

fw->BEDROCK=bedrock
print(name+" is done.")

delete(sluf)
delete(index_lf)
delete(var)
delete(landusef)
delete(landf)
delete(var2)
delete(xoro)

name="XLANDMASK"
lnd_new=new((/1,sis(2),sis(3)/),"float");fw->$name$
lnd=fw->LANDMASK
copy_VarMeta(lnd,lnd_new)
lnd_new=lnd
do i=0,dim(2)-1
    do j=0,dim(3)-1
if(lnd(0,i,j).lt.0)then
lnd(0,i,j)=0
end if
    end do
end do
fw->$name$=lnd_new
fw->LANDMASK=lnd
print(name+" is done.")


print("XUMCHECK  Add Variables Over.")
print("========================================================================")
system("mv -f a.d01.nc geo_em.d01_xum.nc")
print("========================================================================")
system("cp -f geo_em.d01_xum.nc b.d01.nc")
infile=addfile("b.d01.nc","w")


lu_index=infile->LU_INDEX
infile->SC_LAND=lu_index
sc_water=infile->SC_WATER
XLANDMASK=infile->XLANDMASK
sc_water=where(sc_water.eq.8.and.XLANDMASK.eq.1,6,sc_water)
infile->SC_WATER=sc_water

inveg=addfile("./MODIS2CWRF_SBC_d01.nc","r")
hi_res_VAR=sc_water
hi_res_VAR(:,:,:)=inveg->FVC(:,0,:,:)*1.0
xlandmask=infile->LANDMASK
printVarSummary(xlandmask)
printVarSummary(hi_res_VAR)
hi_res_VAR=where(xlandmask.eq.0,0,hi_res_VAR)

hi_res_VAR=where(hi_res_VAR.eq.-999.or.hi_res_VAR.eq.1.e20,0,hi_res_VAR)
hi_res_VAR@units = "fraction" ;
hi_res_VAR@description = "vegetation fraction" ;
hi_res_VAR@sr_y = 1 ;
hi_res_VAR@sr_x = 1 ;
hi_res_VAR@stagger = "M" ;
hi_res_VAR@FieldType = 104 ;
hi_res_VAR@MemoryOrder = "XY " ;
hi_res_VAR@_FillValue = -999.0 ;
infile->XFVEG=hi_res_VAR


system("mv -f b.d01.nc geo_em.d01_veg.nc")
print("========================================================================")

fw=addfile("geo_em.d01_veg.nc","w")
upoint=0
landmask=fw->XLANDMASK
fsand=fw->FR_SAND ; sci
fclay=fw->FR_CLAY ; sci
dim=dimsizes(fsand)
print(dim)
printVarSummary(fsand)
do i=0,dim(2)-1
    do j=0,dim(3)-1
if (landmask(0,i,j).eq.-1)then
      do k=0,dim(1)-1
         if (((fsand(0,k,i,j)).le.0).or.((fsand(0,k,i,j)).gt.100)) then
         print("i="+i+" j="+j+" k="+k+" fsand="+fsand(0,k,i,j)+" landmask="+landmask(0,i,j))
upoint=upoint+1
         end if
         if (((fclay(0,k,i,j)).le.0).or.((fclay(0,k,i,j)).gt.100)) then
         print("i="+i+" j="+j+" k="+k+" fsand="+fsand(0,k,i,j)+" landmask="+landmask(0,i,j))
upoint=upoint+1
         end if
      end do
        end if
    end do
end do
print("undefied points= "+upoint)

print("========================================================================")
fw=addfile("geo_em.d01_veg.nc","w")
name :=        (/"BEDROCK","XFSKY","XFVEG","XHSDV","XMOANG","XMOANI","XMOSTD","XORASP","XORO","XORSLO","XSGASP","XSLPXS","XSLPYS","XSOSLO","XSOSTD"/)
upbd :=        (/10       ,1      ,1      ,1000   ,20      ,1       ,8000    ,10      ,1     ,1       ,5       ,1,1,1,500/)
lwbd :=        (/0.3      ,0.5    ,0.0    ,0.001  ,0.0001  ,0       ,0.000  ,0.001   ,0     ,0       ,0.01    ,0,0,0,0/)
typicalvalue:= (/1.2      ,0.7    ,0.1    ,0.01   ,0.1     ,0.7     ,0    ,0.1     ,1     ,0       ,0.1     ,0,0,0,0/)
landmask:=fw->LANDMASK
cnt := dimsizes(name)

do idx=0,cnt-1
field_2d=fw->$name(idx)$ ; sci
print("Checking"+name(idx))
; dim=dimsizes(field_2d)

mask2d:=conform(field_2d,landmask(0,:,:),(/1,2/))
field_2d=where( mask2d.gt.0.5 .and. (ismissing(field_2d) .or. field_2d .gt. upbd(idx).or.field_2d.lt.lwbd(idx)       ),typicalvalue(idx), field_2d)
field_2d=where( mask2d.lt.0.5 .and. (ismissing(field_2d) .or. field_2d .gt. upbd(idx).or.field_2d.lt.lwbd(idx)       ),0                , field_2d)



fw->$name(idx)$=field_2d ; sci
delete(field_2d)
end do

print("========================================================================")
fw=addfile("geo_em.d01_veg.nc","w")
name :=        (/"XSALF","LANDUSEF" ,"SOILCBOT","SOILCTOP","XSGSRF"/)
upbd :=        (/3       ,1             ,1         ,1         ,5/)
lwbd :=        (/0.01    ,0.0         ,0         ,0         ,0.00/)
typicalvalue :=(/1       ,0.1           ,0         ,0         ,0.1/)
landmask:=fw->LANDMASK
cnt := dimsizes(name)

do idx=0,cnt-1
field_3d=fw->$name(idx)$ ; sci
print("Checking"+name(idx))
mask3d:=conform(field_3d,landmask(0,:,:),(/2,3/))
field_3d=where( mask3d.gt.0.5 .and. (ismissing(field_3d) .or. field_3d .gt. upbd(idx).or.field_3d.lt.lwbd(idx)       ),typicalvalue(idx), field_3d)

fw->$name(idx)$=field_3d ; sci
delete(field_3d)
end do
