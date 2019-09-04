FREQ=1h
CONFIG=eNATL60
CASE=BLB002

for month in $(seq 1 12); do

    case $month in
      7|8|9|10|11|12) year=2009;;
         1|2|3|4|5|6) year=2010;;
    esac

    case $month in
               7|8|9|10) CASEi=${CASE};;
         11|12|1|2|3|4|5|6) CASEi=${CASE}X;;
    esac


    mm=$(printf "%02d" $month)

#    for var in sossheig sozocrtx somecrty; do
    for var in sozocrtx; do

      case $var in
        sossheig) filetyp=gridT-2D;;
        sozocrtx) filetyp=gridU-2D;;
        somecrty) filetyp=gridV-2D;;
      esac

      stdir=/store/CT1/hmg2840/lbrodeau/${CONFIG}/${CONFIG}-${CASEi}-S

      for file in $(ls $stdir/*/${CONFIG}-${CASEi}_${FREQ}_*_${filetyp}_${year}${mm}??-${year}${mm}??.nc); do

	lfs hsm_state $file | grep -q release
     	if [ $? = 0 ] ; then
       		echo $file released
       		fo=$(ls -l $file | awk '{print $NF}')
       		lfs hsm_restore $fo
     		lfs hsm_state $f | grep -q release
	fi
      done
    done	
done

