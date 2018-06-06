#!/bin/bash
BENCH=/home/mengcz/fox/fox
RESDIR=$1
CURPWD=$PWD
if [ -d "$RESDIR" ]; then
    sudo rm -rf "$RESDIR"
fi
mkdir -p "$RESDIR"

#uOC0
RESDIR_UOC0="$RESDIR/UOC0"
for RRATE in 0 25 50 75 100; do
    TMPRESD="$RESDIR_UOC0/$RRATE"
    mkdir -p $TMPRESD
    pushd $TMPRESD
    sudo $BENCH run -d /dev/nvme0n1 -j 1 -c 1 -l 1 -b 32 -p 64 -m 1 -r $RRATE -w $((100-RRATE)) -v 8 -e 1 -o > run.log
    popd
done

#uOC2
RESDIR_UOC2="$RESDIR/UOC2"
for TNUM in 1 2 4; do
    if [ $TNUM = 1 ]; then
        for LNUM in 1 2 4; do
            TMPRESD="$RESDIR_UOC2/${TNUM}_${LNUM}_R"
            mkdir -p $TMPRESD
            pushd $TMPRESD
            sudo $BENCH run -d /dev/nvme0n1 -j $TNUM -c 1 -l $LNUM -b 1 -p 1 -m 1 -r 100 -w 0 -v 8 -e 2 -o > run.log
            popd
            TMPRESD="$RESDIR_UOC2/${TNUM}_${LNUM}_W"
            mkdir -p $TMPRESD
            pushd $TMPRESD
            sudo $BENCH run -d /dev/nvme0n1 -j $TNUM -c 1 -l $LNUM -b 1 -p 1 -m 1 -r 0 -w 100 -v 8 -e 2 -o > run.log
            popd
        done
    else
        LNUM=$TNUM
        TMPRESD="$RESDIR_UOC2/${TNUM}_${LNUM}_R"
        mkdir -p $TMPRESD
        pushd $TMPRESD
        sudo $BENCH run -d /dev/nvme0n1 -j $TNUM -c 1 -l $LNUM -b 1 -p 1 -m 1 -r 100 -w 0 -v 8 -e 2 -o > run.log
        popd
        TMPRESD="$RESDIR_UOC2/${TNUM}_${LNUM}_W"
        mkdir -p $TMPRESD
        pushd $TMPRESD
        sudo $BENCH run -d /dev/nvme0n1 -j $TNUM -c 1 -l $LNUM -b 1 -p 1 -m 1 -r 0 -w 100 -v 8 -e 2 -o > run.log
        popd
    fi
done

#uOC3
RESDIR_UOC3="$RESDIR/UOC3"
for PANUM in 1 2 4; do
    # vector
    TMPRESD="$RESDIR_UOC3/${PANUM}_V_R"
    mkdir -p $TMPRESD
    pushd $TMPRESD
    sudo $BENCH run -d /dev/nvme0n1 -j 1 -c $PANUM -l 1 -b 1 -p 1 -m 1 -r 100 -w 0 -v $((8*PANUM)) -e 2 -o > run.log
    popd
    TMPRESD="$RESDIR_UOC3/${PANUM}_V_W"
    mkdir -p $TMPRESD
    pushd $TMPRESD
    sudo $BENCH run -d /dev/nvme0n1 -j 1 -c $PANUM -l 1 -b 1 -p 1 -m 1 -r 0 -w 100 -v $((8*PANUM)) -e 2 -o > run.log
    popd
    # parallel
    TMPRESD="$RESDIR_UOC3/${PANUM}_P_R"
    mkdir -p $TMPRESD
    pushd $TMPRESD
    sudo $BENCH run -d /dev/nvme0n1 -j $PANUM -c $PANUM -l 1 -b 1 -p 1 -m 1 -r 100 -w 0 -v 8 -e 2 -o > run.log
    popd
    TMPRESD="$RESDIR_UOC3/${PANUM}_P_W"
    mkdir -p $TMPRESD
    pushd $TMPRESD
    sudo $BENCH run -d /dev/nvme0n1 -j $PANUM -c $PANUM -l 1 -b 1 -p 1 -m 1 -r 0 -w 100 -v 8 -e 2 -o > run.log
    popd
done
