#!/bin/bash

BASEDIR=/Users/evanjiang/Desktop/comp_512/COMP512_A2/comp512p2
group=26
gameid=game-$group-players5_interval300

export autotesthost=127.0.0.1

# Configure processes based on number of players
export process1=${autotesthost}:8001
export process2=${autotesthost}:8002
export process3=${autotesthost}:8003
export process4=${autotesthost}:8004
export process5=${autotesthost}:8005

maxmoves=$1
interval=$2
randseed=$3

if [[ ! -d $BASEDIR ]]
then
    echo "Error $BASEDIR is not a valid dir."
    exit 1
fi

if [[ ! -f $BASEDIR/comp512p2.jar ]]
then
    echo "Error cannot locate $BASEDIR/comp512p2.jar . Make sure it is present."
    exit 1
fi

if [[ ! -d $BASEDIR/comp512st ]]
then
    echo "Error cannot locate $BASEDIR/comp512st directory . Make sure it is present."
    exit 1
fi

export CLASSPATH=$BASEDIR/comp512p2.jar:$BASEDIR/bin:$BASEDIR

# Build the process group string.
export processgroup=$(env | grep '^process[1-9]=' | sort | sed -e 's/.*=//')
processgroup=$(echo $processgroup | sed -e 's/ /,/g')

numplayers=$(echo $processgroup | awk -F',' '{ print NF}')

export TIMONOCHROME=true
export UPDATEDISPLAY=false

playernum=1
for process in $(echo $processgroup | sed -e 's/,/ /g')
do
    echo "Starting player $playernum on $process"
    java comp512st.tests.TreasureIslandAppAuto $process $processgroup $gameid $numplayers $playernum $maxmoves $interval $randseed$playernum  > $gameid-$playernum-display.log 2>&1 &
    playernum=$(expr $playernum + 1);
done

# Don't use wait - let the outer script monitor completion
echo "Test processes started for $gameid"
