# TODO: Edit this dir name to where your comp512.jar and build_tiapp.sh script are.
BASEDIR=/Users/evanjiang/Desktop/comp_512/COMP512_A2/comp512p2
BINDIR=$BASEDIR/bin

if [[ ! -d $BASEDIR ]]; then
    echo "Error $BASEDIR is not a valid dir."
    exit 1
fi

if [[ ! -f $BASEDIR/comp512p2.jar ]]; then
    echo "Error cannot locate $BASEDIR/comp512p2.jar. Make sure it is present."
    exit 1
fi

if [[ ! -d $BASEDIR/comp512st ]]; then
    echo "Error cannot locate $BASEDIR/comp512st directory. Make sure it is present."
    exit 1
fi

# Create bin directory if it doesn't exist
mkdir -p $BINDIR

export CLASSPATH=$BASEDIR/comp512p2.jar:$BASEDIR
cd $BASEDIR

# Compile all Java files into the bin directory
javac -d $BINDIR $(find -L comp512st -name '*.java')

echo "Build complete. Classes are in $BINDIR."