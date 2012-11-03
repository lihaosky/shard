if [ $# -le 0 ]
then
echo 'Usage: ./generateLoad "[options]"'
exit
fi
java -cp . com.yahoo.ycsb.util.LoadGenerator $1
