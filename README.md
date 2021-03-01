# Beam Test Assignment
# Ravi Bhargava

Get code from repo: 

>git clone https://github.com/ravi-softwarethreads/beamtest.git

To compile project

>cd beamtest/beamtest

Maven needs to be installed on machine

>mvn install

To run:

>mvn compile exec:java -Dexec.mainClass="com.beamtest.ProcessLog" -Dexec.args="--outputOne=output/routeone/one.txt --outputTwo=output/routetwo/two.txt --outputArchive=archive/archive.txt"

Input is taken from file input/input.txt in the distribution
There are three output file generated :

output/routeone/one.txt for valid log entries with log format "1" specified with arg --outputOne=output/routeone/one.txt 
output/routetwo/two.txt for valid log entries with log format "2" specified with arg --outputTwo=output/routetwo/two.txt
archive/archive.txt for all valid log entries in input specified with arg --outputArchive=archive/archive.txt


                                   output/routetwo for log entries with log format "2"
                                   archive
 
 
