#!/bin/bash
# ECE 454 Spring 2020 Assignment 3
# Ben and friends' full test script
# Use this to test against various A3 failure scenarios and see your overall performance

# INSTRUCTIONS: simply run `bash fulltest.sh` from within the same directory as the rest
# of your A3 code, on a valid ece linux instance (ideally not one listed in $SERVER_HOSTS).

# CONDITIONS: Have your ssh public key stuff figured out (for auto ssh login) and that permissions 700 thingy.
# And make sure build.sh isn't broken. DM me if this script is broken tho (or isn't comprehensive enough)

# DISCLAIMER: Use this script at your own discretion. I ain't perfect and this script might not be either,
# but we're all beautiful in our own ways... sorta
# Good luck, fam


# ---------- User variables (change as desired) ----------

# set to 1 if you know how to use this script
HIDE_HELP=0

# 0=debug (verbose), 1=normal, 2=minimal (final output only)
OUTPUT_LEVEL=1

# location for log file
OUTPUT_LOG="$PWD/log_fulltest.txt"

# list of hosts to run the servers on (script will rotate through them, running up to 2 at once)
SERVER_HOSTS=(eceubuntu1 eceubuntu2 ecetesla2 ecetesla3)

# 0=random port every time a server starts, 1=servers reuse ports
SERVER_PORT_REUSE=1

# 0=kill/run servers in cyclic order, 1=kill/run random server each time
KILL_RANDOM_SERVER=1

# seconds to delay between starting and killing servers
KILL_DELAY=2

# list of num_threads for client to use
NUM_THREADS_POOL=(1 4 8)

# list of num_seconds for client to use
NUM_SECONDS_POOL=(20)

# list of keyspace_size for client to use
KEYSPACE_SIZE_POOL=(1000 100000 1000000)

# --------------------------------------------------------



# ------ Sourcing and constants ------
FULLTEST_SCRIPT_VERSION=1

source settings.sh
JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

# Temporary files
CLIENT_OUT="client_output.txt"
LIN_IN="lintest_input"
LIN_OUT="lintest_output"

ALL_SERVER_HOSTS=(eceubuntu1 eceubuntu2 ecetesla0 ecetesla1 ecetesla2 ecetesla3 eceubuntu4)

# Thresholds to meet (from assignment instructions)
THROUGHPUT_THRESH=3125 # throughput threshold to exceed for each thread (RPCs/s), based off 25k for 8 threads
LATENCY_THRESH=1 # latency threshold to stay below (ms)

LIN_VIOLATIONS_FC_THRESH=0 # 0 linearizability violations for full credit
LIN_VIOLATIONS_HC_THRESH=9 # 1-9 linearizability violations for half credit
LIN_VIOLATIONS_QC_THRESH=9999 # 10-9999 linearizability violations for quarter credit

BRIEF_DELAY=0.5 # delay before performing cleanup and starting servers (to catch recently started processes)
SSH_TIMEOUT=10

OUTPUT_LEVEL_NAMES=("verbose" "normal" "minimal")

# Define ANSI colour values
C_RED=31; C_GRN=32; C_YEL=33; C_BLU=34; C_MAG=35

# ------ Display funcs ------
disp() {
    # Display output text
    # Arguments: level, text, colour (optional)
    
    LEVEL="$1"
    TEXT="$2"
    [[ $# -ge 3 ]] && COLOUR="\033[$3m" || COLOUR=""

    CLEAR="\033[0m"
    PREFIX_TAG="[ECE454A3-Fulltest]"
    [[ $LEVEL -eq 0 ]] && PREFIX_LEVEL="[Debug]" || PREFIX_LEVEL=""
    PREFIX="$PREFIX_TAG$PREFIX_LEVEL"

    echo -e "$PREFIX $TEXT" >> $OUTPUT_LOG
    if [[ $LEVEL -lt $OUTPUT_LEVEL ]]; then return; fi
    echo -e --- "$PREFIX $COLOUR$TEXT$CLEAR"
}

showHelpPrompt() {
    # A help prompt to show by default when someone downloads this script :)
    # Disable this prompt by setting HIDE_HELP=1 on line 20

    if [[ $HIDE_HELP -ne 0 ]]; then return; fi

    disp 2 "You're using Ben's ECE454 A3 testing script! Good choice ;)"
    disp 2
    disp 2 "Seems like you haven't used this script before, so here's some instructions:"
    disp 2 "(To suppress this message, set HIDE_HELP=1 on line 20 of this script)" $C_RED
    disp 2 "There's a bunch of parameters that you can change to customize your testing near the"
    disp 2 " top of this script, modify them as you please."
    disp 2
    read -t 5
    disp 2 "The script will run the client in all permutations of configurations for the 3"
    disp 2 " input parameters: NUM_THREADS_POOL, NUM_SECONDS_POOL, and KEYSPACE_SIZE_POOL."
    disp 2 "You can provide more than 2 server hostnames in SERVER_HOSTS, and the script will"
    disp 2 " use them all, killing and starting servers so that there are 1 or 2 alive at a time."
    disp 2 "The host that you run this script on will start the client thread in another process,"
    disp 2 " so I recommend using a host that isn't in the SERVER_HOSTS list."
    disp 2
    read -t 5
    disp 2 "The rest of the functionality should be self-explanatory, so good luck! :)"
    disp 2 "Press enter to continue..." $C_GRN
    read -t 60
}

# ------ Client/server funcs ------
setup() {
    # Setup for script running, exits program if something goes wrong

    disp 1 "Compiling sources..."
    bash build.sh &>/dev/null
    if [[ $? -ne 0 ]]; then
        disp 2 "Compilation Error" $C_RED
        exit 1
    fi

    disp 1 "Creating ZooKeeper node..."
    $JAVA_HOME/bin/java CreateZNode $ZKSTRING /$USER &>/dev/null
    
    disp 0 "Killing any existing server nodes..."
    for SERVER in ${ALL_SERVER_HOSTS[@]}; do
        if [[ $SERVER == $HOSTNAME ]]; then continue; fi
        killServ $SERVER
    done
}

runServ() {
    # Start a new server on the given host
    # Arguments: server hostname

    HOST="$1"

    sleep $BRIEF_DELAY
    
    KV_PORT=`shuf -i 10000-10999 -n 1`
    if [[ $SERVER_PORT_REUSE -eq 1 ]]; then
        if [[ -n ${SERVER_PORTS[$HOST]} ]]; then
            KV_PORT=${SERVER_PORTS[$HOST]}
        else
            SERVER_PORTS[$HOST]=$KV_PORT
        fi
    fi

    RUN_SERV_CMD="export CLASSPATH='$CLASSPATH'; $JAVA_HOME/bin/java StorageNode \`hostname\` $KV_PORT $ZKSTRING /$USER"

    disp 0 "Starting server on $HOST:$KV_PORT..."
    (ssh -oStrictHostKeyChecking=no -oConnectTimeout=$SSH_TIMEOUT $HOST "cd $PWD && $RUN_SERV_CMD" &>/dev/null & )
}

killServ() {
    # Kill any running servers on the given host
    # Arguments: server hostname

    HOST="$1"
    disp 0 "Killing server on $HOST..."
    (ssh -oStrictHostKeyChecking=no -oConnectTimeout=$SSH_TIMEOUT $HOST "pgrep -U $USER -f StorageNode | xargs kill -9" &>/dev/null )
}

runClient() {
    # Run client script
    # Arguments: num_threads, num_seconds, keyspace_size

    disp 0 "Starting client on $HOSTNAME..."
    $JAVA_HOME/bin/java A3Client $ZKSTRING /$USER $1 $2 $3 > $CLIENT_OUT
}

clientIsActive() {
    # Determines if client thread is still active

    kill -0 $CLIENTPID &>/dev/null
}

getFirstServ() {
    # Determine which server to initially start
    # Returns: index of server

    if [[ $KILL_RANDOM_SERVER -eq 0 ]]; then
        echo 0
    else
        echo $(( $RANDOM % $NUM_SERVERS ))
    fi
}

getNextServ() {
    # Determine next server to start and then kill
    # Returns: index of server

    LAST_SERV=$1
    OTHER_SERV=$2

    if [[ $KILL_RANDOM_SERVER -eq 0 ]]; then
        NEXT_SERV=$(( ($LAST_SERV + 1) % $NUM_SERVERS ))
    else
        NEXT_SERV=$(( $RANDOM % $NUM_SERVERS ))
    fi

    if [[ $NEXT_SERV -eq $OTHER_SERV ]]; then
        NEXT_SERV=$(( ($NEXT_SERV + 1) % $NUM_SERVERS ))
    fi

    echo $NEXT_SERV
}

# ------ Results funcs ------
calcResults() {
    # Calculate results for the latest run, much of this is copied from `runclient.sh`

    THROUGHPUT=`cat $CLIENT_OUT | grep "Aggregate throughput:" | sed "s/.*throughput: \([0-9]*\).*/\1/"`
    LATENCY=`cat $CLIENT_OUT | grep "Average latency:" | sed "s/.*latency: \([0-9]*\.[0-9]*\).*/\1/"`

    mkdir $LIN_IN $LIN_OUT &>/dev/null
    cp execution.log $LIN_IN/ &>/dev/null
    $JAVA_HOME/bin/java ca.uwaterloo.watca.LinearizabilityTest $LIN_IN/ $LIN_OUT/ &>/dev/null
    cp $LIN_OUT/scores.log scores.txt &>/dev/null

    JUNK_OPS=`cat scores.txt | grep 'Score = 2' | wc -l`
    LIN_VIOLATIONS=`cat scores.txt | grep 'Score = 1' | wc -l`
}

showResults() {
    # Displays the results of the latest run, indicating if thresholds are met

    NORMALIZED_THROUGHPUT=`echo "$THROUGHPUT / $NUM_THREADS" | bc`
    THROUGHPUT_SUM=`echo "$THROUGHPUT_SUM + $NORMALIZED_THROUGHPUT" | bc`
    LATENCY_SUM=`echo "$LATENCY_SUM + $LATENCY" | bc`

    disp 0
    disp 0 "Total throughput: $THROUGHPUT RPCs/s"

    if [[ $NORMALIZED_THROUGHPUT -ge $THROUGHPUT_THRESH ]]; then
        disp 1 "Throughput: $NORMALIZED_THROUGHPUT RPCs/s per thread (meets expectations)" $C_GRN
    else
        disp 1 "Throughput: $NORMALIZED_THROUGHPUT RPCs/s per thread (below expectations)" $C_RED
    fi

    (( $(echo "$LATENCY < $LATENCY_THRESH" | bc -l) )) && OUTCOL=$C_GRN || OUTCOL=$C_YEL
    disp 1 "Latency: $LATENCY ms" $OUTCOL

    [[ $JUNK_OPS -eq 0 ]] && OUTCOL=$C_GRN || OUTCOL=$C_YEL
    disp 1 "Number of get operations returning junk: $JUNK_OPS" $OUTCOL

    if [[ $LIN_VIOLATIONS -le $LIN_VIOLATIONS_FC_THRESH ]]; then
        disp 1 "Number of other linearizability violations: $LIN_VIOLATIONS (full credit)" $C_GRN
    else
        INCORRECT_OUTPUTS=$(( $INCORRECT_OUTPUTS + 1 ))

        if [[ $LIN_VIOLATIONS -le $LIN_VIOLATIONS_HC_THRESH ]]; then
            disp 1 "Number of other linearizability violations: $LIN_VIOLATIONS (half credit)" $C_YEL
        elif [[ $LIN_VIOLATIONS -le $LIN_VIOLATIONS_QC_THRESH ]]; then
            disp 1 "Number of other linearizability violations: $LIN_VIOLATIONS (quarter credit)" $C_RED
        else
            disp 1 "Number of other linearizability violations: $LIN_VIOLATIONS (NO CREDIT)" $C_RED
        fi
    fi
}

showFinalResults() {
    # Displays the final results of all runs, indicating if thresholds are met

    AVG_THROUGHPUT=`echo "$THROUGHPUT_SUM / $NUM_TESTS" | bc`
    AVG_LATENCY=`echo "scale=2; $LATENCY_SUM / $NUM_TESTS" | bc -l`

    disp 2 "Final results after $NUM_TESTS tests:" $C_BLU

    if [[ $AVG_THROUGHPUT -ge $THROUGHPUT_THRESH ]]; then
        disp 2 "Average throughput: $AVG_THROUGHPUT RPCs/s per thread (meets expectations)" $C_GRN
    else
        disp 2 "Average throughput: $AVG_THROUGHPUT RPCs/s per thread (below expectations)" $C_RED
    fi

    if (( $(echo "$AVG_LATENCY < $LATENCY_THRESH" | bc -l) )); then
        disp 2 "Average latency: $AVG_LATENCY ms" $C_GRN
    else
        disp 2 "Average latency: $AVG_LATENCY ms" $C_YEL
    fi

    if [[ $INCORRECT_OUTPUTS -eq 0 ]]; then
        disp 2 "Correctness: All tests had correct output, nice work!" $C_GRN
    else
        disp 2 "Correctness: $INCORRECT_OUTPUTS tests failed, keep at it!" $C_RED
    fi
}

# ------ Cleanup funcs ------
cleanup() {
    # End running processes and remove temp files

    sleep $BRIEF_DELAY

    for SERVER in ${SERVER_HOSTS[@]}; do
        killServ $SERVER
    done

    disp 0 "Killing client process and SSH sessions on $HOSTNAME..."
    kill `pgrep -U $USER -f A3Client` &>/dev/null
    kill `pgrep -U $USER -f "ssh.*eceubuntu"` &>/dev/null

    disp 0 "Removing temporary output files..."
    rm $CLIENT_OUT &>/dev/null
    rm -r $LIN_IN  &>/dev/null
    rm -r $LIN_OUT &>/dev/null
}

captureInterruptFunc() {
    # Capture an interrupt (ctrl-C) to do cleanup

    echo
    disp 2 "Interrupt detected, performing cleanup and exiting" $C_MAG

    cleanup
    exit -1
}



# ------ It's time to get started, baby ------
NUM_TESTS=$(( ${#NUM_THREADS_POOL[@]} * ${#NUM_SECONDS_POOL[@]} * ${#KEYSPACE_SIZE_POOL[@]} ))
NUM_SERVERS=${#SERVER_HOSTS[@]}

THROUGHPUT_SUM=0
LATENCY_SUM=0
INCORRECT_OUTPUTS=0

showHelpPrompt
echo -n > $OUTPUT_LOG

disp 2 "Starting Ben's full test script :)" $C_MAG

if [[ $NUM_SERVERS -lt 2 ]]; then
    disp 2 "There need to be at least 2 servers supplied!" $C_RED
    exit 1
fi

disp 0 "Script version: $FULLTEST_SCRIPT_VERSION"
disp 2
disp 2 "Parameters:"
disp 2 "        OUTPUT_LEVEL: ${OUTPUT_LEVEL_NAMES[$OUTPUT_LEVEL]}"
disp 2 "          OUTPUT_LOG: $OUTPUT_LOG"
disp 2
disp 1 "         CLIENT_HOST: $HOSTNAME"
disp 1 "        SERVER_HOSTS: (${SERVER_HOSTS[*]})"
disp 1 "   SERVER_PORT_REUSE: `[[ $SERVER_PORT_REUSE -eq 1 ]] && echo true || echo false`"
disp 1 "  KILL_RANDOM_SERVER: `[[ $KILL_RANDOM_SERVER -eq 1 ]] && echo true || echo false`"
disp 1 "          KILL_DELAY: $KILL_DELAY s"
disp 1
disp 1 "           NUM_TESTS: $NUM_TESTS"
disp 1 "    NUM_THREADS_POOL: (${NUM_THREADS_POOL[*]})"
disp 1 "    NUM_SECONDS_POOL: (${NUM_SECONDS_POOL[*]})"
disp 1 "  KEYSPACE_SIZE_POOL: (${KEYSPACE_SIZE_POOL[*]})"
disp 1

setup

trap "captureInterruptFunc" INT # capture interrupt signal to do cleanup

# ------ Repeat tests with various client arguments ------
test_i=0
for NUM_THREADS in ${NUM_THREADS_POOL[@]}; do
for NUM_SECONDS in ${NUM_SECONDS_POOL[@]}; do
for KEYSPACE_SIZE in ${KEYSPACE_SIZE_POOL[@]}; do
    test_i=$(( $test_i + 1 ))

    disp 1
    disp 1 "Test $test_i/$NUM_TESTS - num_threads: $NUM_THREADS, num_seconds: $NUM_SECONDS, keyspace_size: $KEYSPACE_SIZE" $C_BLU

    # ------ Start client and servers ------
    runClient $NUM_THREADS $NUM_SECONDS $KEYSPACE_SIZE &
    CLIENTPID=$!

    unset SERVER_PORTS
    declare -A SERVER_PORTS

    ACTIVE_SERV_A=`getFirstServ`
    ACTIVE_SERV_B=`getNextServ $ACTIVE_SERV_A $ACTIVE_SERV_A`
    disp 0 "Initial active servers: ${SERVER_HOSTS[$ACTIVE_SERV_A]} ${SERVER_HOSTS[$ACTIVE_SERV_B]}"

    runServ ${SERVER_HOSTS[$ACTIVE_SERV_A]}
    runServ ${SERVER_HOSTS[$ACTIVE_SERV_B]}

    # ------ Server kill/restart loop while client is running ------
    while clientIsActive; do
        sleep $KILL_DELAY
        clientIsActive || break
        
        killServ ${SERVER_HOSTS[$ACTIVE_SERV_A]}
        
        sleep $BRIEF_DELAY
        clientIsActive || break
        
        ACTIVE_SERV_A=`getNextServ $ACTIVE_SERV_A $ACTIVE_SERV_B`
        runServ ${SERVER_HOSTS[$ACTIVE_SERV_A]}

        disp 0 "Current active servers: ${SERVER_HOSTS[$ACTIVE_SERV_A]} ${SERVER_HOSTS[$ACTIVE_SERV_B]}"

        sleep $KILL_DELAY
        clientIsActive || break
        
        killServ ${SERVER_HOSTS[$ACTIVE_SERV_B]}

        sleep $BRIEF_DELAY
        clientIsActive || break
        
        ACTIVE_SERV_B=`getNextServ $ACTIVE_SERV_B $ACTIVE_SERV_A`
        runServ ${SERVER_HOSTS[$ACTIVE_SERV_B]}

        disp 0 "Current active servers: ${SERVER_HOSTS[$ACTIVE_SERV_A]} ${SERVER_HOSTS[$ACTIVE_SERV_B]}"
    done

    # ------ Calculate results ------
    disp 0 "Client is terminated, calculating results"
    calcResults
    cleanup
    showResults

done; done; done

# ------ Let's close it out now fellas ------
trap - INT # release interrupt capture

disp 1
showFinalResults

cleanup &>/dev/null