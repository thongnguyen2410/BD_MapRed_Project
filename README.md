# Big data - Map Reduce project

## Part 1 (a) (b)

**Set up a single node cluster and optionally an eclipse development environment to create and test your programs.** 

This project uses [Google cloud](https://cloud.google.com/) VM for setting up a Cloudera quick start docker container for Hadoop Map Reduce development environment. [This](https://medium.com/@alipazaga07/big-data-as-a-service-get-easily-running-a-cloudera-quickstart-image-with-dockers-in-gcp-34d28aa7dad7) shows how the setting up in details.

*Below is how to connect to the Cloudera quick start container and check Hadoop services status:*

```bash
thongnguyen2410@small:~$ sudo docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                                NAMES
194453c4f758        90568ffbcb7c        "/usr/bin/docker-qui…"   2 days ago          Up 2 days           0.0.0.0:90->80/tcp, 0.0.0.0:7190->7180/tcp, 0.0.0.0:8777->8888/tcp   trusting_gagarin
thongnguyen2410@small:~$ sudo docker exec -it 194453c4f758  bash
[root@quickstart /]# service --status-all
...
Hadoop datanode is running                                 [  OK  ]
Hadoop journalnode is running                              [  OK  ]
Hadoop namenode is running                                 [  OK  ]
Hadoop secondarynamenode is running                        [  OK  ]
Hadoop httpfs is running                                   [  OK  ]
Hadoop historyserver is running                            [  OK  ]
Hadoop nodemanager is running                              [  OK  ]
...
```

## Part 1 (c)

**Get WordCount (test run)**

### How to build and run

**The script `run.sh` will:**

- run `javac` to build `.java` files. This use the `-cp` to refer to Hadoop libs(.jar files) in e.g. `/usr/lib/hadoop`
- run `jar` to package `.class` files to `.jar` file
- run `hadoop fs -copyFromLocal` to copy test files in`input/*` to HDFS
- run `hadoop jar` to execute `.jar` file in `pseudo distributed` mode (or run `java -jar` in `local` mode)
- run `hadoop fs -cat` to display output

**Usage of `run.sh`:**

This script is default to use Hadoop libs at `/usr/lib/hadoop`. If in your environment, Hadoop libs is at different location, then run the below before this script:

``` bash
HADOOP_LIB_DIR=</path/to/lib/hadoop>
```

To run in `pseudo distributed` mode:

```bash
Usage  : ./run.sh <package_dir> <class_name> [numReduceTasks]
Example: ./run.sh part1/c WordCount
```

Or to run in `local` mode:

```
Usage  : ./run.sh <package_dir> <class_name> local
Example: ./run.sh part1/c WordCount local
```

### WordCount ouput

```bash
[cloudera@quickstart BD_MapRed_Project]$ ./run.sh part1/c WordCount 4
...
==================================================
hadoop fs -cat /user/cloudera/input/*
==================================================
one six three
two three five
two six four six five
three six four
four five five six
four five six
==================================================
hadoop fs -cat /user/cloudera/output/*
==================================================
==>/user/cloudera/output/_SUCCESS<==
==>/user/cloudera/output/part-r-00000<==
==>/user/cloudera/output/part-r-00001<==
one     1
six     6
three   3
==>/user/cloudera/output/part-r-00002<==
==>/user/cloudera/output/part-r-00003<==
five    5
four    4
two     2
```

## Part 1 (d)

**Modify WordCount to InMapperWordCount and test run**

```bash
./run.sh part1/d InMapperWordCount
```

## Part 1 (e)

**Average Computation Algorithm for Apache access log**

```bash
./run.sh part1/e ApacheLogAvg
```

## Part 1 (f)

**In-mapper combining version of Average Computation Algorithm for Apache access log**

```bash
./run.sh part1/f InMapperApacheLogAvg
```

## Part 2

 **Pairs algorithm to compute relative frequencies** 

```bash
./run.sh part2 RelativeFreqPair 2
```

## Part 3

 **Stripes algorithm to compute relative frequencies** 

```bash
./run.sh part3 RelativeFreqStripe 2
```

## Part 4

 **Pairs in Mapper and Stripes in Reducer to compute relative frequencies** 

```bash
./run.sh part4 RelativeFreqPairStripe 2
```

## Part 5

**Solve a MapReduce problem of your choice!**

The problem of facebook common friends finding is described [here](http://stevekrenzel.com/articles/finding-friends).

>Assume the friends are stored as Person->[List of Friends], our friends list is then:
>
>> A -> B C D
>>
>> B -> A C D E
>>
>> C -> A B D E
>>
>> D -> A B C E
>>
>> E -> B C D
>
>...
>
>The result after reduction is:
>
>> (A B) -> (C D)
>>
>> (A C) -> (B D)
>>
>> (A D) -> (B C)
>>
>> (B C) -> (A D E)
>>
>> (B D) -> (A C E)
>>
>> (B E) -> (C D)
>>
>> (C D) -> (A B E)
>>
>> (C E) -> (B D)
>>
>> (D E) -> (B C)

```bash
./run.sh part5 FriendFinding 2
```

```bash
==================================================
hadoop fs -cat /user/cloudera/input/*
==================================================
A B C D
B A C D E
C A B D E
D A B C E
E B C D
==================================================
hadoop fs -cat /user/cloudera/output/*
==================================================
==>/user/cloudera/output/_SUCCESS<==
==>/user/cloudera/output/part-r-00000<==
(A, B)  [ D, C ]
(A, D)  [ B, C ]
(B, C)  [ D, E, A ]
(B, E)  [ D, C ]
(C, D)  [ E, A, B ]
(D, E)  [ B, C ]
==>/user/cloudera/output/part-r-00001<==
(A, C)  [ D, B ]
(B, D)  [ E, A, C ]
(C, E)  [ D, B ]
```

