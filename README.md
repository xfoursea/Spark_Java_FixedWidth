# SparkJavaETL

This is a POC project to demo how to use Spark to

     .Read and Parse FixedWidthFile
     .Execute masking and validation per line
     .Valid and invalid lines are saved to seperate HDFS places
     .Query the loaded file
     .Save masked RDD to a Hive table


Tested with Hortonworks 2.3.0 Sandbox.


To compile:

    mvn clean package

To run:

    su - spark
    cd /usr/hdp/current/spark-client
    ./bin/spark-submit --class com.hortonworks.rxu.SparkEtl --master yarn-client --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /home/spark/sparketl-1.0.jar /user/root/people.txt /user/spark/test /user/spark/valid /user/spark/invalid



Original file:

    [root@sandbox ~]# cat people.txt
    12301     Johnny              Begood              Programmer          12306
    12302     Ainta               Listening           Programmer          12306
    12303     Neva                Mind                Architect           12306
    12304     Joseph              Blow                Tester              12308
    12305     Sallie              Mae                 Programmer          12306
    12306     Bilbo               Baggins             Development Manager 12307
    12307     Nuther              One                 Director            11111
    12308     Yeta                Notherone           Testing Manager     12307
    12309     Evenmore            Dumbnames           Senior Architect    12307
    12310     Last                Sillyname           Senior Tester       12308
    12311     Johnny              Test                Invalid             12311
    12312     Johnny                                  Invalid             12313

Output:

 1. Masked file:


    [root@sandbox ~]# hadoop fs -cat /user/spark/test/part-00000
    12301     Johnny              Begood              Progxxxxxxxxxxxxxxx 12306
    12302     Ainta               Listening           Progxxxxxxxxxxxxxxx 12306
    12303     Neva                Mind                Archxxxxxxxxxxxxxxx 12306
    12304     Joseph              Blow                Testxxxxxxxxxxxxxxx 12308
    12305     Sallie              Mae                 Progxxxxxxxxxxxxxxx 12306
    12306     Bilbo               Baggins             Devexxxxxxxxxxxxxxx 12307
    12307     Nuther              One                 Direxxxxxxxxxxxxxxx 11111
    [root@sandbox ~]# hadoop fs -cat /user/spark/test/part-00001
    12308     Yeta                Notherone           Testxxxxxxxxxxxxxxx 12307
    12309     Evenmore            Dumbnames           Senixxxxxxxxxxxxxxx 12307
    12310     Last                Sillyname           Senixxxxxxxxxxxxxxx 12308
    12311     Johnny              Test                Invaxxxxxxxxxxxxxxx 12311
    12312     Johnny                                  Invaxxxxxxxxxxxxxxx 12313

 2. Invalid file:


    [root@sandbox ~]# hadoop fs -cat /user/spark/invalid/part-00001
    12311     Johnny              Test                Invaxxxxxxxxxxxxxxx 12311
    12312     Johnny                                  Invaxxxxxxxxxxxxxxx 12313

 3. Valid file:


    root@sandbox ~]# hadoop fs -cat /user/spark/valid/part-00000
    12309     Evenmore            Dumbnames           Senixxxxxxxxxxxxxxx 12307
    12308     Yeta                Notherone           Testxxxxxxxxxxxxxxx 12307
    12306     Bilbo               Baggins             Devexxxxxxxxxxxxxxx 12307
    12307     Nuther              One                 Direxxxxxxxxxxxxxxx 11111
    12302     Ainta               Listening           Progxxxxxxxxxxxxxxx 12306
    12305     Sallie              Mae                 Progxxxxxxxxxxxxxxx 12306
    12310     Last                Sillyname           Senixxxxxxxxxxxxxxx 12308
    [root@sandbox ~]# hadoop fs -cat /user/spark/valid/part-00001
    12301     Johnny              Begood              Progxxxxxxxxxxxxxxx 12306
    12304     Joseph              Blow                Testxxxxxxxxxxxxxxx 12308
    12303     Neva                Mind                Archxxxxxxxxxxxxxxx 12306

4. Hive table:


    hive> select * from masked_t1;
    OK
    12301    	Johnny             	Begood             	Progxxxxxxxxxxxxxxx	12306
    12302    	Ainta              	Listening          	Progxxxxxxxxxxxxxxx	12306
    12303    	Neva               	Mind               	Archxxxxxxxxxxxxxxx	12306
    12304    	Joseph             	Blow               	Testxxxxxxxxxxxxxxx	12308
    12305    	Sallie             	Mae                	Progxxxxxxxxxxxxxxx	12306
    12306    	Bilbo              	Baggins            	Devexxxxxxxxxxxxxxx	12307
    12307    	Nuther             	One                	Direxxxxxxxxxxxxxxx	11111
    12308    	Yeta               	Notherone          	Testxxxxxxxxxxxxxxx	12307
    12309    	Evenmore           	Dumbnames          	Senixxxxxxxxxxxxxxx	12307
    12310    	Last               	Sillyname          	Senixxxxxxxxxxxxxxx	12308
    12311    	Johnny             	Test               	Invaxxxxxxxxxxxxxxx	12311
    12312    	Johnny             	                   	Invaxxxxxxxxxxxxxxx	12313

