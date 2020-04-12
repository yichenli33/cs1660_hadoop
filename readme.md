This is the repository for CS1660 hadoop project.
pitt id: yil167

########################
Link to the video demo: https://github.com/yichenli33/cs1660_hadoop or https://drive.google.com/file/d/1guQlqz9KksCj3vtmP-Q8zfgmhLMS8Fzc/view?usp=sharing
########################


########################
Project Description
########################
1. GUI application:
To run GUI application on docker container, I used xQuartz with socat to open a display listening port, and the IP will be passed in as environment variable. --env DISPLAY=100.64.9.227:0

2. Java Compilation
java files together with zip files will be copied to /usr/src/app directory in the image. There, use javac -cp *: SearchGUI.java to compile it and use CMD to run the GUI application.

3. Using apache http client and Google REST API to access Storage Bucket, Jobs.

4. GCP cluster and API credentials
detailed walk through shown in video.

5. InvertedIndex Algorithm Implementation
    a. Mapper uses textInputFormat to read lineoffset and content. The word and document name is stored in WordPair object. The Mapper will output WordPair object and IntWritable.
    b. WordPair is a class that extends "Writable, WritableComponent", and is a composite key containing natural key "word" and value key "docName".

6. InvertedIndex ALgorithm Implemented with counters
    a.  first set up enumeration COUNTER that consists of WORD_COUNT and MAPPER_COUNT.
     WORD_COUNT records count of words by all mappers.
     MAPPER_COUNT records count of number of mappers.

########################
List of Requirements Met
########################
1. First Java GUI application execution from
2. InvertedIndex implemented with counters
3. Inverted Indexing MapReduce Implementation and Execution on the Cluster (GCP)




------------------------------
display port
------------------------------
local machine IP: 100.64.9.227

------------------------------
docker command
------------------------------
docker build --tag searchguiimage:3.0 . 

docker run --privileged --env DISPLAY=100.64.9.227:0 --env --name searchGUIContainer searchguiimage:3.0
