FROM ubuntu:16.04 
WORKDIR /usr/src/app 
COPY * ./

ENV ACCESS_KEY null

RUN apt-get update && apt-get install -y default-jdk && javac -cp *: SearchGUI.java

CMD ["java", "-cp", "*:", "SearchGUI"]

