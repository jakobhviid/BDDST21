docker build . -t pyexample1:latest 
docker run --rm --network confluent -ti pyexample1 /bin/bash