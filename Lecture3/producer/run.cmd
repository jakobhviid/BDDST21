docker build . -t pyexample2:latest 
docker run --rm --network confluent -ti pyexample2 /bin/bash