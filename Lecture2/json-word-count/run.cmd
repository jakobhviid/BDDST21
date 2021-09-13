docker build . -t pyexample2:latest 
docker run --rm --ip 172.200.0.240 --hostname pyexample2 --env-file hadoop.env --network hadoop --name pyexample2 pyexample2