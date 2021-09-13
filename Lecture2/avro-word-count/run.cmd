docker build . -t pyexample3:latest 
docker run --rm --ip 172.200.0.240 --hostname pyexample3 --env-file hadoop.env --network hadoop --name pyexample3 pyexample3