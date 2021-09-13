docker build . -t pyexample1:latest 
docker run --rm --ip 172.200.0.240 --hostname pyexample1 --env-file hadoop.env --network hadoop --name pyexample1 pyexample1