docker build . -t pyexample4:latest 
docker run --rm --ip 172.200.0.240 --hostname pyexample4 --env-file hadoop.env --network hadoop --name pyexample4 pyexample4