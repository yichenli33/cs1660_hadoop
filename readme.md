local machine IP: 100.64.9.227


docker build --tag searchguiimage:3.0 . 


docker run --privileged --env DISPLAY=100.64.9.227:0 --env --name searchGUIContainer searchguiimage:3.0