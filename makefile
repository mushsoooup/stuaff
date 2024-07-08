all: store-server web-server load-balancer

store-server:
	cd src && go build -o ../store-server ./kvstore/exec

web-server:
	cd src && go build -o ../web-server ./web
	
load-balancer:
	cd src && go build -o ../load-balancer ./lb/exec

clean:
	rm web-server store-server load-balancer