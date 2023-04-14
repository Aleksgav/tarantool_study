build:
	tarantoolctl rocks make

start:
	cartridge start

db_clean:
	cartridge clean
	rm -rf tmp/*

setup_vshard:
	cartridge replicasets setup --bootstrap-vshard
