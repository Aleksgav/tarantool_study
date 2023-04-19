build:
	tarantoolctl rocks make

start:
	cartridge start

db_clean:
	cartridge clean
	rm -rf tmp/*

setup_vshard:
	cartridge replicasets setup --bootstrap-vshard

check:
	.rocks/bin/luacheck .

luatest:
	cartridge stop  # to prevent "address already in use" error
	.rocks/bin/luatest -v
